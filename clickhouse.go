package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/hashicorp/vault/sdk/database/dbplugin/v5"
	"github.com/hashicorp/vault/sdk/database/helper/connutil"
	"github.com/hashicorp/vault/sdk/database/helper/dbutil"
	"github.com/hashicorp/vault/sdk/helper/dbtxn"
	"github.com/hashicorp/vault/sdk/helper/template"
)

const (
	clickhouseTypeName = "clickhouse"

	onCluster = "ON CLUSTER '{cluster}'"

	defaultChangePasswordStatement = `ALTER USER "{{username}}" IDENTIFIED BY '{{password}}';`

	defaultUserNameTemplate = `{{ printf "v-%s-%s-%s-%s" (.DisplayName | truncate 8) (.RoleName | truncate 8) (random 20) (unix_time) | truncate 32 }}`
)

func New() (interface{}, error) {
	db := new()
	// Wrap the plugin with middleware to sanitize errors
	dbType := dbplugin.NewDatabaseErrorSanitizerMiddleware(db, db.secretValues)
	return dbType, nil
}

func new() *Clickhouse {
	connProducer := &connutil.SQLConnectionProducer{}
	connProducer.Type = clickhouseTypeName

	db := &Clickhouse{
		SQLConnectionProducer: connProducer,
	}

	return db
}

type Clickhouse struct {
	*connutil.SQLConnectionProducer

	usernameProducer template.StringTemplate
}

func (p *Clickhouse) Initialize(ctx context.Context, req dbplugin.InitializeRequest) (dbplugin.InitializeResponse, error) {
	newConf, err := p.SQLConnectionProducer.Init(ctx, req.Config, req.VerifyConnection)
	if err != nil {
		return dbplugin.InitializeResponse{}, err
	}

	usernameTemplate, err := strutil.GetString(req.Config, "username_template")
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("failed to retrieve username_template: %w", err)
	}
	if usernameTemplate == "" {
		usernameTemplate = defaultUserNameTemplate
	}

	up, err := template.NewTemplate(template.Template(usernameTemplate))
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("unable to initialize username template: %w", err)
	}
	p.usernameProducer = up

	_, err = p.usernameProducer.Generate(dbplugin.UsernameMetadata{})
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("invalid username template: %w", err)
	}

	resp := dbplugin.InitializeResponse{
		Config: newConf,
	}
	return resp, nil
}

func (c *Clickhouse) getConnection(ctx context.Context) (*sql.DB, error) {
	db, err := c.Connection(ctx)
	if err != nil {
		return nil, err
	}

	return db.(*sql.DB), nil
}

func (c *Clickhouse) UpdateUser(ctx context.Context, req dbplugin.UpdateUserRequest) (dbplugin.UpdateUserResponse, error) {
	if req.Username == "" {
		return dbplugin.UpdateUserResponse{}, fmt.Errorf("missing username")
	}
	if req.Password == nil && req.Expiration == nil {
		return dbplugin.UpdateUserResponse{}, fmt.Errorf("no changes requested")
	}

	merr := &multierror.Error{}
	if req.Password != nil {
		err := c.changeUserPassword(ctx, req.Username, req.Password)
		merr = multierror.Append(merr, err)
	}
	return dbplugin.UpdateUserResponse{}, merr.ErrorOrNil()
}

func (c *Clickhouse) changeUserPassword(ctx context.Context, username string, changePass *dbplugin.ChangePassword) error {
	stmts := changePass.Statements.Commands

	password := changePass.NewPassword
	if password == "" {
		return fmt.Errorf("missing password")
	}

	c.Lock()
	defer c.Unlock()

	db, err := c.getConnection(ctx)
	if err != nil {
		return fmt.Errorf("unable to get connection: %w", err)
	}

	if len(stmts) == 0 {
		stmt := defaultChangePasswordStatement
		isClusterExist, err := c.isClusterExist(ctx)
		if err != nil {
			return err
		}
		if isClusterExist {
			stmt = fmt.Sprintf("%s %s", stmt, onCluster)
		}
		stmts = []string{stmt}
	}

	// Check if the user exists
	var exists bool
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT c > 0 AS exists FROM ( SELECT count() AS c FROM system.users WHERE name='%s' );", username)).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer tx.Rollback()

	for _, stmt := range stmts {
		for _, query := range strutil.ParseArbitraryStringSlice(stmt, ";") {
			query = strings.TrimSpace(query)
			if len(query) == 0 {
				continue
			}

			m := map[string]string{
				"name":     username,
				"username": username,
				"password": password,
			}
			if err := dbtxn.ExecuteTxQueryDirect(ctx, tx, m, query); err != nil {
				return fmt.Errorf("failed to execute query: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (c *Clickhouse) NewUser(ctx context.Context, req dbplugin.NewUserRequest) (dbplugin.NewUserResponse, error) {
	if len(req.Statements.Commands) == 0 {
		return dbplugin.NewUserResponse{}, dbutil.ErrEmptyCreationStatement
	}

	c.Lock()
	defer c.Unlock()

	username, err := c.usernameProducer.Generate(req.UsernameConfig)
	if err != nil {
		return dbplugin.NewUserResponse{}, err
	}

	db, err := c.getConnection(ctx)
	if err != nil {
		return dbplugin.NewUserResponse{}, fmt.Errorf("unable to get connection: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return dbplugin.NewUserResponse{}, fmt.Errorf("unable to start transaction: %w", err)
	}
	defer tx.Rollback()

	for _, stmt := range req.Statements.Commands {
		// Otherwise, it's fine to split the statements on the semicolon.
		for _, query := range strutil.ParseArbitraryStringSlice(stmt, ";") {
			query = strings.TrimSpace(query)
			if len(query) == 0 {
				continue
			}
			query = query + ";"

			m := map[string]string{
				"name":     username,
				"username": username,
				"password": req.Password,
			}
			if err := dbtxn.ExecuteTxQueryDirect(ctx, tx, m, query); err != nil {
				return dbplugin.NewUserResponse{}, fmt.Errorf("failed to execute query: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return dbplugin.NewUserResponse{}, err
	}

	resp := dbplugin.NewUserResponse{
		Username: username,
	}
	return resp, nil
}

func (c *Clickhouse) DeleteUser(ctx context.Context, req dbplugin.DeleteUserRequest) (dbplugin.DeleteUserResponse, error) {
	c.Lock()
	defer c.Unlock()

	if len(req.Statements.Commands) == 0 {
		return dbplugin.DeleteUserResponse{}, c.defaultDeleteUser(ctx, req.Username)
	}

	return dbplugin.DeleteUserResponse{}, c.customDeleteUser(ctx, req.Username, req.Statements.Commands)
}

func (c *Clickhouse) customDeleteUser(ctx context.Context, username string, revocationStmts []string) error {
	db, err := c.getConnection(ctx)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
	}()

	for _, stmt := range revocationStmts {
		for _, query := range strutil.ParseArbitraryStringSlice(stmt, ";") {
			query = strings.TrimSpace(query)
			if len(query) == 0 {
				continue
			}

			m := map[string]string{
				"name":     username,
				"username": username,
			}
			if err := dbtxn.ExecuteTxQueryDirect(ctx, tx, m, query); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (c *Clickhouse) isClusterExist(ctx context.Context) (bool, error) {
	db, err := c.getConnection(ctx)
	if err != nil {
		return false, err
	}
	var existCluster bool
	err = db.QueryRowContext(ctx, "SELECT COUNT() > 0 as existCluster FROM system.macros where macro = 'cluster';").Scan(&existCluster)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return existCluster, nil
}

func (c *Clickhouse) defaultDeleteUser(ctx context.Context, username string) error {
	reqCluster := ""
	db, err := c.getConnection(ctx)
	if err != nil {
		return err
	}

	// Check if the user exists
	var exists bool
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT c > 0 AS exists FROM ( SELECT count() AS c FROM system.users WHERE name='%s' );", username)).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if !exists {
		return nil
	}
	//log.Println(username)

	isCluster, err := c.isClusterExist(ctx)
	if err != nil {
		return err
	}

	if isCluster {
		reqCluster = onCluster
	}

	// Drop this user
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS \"%s\" %s;", username, reqCluster))
	if err != nil {
		return fmt.Errorf("%v: %v", err, isCluster)
	}

	defer db.Close()

	return nil
}

func (c *Clickhouse) secretValues() map[string]string {
	return map[string]string{
		c.Password: "[password]",
	}
}

func (c *Clickhouse) Type() (string, error) {
	return clickhouseTypeName, nil
}
