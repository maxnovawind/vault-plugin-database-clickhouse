package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/vault/sdk/database/dbplugin/v5"
	dbtesting "github.com/hashicorp/vault/sdk/database/dbplugin/v5/testing"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/xo/dburl"
)

const (
	adminUsername = "maxnovawind"
	adminPassword = "maxpassadmin"
)

func getRequestTimeout(t *testing.T) time.Duration {
	rawDur := os.Getenv("VAULT_TEST_DATABASE_REQUEST_TIMEOUT")
	if rawDur == "" {
		return 2 * time.Second
	}

	dur, err := time.ParseDuration(rawDur)
	if err != nil {
		t.Fatalf("Failed to parse custom request timeout %q: %s", rawDur, err)
	}
	return dur
}

func prepareClickhouseTestContainer(t *testing.T) (connString string, cleanup func()) {
	// chVer should match a redis repository tag. Default to latest.
	chVer := os.Getenv("CLICKHOUSE_VERSION")
	if chVer == "" {
		chVer = "latest"
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Failed to connect to docker: %s", err)
	}

	ro := &dockertest.RunOptions{
		Repository: "clickhouse/clickhouse-server",
		Tag:        chVer,
		Env: []string{fmt.Sprintf("CLICKHOUSE_USER=%s", adminUsername),
			fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", adminPassword),
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1"},
	}
	resource, err := pool.RunWithOptions(ro)
	if err != nil {
		t.Fatalf("Could not start local clickhouse docker container: %s", err)
	}

	cleanup = func() {
		err := pool.Retry(func() error {
			return pool.Purge(resource)
		})
		if err != nil {
			if strings.Contains(err.Error(), "No such container") {
				return
			}
			t.Fatalf("Failed to cleanup local container: %s", err)
		}
	}
	address := fmt.Sprintf("clickhouse://%s:9000", resource.Container.NetworkSettings.Networks["bridge"].IPAddress)
	connString = fmt.Sprintf("%s?username=%s&password=%s", address, adminUsername, adminPassword)
	t.Log(connString)
	pool.MaxWait = time.Minute * 2
	if err = pool.Retry(func() error {
		t.Log("Waiting for the database to start...")

		var err error
		var db *sql.DB
		db, err = sql.Open("clickhouse", connString)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		t.Fatalf("Could not connect to clickhouse: %s", err)
		cleanup()
	}
	time.Sleep(3 * time.Second)
	return connString, cleanup
}

func TestClickhouse_New(t *testing.T) {
	t.Parallel()
	db, err := New()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	dbtype, err := db.(dbplugin.DatabaseErrorSanitizerMiddleware).Type()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	assert.Equal(t, "clickhouse", dbtype)
}

func TestClickhouse_Initialize(t *testing.T) {
	t.Parallel()
	connURL, cleanup := prepareClickhouseTestContainer(t)
	t.Cleanup(cleanup)

	db := new()
	defer dbtesting.AssertClose(t, db)

	expectedConfig := map[string]interface{}{
		"connection_url": connURL,
	}
	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}
	resp := dbtesting.AssertInitialize(t, db, req)
	if !reflect.DeepEqual(resp.Config, expectedConfig) {
		t.Fatalf("Actual: %#v\nExpected: %#v", resp.Config, expectedConfig)
	}

	connProducer := db.SQLConnectionProducer
	if !connProducer.Initialized {
		t.Fatal("Database should be initialized")
	}
}

func TestClickhouse_InitializeFail(t *testing.T) {
	t.Parallel()

	db := new()
	defer dbtesting.AssertClose(t, db)

	expectedConfig := map[string]interface{}{
		"connection_url": "dsd",
	}
	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": "http://",
		},
		VerifyConnection: true,
	}
	resp, _ := db.Initialize(context.Background(), req)
	if reflect.DeepEqual(resp.Config, expectedConfig) {
		t.Fatalf("Actual: %#v\nNot Expected: %#v", resp.Config, expectedConfig)
	}
}

func TestClickhouse_InitializeFailUserTmpl(t *testing.T) {
	t.Parallel()
	connURL, cleanup := prepareClickhouseTestContainer(t)
	t.Cleanup(cleanup)
	db := new()
	defer dbtesting.AssertClose(t, db)
	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url":    connURL,
			"username_template": 1000,
		},
		VerifyConnection: true,
	}
	resp, err := db.Initialize(context.Background(), req)
	if err == nil {
		t.Fatalf("Should not initialize this config: %#v", resp)
	}
}

func TestClickhouse_InitializeFailUserTmplExecute(t *testing.T) {
	t.Parallel()
	connURL, cleanup := prepareClickhouseTestContainer(t)
	t.Cleanup(cleanup)
	db := new()
	defer dbtesting.AssertClose(t, db)
	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url":    connURL,
			"username_template": "{{if}}$dsds{{end}}",
		},
		VerifyConnection: true,
	}
	resp, err := db.Initialize(context.Background(), req)
	if err == nil {
		t.Fatalf("Should not initialize this config: %#v", resp)
	}
}

func TestClickhouse_getConnectionFail(t *testing.T) {
	t.Parallel()
	db := new()
	_, err := db.getConnection(context.Background())
	if err == nil {
		t.Fatalf("not supposed to failed")
	}
}

func TestClickhouse_NewUser(t *testing.T) {
	t.Parallel()
	type testCase struct {
		displayName           string
		roleName              string
		creationStmts         []string
		usernameTemplate      string
		expectErr             bool
		expectedUsernameRegex string
		skipCreateError       bool
		disableInit           bool
	}

	useCases := map[string]testCase{

		"Success Custom Name Creation": {
			displayName:      "token",
			roleName:         "my-role",
			usernameTemplate: "opensee-{{unix_time}}-{{random 8}}",
			creationStmts: []string{
				`CREATE USER "{{name}}" IDENTIFIED BY '{{password}}';
				GRANT ALL ON default.* TO "{{name}}";`},
			expectErr: false,
		},

		"Success Custom Username Creation": {
			displayName:      "token",
			roleName:         "my-role",
			usernameTemplate: "opensee-{{unix_time}}-{{random 8}}",
			creationStmts: []string{
				`CREATE USER "{{username}}" IDENTIFIED BY '{{password}}';
				GRANT ALL ON default.* TO "{{username}}";`},
			expectErr: false,
		},
		"Success Default Username Creation": {
			displayName: "token",
			roleName:    "my-role",
			creationStmts: []string{
				`CREATE USER "{{username}}" IDENTIFIED BY '{{password}}';
				GRANT ALL ON default.* TO "{{username}}";`},
			expectErr: false,
		},
		"Failed Default Username Creation": {
			displayName: "token",
			roleName:    "my-role",
			creationStmts: []string{
				`CREATE USER "{{username}}" IDENTIFIED BY '{{password}}';
				GRANT ALL ON default.* TO "{{username}}";`},
			expectErr:   true,
			disableInit: true,
		},
		"Failed Empty creation": {
			displayName:           "token",
			roleName:              "my-role",
			creationStmts:         []string{},
			expectErr:             true,
			expectedUsernameRegex: `^$`,
		},
		"Failed username template": {
			displayName: "token",
			roleName:    "my-role",
			creationStmts: []string{
				`CREATE USER "{{username}}" IDENTIFIED BY '{{password}}';
				GRANT ALL ON default.* TO "{{username}}";`},
			usernameTemplate: "{{if}}$dsdsds{{end}}",
			expectErr:        true,
			skipCreateError:  true,
		},
		"Failed Bad Statement": {
			displayName: "token",
			roleName:    "my-role",
			creationStmts: []string{
				`foo bar';`},
			expectErr: true,
		},
	}
	connURL, cleanup := prepareClickhouseTestContainer(t)
	t.Cleanup(cleanup)

	for name, test := range useCases {
		t.Run(name, func(t *testing.T) {

			db := new()
			defer dbtesting.AssertClose(t, db)

			initReq := dbplugin.InitializeRequest{
				Config: map[string]interface{}{
					"connection_url":    connURL,
					"username_template": test.usernameTemplate,
				},
				VerifyConnection: true,
			}
			if !test.skipCreateError {
				dbtesting.AssertInitialize(t, db, initReq)
			}

			createReq := dbplugin.NewUserRequest{
				UsernameConfig: dbplugin.UsernameMetadata{
					DisplayName: test.displayName,
					RoleName:    test.roleName,
				},
				Statements: dbplugin.Statements{
					Commands: test.creationStmts,
				},
				Password:   "test",
				Expiration: time.Time{},
			}

			ctx, cancel := context.WithTimeout(context.Background(), getRequestTimeout(t))
			defer cancel()

			if test.disableInit {
				db.Initialized = false
			}

			createResp, err := db.NewUser(ctx, createReq)
			if test.expectErr && err == nil {
				t.Fatalf("err expected, got nil")
			}
			if !test.expectErr && err != nil {
				t.Fatalf("no error expected, got: %s", err)
			}
			re := regexp.MustCompile(test.expectedUsernameRegex)
			if !re.MatchString(createResp.Username) {
				t.Fatalf("Username [%s] does not match regex [%s]", createResp.Username, test.expectedUsernameRegex)
			}

			err = testCredentialsExist(connURL, createResp.Username, createReq.Password)
			if test.expectErr && err == nil {
				t.Fatalf("err expected, got nil")
			}
			if !test.expectErr && err != nil {
				t.Fatalf("no error expected, got: %s", err)
			}
		})
	}
}

func TestClickhouse_secretValue(t *testing.T) {
	t.Parallel()
	db := new()
	assert.Equal(t, map[string]string{
		db.Password: "[password]",
	}, db.secretValues())

}

func TestClickhouse_Type(t *testing.T) {
	t.Parallel()
	db := new()
	str, _ := db.Type()
	assert.Equal(t, "clickhouse", str)

}

func TestClickhouse_DeleteUser(t *testing.T) {
	t.Parallel()
	type testCase struct {
		deleteStatements  []string
		overwriteUsername string
		disableInit       bool
		skipCreateUser    bool
		expectErr         bool
	}

	useCases := map[string]testCase{
		"Sucess name delete": {
			deleteStatements: []string{`
				DROP USER "{{name}}";`,
			},
			expectErr: false,
		},
		"Sucess username delete": {
			deleteStatements: []string{`
				DROP USER "{{username}}";`,
			},
			expectErr: false,
		},
		"Failed delete": {
			deleteStatements: []string{`
				DROP USER "{{username}}";`,
			},
			disableInit: true,
			expectErr:   true,
		},
		"Sucessdefault delete": {
			expectErr: false,
		},
		"Sucess default delete with skip user creation": {
			expectErr:         false,
			skipCreateUser:    true,
			overwriteUsername: "ddd",
		},
		"Failed default delete with skip user creation when select": {
			expectErr:         true,
			skipCreateUser:    true,
			overwriteUsername: "\"'''^$*}",
		},
		"Failed custom failed delete": {
			expectErr: true,
			deleteStatements: []string{`
				DRO USER "{{username}}";`,
			},
		},
		"Failed get connection when default delete": {
			disableInit: true,
			expectErr:   true,
		},
	}

	connURL, cleanup := prepareClickhouseTestContainer(t)
	defer cleanup()

	for name, test := range useCases {
		t.Run(name, func(t *testing.T) {
			db := new()
			defer dbtesting.AssertClose(t, db)

			initReq := dbplugin.InitializeRequest{
				Config: map[string]interface{}{
					"connection_url": connURL,
				},
				VerifyConnection: true,
			}
			dbtesting.AssertInitialize(t, db, initReq)

			createReq := dbplugin.NewUserRequest{
				UsernameConfig: dbplugin.UsernameMetadata{
					DisplayName: "token",
					RoleName:    "my-role",
				},
				Statements: dbplugin.Statements{
					Commands: []string{`
						CREATE USER "{{username}}" IDENTIFIED BY '{{password}}';
						GRANT ALL ON default.* TO "{{username}}";`,
					},
				},
				Password:   adminPassword,
				Expiration: time.Time{},
			}

			ctx, cancel := context.WithTimeout(context.Background(), getRequestTimeout(t))
			defer cancel()

			var usernameResp string

			if !test.skipCreateUser {
				createResp, err := db.NewUser(ctx, createReq)
				if err != nil {
					t.Fatalf("err: %s", err)
				}

				assertCredentialsExist(t, connURL, createResp.Username, createReq.Password)
				usernameResp = createResp.Username
			}

			if test.overwriteUsername != "" {
				usernameResp = test.overwriteUsername
			}

			deleteReq := dbplugin.DeleteUserRequest{
				Username: usernameResp,
				Statements: dbplugin.Statements{
					Commands: test.deleteStatements,
				},
			}

			if test.disableInit {
				db.Initialized = false
			}

			ctx, cancel = context.WithTimeout(context.Background(), getRequestTimeout(t))
			defer cancel()

			_, err := db.DeleteUser(ctx, deleteReq)

			if test.expectErr && err == nil {
				t.Fatalf("err expected, got nil")
			}
			if !test.expectErr && err != nil {
				t.Fatalf("no error expected, got: %s", err)
			}
			if err == nil {
				assertCredentialsDoNotExist(t, connURL, usernameResp, createReq.Password)
			}
		})
	}
}

func TestClickhouse_isCluster(t *testing.T) {

}

func TestClickhouse_UpdateUser(t *testing.T) {
	t.Parallel()

	username := "TESTUSER"
	initialPassword := "myreallysecurepassword"

	type testCase struct {
		req dbplugin.UpdateUserRequest

		expectedPassword string
		expectErr        bool
		disableInit      bool
	}

	tests := map[string]testCase{
		"Failed missing username": {
			req: dbplugin.UpdateUserRequest{
				Username: "",
				Password: &dbplugin.ChangePassword{
					NewPassword: "newpassword",
				},
			},
			expectedPassword: initialPassword,
			expectErr:        true,
		},
		"Failed missing password": {
			req: dbplugin.UpdateUserRequest{
				Username: username,
			},
			expectedPassword: initialPassword,
			expectErr:        true,
		},
		"Failed empty password": {
			req: dbplugin.UpdateUserRequest{
				Username: username,
				Password: &dbplugin.ChangePassword{
					NewPassword: "",
				},
			},
			expectedPassword: initialPassword,
			expectErr:        true,
		},
		"Failed missing username and password": {
			req:              dbplugin.UpdateUserRequest{},
			expectedPassword: initialPassword,
			expectErr:        true,
		},
		"Sucess changePassword": {
			req: dbplugin.UpdateUserRequest{
				Username: username,
				Password: &dbplugin.ChangePassword{
					NewPassword: "somenewpassword",
				},
			},
			expectedPassword: "somenewpassword",
			expectErr:        false,
		},
		"Failed getConnection": {
			req: dbplugin.UpdateUserRequest{
				Username: username,
				Password: &dbplugin.ChangePassword{
					NewPassword: "somenewpassword",
				},
			},
			expectedPassword: "somenewpassword",
			expectErr:        true,
			disableInit:      true,
		},
		"Failed bad statements": {
			req: dbplugin.UpdateUserRequest{
				Username: username,
				Password: &dbplugin.ChangePassword{
					NewPassword: "somenewpassword",
					Statements: dbplugin.Statements{
						Commands: []string{
							"foo bar",
						},
					},
				},
			},
			expectedPassword: initialPassword,
			expectErr:        true,
		},
		"Success custom changepassword statement": {
			req: dbplugin.UpdateUserRequest{
				Username: username,
				Password: &dbplugin.ChangePassword{
					NewPassword: "somenewpassword",
					Statements: dbplugin.Statements{
						Commands: []string{`
						ALTER USER "{{username}}" IDENTIFIED BY '{{password}}';`,
						},
					},
				},
			},
			expectedPassword: "somenewpassword",
			expectErr:        false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			connURL, cleanup := prepareClickhouseTestContainer(t)
			t.Cleanup(cleanup)

			db := new()
			defer dbtesting.AssertClose(t, db)

			initReq := dbplugin.InitializeRequest{
				Config: map[string]interface{}{
					"connection_url": connURL,
				},
				VerifyConnection: true,
			}
			dbtesting.AssertInitialize(t, db, initReq)

			createReq := dbplugin.NewUserRequest{
				Statements: dbplugin.Statements{
					Commands: []string{fmt.Sprintf(`
						CREATE USER IF NOT EXISTS "%s" IDENTIFIED BY '{{password}}';
						GRANT ALL ON default.* TO "%s";`, username, username),
					},
				},
				Password:   initialPassword,
				Expiration: time.Time{},
			}

			ctx, cancel := context.WithTimeout(context.Background(), getRequestTimeout(t))
			defer cancel()

			_, err := db.NewUser(ctx, createReq)
			if err != nil {
				t.Fatalf("failed to create user: %s", err)
			}

			assertCredentialsExist(t, connURL, username, initialPassword)

			ctx, cancel = context.WithTimeout(context.Background(), getRequestTimeout(t))
			defer cancel()

			if test.disableInit {
				db.Initialized = false
			}
			_, err = db.UpdateUser(ctx, test.req)
			if test.expectErr && err == nil {
				t.Fatalf("err expected, got nil")
			}
			if !test.expectErr && err != nil {
				t.Fatalf("no error expected, got: %s", err)
			}
			if db.Initialized {
				assertCredentialsExist(t, connURL, username, test.expectedPassword)
			}

		})
	}
}

func testCredentialsExist(connString string, username string, password string) error {
	strParse, err := dburl.Parse(connString)
	if err != nil {
		return err
	}
	address := fmt.Sprintf("%s://%s:%s?username=%s&password=%s", strParse.Driver, strParse.Hostname(), strParse.Port(), username, password)
	db, err := sql.Open("clickhouse", address)
	if err != nil {
		return fmt.Errorf("%s => %s", err, address)
	}
	defer db.Close()
	return db.Ping()
}

func assertCredentialsExist(t *testing.T, connString, username, password string) {
	t.Helper()
	err := testCredentialsExist(connString, username, password)
	if err != nil {
		t.Fatalf("failed to login: %s", err)
	}
}

func assertCredentialsDoNotExist(t *testing.T, connString, username, password string) {
	t.Helper()
	err := testCredentialsExist(connString, username, password)
	if err == nil {
		t.Fatalf("logged in when it shouldn't have been able to")
	}
}
