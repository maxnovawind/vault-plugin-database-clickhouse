package main

import (
	"log"
	"os"

	"github.com/hashicorp/vault/api"
	dbplugin "github.com/hashicorp/vault/sdk/database/dbplugin/v5"
	clickhouse "github.com/maxnovawind/vault-plugin-database-clickhouse"
)

func main() {
	apiClientMeta := &api.PluginAPIClientMeta{}
	flags := apiClientMeta.FlagSet()
	flags.Parse(os.Args[1:])

	err := Run()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

// Run starts serving the plugin
func Run() error {
	dbplugin.ServeMultiplex(clickhouse.New)
	return nil
}
