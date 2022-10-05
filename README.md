# vault-plugin-database-clickhouse

A [Vault](https://www.vaultproject.io) plugin for Clickhouse

This project uses the database plugin interface introduced in Vault version 0.7.1.


## Build

```
make build
```

This command will generate a bin folder that contain the plugin in binary format.


## Tests

`make test` will run a basic test suite against a Docker version of Clickhouse.

If you want to test with a specific version of clickhouse:

```
export CLICKHOUSE_VERSION="22.2.3.5"
```


## Pre-request

First you have to download the binary vault > 0.7.1 in order to use plugin inside vault.

## Pugin Installation

The Vault plugin system is documented on the [Vault documentation site](https://www.vaultproject.io/docs/internals/plugins.html).

You will need to define a plugin directory using the `plugin_directory` configuration directive, then place the `vault-plugin-database-clickhouse` executable generated above in the directory 
You can follow this example (ex: [Example script](scripts/local_dev.sh))

Run make run for initialize a vault and plugin locally:

```
make run
```

## Configuration

Before using the vault plugin, you have to make sure that `access_managment` settings is enable for the admin user.

The plugin support the vault username_template.

Be careful, clickhouse have a restiction in the password definition.
We have to define password policy which is supported it by clickhouse:
```
length = 20

rule "charset" {
  charset = "abcdefghijklmnopqrstuvwxyz"
}

rule "charset" {
  charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  min-chars = 1
}

rule "charset" {
  charset = "0123456789"
  min-chars = 1
}
```

For using the clickhouse vault plugin with the "clickhouse-password-policy":
```
vault write database/config/clickhouse \
    plugin_name=vault-plugin-database-clickhouse \
    connection_url="clickhouse://172.21.0.2:9000?username={{username}}&password={{password}}" \
    allowed_roles="*" \
    username="admin_mgmt" \
    password="test" \
    username_template="my-org-{{unix_time}}-{{random 8}}" \
    password_policy="clickhouse-password-policy"
```


Define the role in vault plugin:
```
vault write database/roles/my-clickhouse-role \
    db_name=clickhouse \
    creation_statements="CREATE USER \"{{username}}\" IDENTIFIED BY '{{password}}' SETTINGS PROFILE default ON CLUSTER '{cluster}'; GRANT ALL ON default.* TO \"{{name}}\";"\
    max_ttl="1m"
```

Then consume the path credentials for retrieving the temporary access:
```
vault read database/roles/my-clickhouse-role

Key                Value
---                -----
lease_id           database/creds/my-role/ebVtbcgsNg0rHr4ow11kAzfT
lease_duration     1m
lease_renewable    true
password           JimWEVQJzNAy99gtJhpj
username           my-org-1664976032-aRhgyUF4
```