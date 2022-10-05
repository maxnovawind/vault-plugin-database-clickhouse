#!/bin/bash
vault write database/roles/my-role \
    db_name=clickhouse \
    creation_statements="CREATE USER \"{{username}}\" IDENTIFIED BY '{{password}}' SETTINGS PROFILE default ON CLUSTER '{cluster}'; GRANT ALL ON default.* TO \"{{name}}\";"\
    max_ttl="1m"