#!/bin/bash


vault write database/config/clickhouse \
    plugin_name=vault-plugin-database-clickhouse \
    connection_url="clickhouse://172.21.0.2:9000?username={{username}}&password={{password}}" \
    allowed_roles="*" \
    username="admin_mgmt" \
    password="test" \
    username_template="my-org-{{unix_time}}-{{random 8}}" \
    password_policy="my-org"
    