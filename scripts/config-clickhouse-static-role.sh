#!/bin/bash


vault write database/static-roles/my-static \
    db_name=clickhouse \
    username="toto" \
    rotation_period=30