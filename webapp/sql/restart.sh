#!/bin/bash
set -xe

CURRENT_DIR=$(cd $(dirname $0);pwd)
cd $CURRENT_DIR

export LANG="C.UTF-8"

docker stop isudb
docker container run --rm --name isudb -d -v `pwd`/conf:/etc/mysql/conf.d -v `pwd`/sqlfiles:/docker-entrypoint-initdb.d -v `pwd`/datadir:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=secret -p 3306:3306 mysql:8.0
