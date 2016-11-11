#!/bin/bash

set -e
set -u
set -o pipefail

docker run -d --name snap-cassandra -p 9042:9042 -p 7199:7199 -p 8082:8082 -p 9160:9160 -d candysmurfhub/cassandra
DOCKER_HOST=${DOCKER_HOST-}
if [[ -z "${DOCKER_HOST}" ]]; then
  SNAP_CASSANDRA_HOST="127.0.0.1"
else
  SNAP_CASSANDRA_HOST=`echo $DOCKER_HOST | grep -o '[0-9]\+[.][0-9]\+[.][0-9]\+[.][0-9]\+'` 
fi

export SNAP_CASSANDRA_HOST
_info "Cassandra Host: ${SNAP_CASSANDRA_HOST}"

_info "Waiting for cassandra docker container"
while ! curl -sG "http://${SNAP_CASSANDRA_HOST}:8082/serverbydomain?querynames=org.apache.cassandra.metrics" > /dev/null 2>&1; do
  sleep 1
  echo -n "."
done

sleep 10

UNIT_TEST="go_test"
test_unit

docker rm snap-cassandra -f
