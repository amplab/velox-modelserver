#!/usr/bin/env sh

java -Dlog4j.configuration=file:log4j.properties \
  -jar target/velox-model-server-0.0.1-SNAPSHOT.jar server config.yml &
