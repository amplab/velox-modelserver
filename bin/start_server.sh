#!/usr/bin/env sh

java -Xms3g -Xmx3g -Dlog4j.configuration=file:conf/log4j.properties \
  -cp veloxms-core/target/veloxms-core-0.0.1-SNAPSHOT.jar edu.berkeley.veloxms.VeloxApplication server conf/config.yml
