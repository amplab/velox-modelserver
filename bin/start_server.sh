#!/usr/bin/env sh

java -Xms5g -Xmx10g -Dlog4j.configuration=file:conf/log4j.properties \
  -cp assembly/target/scala-2.11/velox-assembly-0.1.jar edu.berkeley.veloxms.VeloxApplication server conf/config.yml
