#!/usr/bin/env bash

java -XX:+UseParallelGC -Xmx10g -Xms10g -Dlog4j.configuration=file:conf/log4j.properties \
  -cp /home/ubuntu/velox-modelserver/veloxms-client/target/veloxms-client-0.0.1-SNAPSHOT.jar:conf/log4j.properties \
  edu.berkeley.veloxms.client.VeloxWorkloadDriver \
  --numRequests 10000 \
  --veloxURLFile /home/ubuntu/velox-modelserver/conf/server_partitions.txt \
  --numUsers 100000 \
  --numItems 50000 \
  --numPartitions 4 \
  --percentObs 0.2

