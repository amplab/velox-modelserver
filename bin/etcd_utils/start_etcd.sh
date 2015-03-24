#!/usr/bin/env bash
source /home/ubuntu/etcd_variables.sh

echo /home/ubuntu/etcd/etcd -name velox$ETCD_PARTITION \
  -initial-advertise-peer-urls http://$HOSTNAME:2380 \
  -listen-peer-urls http://$HOSTNAME:2380 \
  -initial-cluster-token $ETCD_CLUSTER_TOKEN \
  -initial-cluster $ETCD_CLUSTER -initial-cluster-state new \
  -listen-client-urls http://$HOSTNAME:4001

/home/ubuntu/etcd/etcd -name velox$ETCD_PARTITION \
  -initial-advertise-peer-urls http://$HOSTNAME:2380 \
  -listen-peer-urls http://$HOSTNAME:2380 \
  -initial-cluster-token $ETCD_CLUSTER_TOKEN \
  -initial-cluster $ETCD_CLUSTER -initial-cluster-state new \
  -listen-client-urls http://$HOSTNAME:4001,http://$HOSTNAME:2379
