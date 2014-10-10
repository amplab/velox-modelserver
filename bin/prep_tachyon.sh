#!/usr/bin/env sh


# source ~/ec2-variables.sh

curl -H "Content-Type: application/json" -d '{
  "itemsDst":"tachyon://ec2-204-236-207-142.compute-1.amazonaws.com:19998/item-model",
"usersDst":"tachyon://ec2-204-236-207-142.compute-1.amazonaws.com:19998/user-model",
"obsDst":"tachyon://ec2-204-236-207-142.compute-1.amazonaws.com:19998/movie-ratings",
"itemsSrc":"/home/ubuntu/data/product_model.txt",
"usersSrc":"/home/ubuntu/data/user_model.txt",
"obsSrc":"/home/ubuntu/data/ratings.dat",
"partition":0}' http://localhost:8080/misc/prep-tachyon


# curl -H "Content-Type: application/json" -d '{
#   "tachloc":"tachyon://ec2-204-236-207-142.compute-1.amazonaws.com:19998/test-store",
#   "create":true,
#   "part":0,
#   "key":123}' http://localhost:8080/misc/prep-tachyon


