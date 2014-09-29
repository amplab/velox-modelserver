#!/usr/bin/env sh


curl -H "Content-Type: application/json" -d '{
  "itemsDst":"tachyon://ec2-54-234-135-209.compute-1.amazonaws.com:19998/item-model",
"usersDst":"tachyon://ec2-54-234-135-209.compute-1.amazonaws.com:19998/user-model",
"obsDst":"tachyon://ec2-54-234-135-209.compute-1.amazonaws.com:19998/movie-ratings",
"itemsSrc":"/home/ubuntu/data/product_model.txt",
"usersSrc":"/home/ubuntu/data/user_model.txt",
"obsSrc":"/home/ubuntu/data/ratings.dat",
"partition":0}' http://localhost:8080/misc/prep-tachyon

