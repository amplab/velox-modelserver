#!/usr/bin/env sh


java -jar target/velox-model-writer-0.0.1-SNAPSHOT.jar \
  writeModels ~/ml-10mil-data/whole-model/user_models/10M-50factors-all.txt \
  ~/ml-10mil-data/whole-model/item_models/10M-50factors-all.txt \
  ~/ml-10mil-data/ml-10M100K/ratings.dat
