java -Xms17g -Xmx17g -Dlog4j.configuration=file:log4j.properties \
  -jar target/velox-model-writer-0.0.1-SNAPSHOT.jar \
  matPredictions \
  ~/ml-10mil-data/whole-model/user_models/10M-50factors-all.txt \
  ~/ml-10mil-data/whole-model/item_models/10M-50factors-all.txt
