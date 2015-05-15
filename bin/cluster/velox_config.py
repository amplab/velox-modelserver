import json

matrixfact_config = {
        'onlineUpdateDelayInMillis': 5000,
        'batchRetrainDelayInMillis': 500000,
        'dimensions': 50,
        'modelType': 'MatrixFactorizationModel',
        }

newsgroups_config = {
        'onlineUpdateDelayInMillis': 5000,
        'batchRetrainDelayInMillis': 50000000,
        'dimensions': 20,
        'trainPath': 's3n://20newsgroups/',
        'modelType': 'NewsgroupsModel',
        }

config = {
        'sparkMaster': "local[2]",
        'sparkDataLocation': "/Users/tomerk11/Desktop/velox-data",
        'models': {
                'matrixfact': json.dumps(matrixfact_config),
                'newsgroups': json.dumps(newsgroups_config)
                }
        }
