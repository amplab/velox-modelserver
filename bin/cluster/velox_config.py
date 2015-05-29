import json

matrixfact_config = {
        'onlineUpdateDelayInMillis': 5000,
        'batchRetrainDelayInMillis': 50000000,
        'config': {
                'numFeatures': 50
                },
        'modelType': 'edu.berkeley.veloxms.examples.MatrixFactorizationModel',
        }

newsgroups_config = {
        'onlineUpdateDelayInMillis': 5000,
        'batchRetrainDelayInMillis': 50000000,
        'config': {
                'dataPath': 's3n://20newsgroups/',
                },
        'modelType': 'edu.berkeley.veloxms.examples.NewsgroupsModel',
        }

config = {
        'sparkMaster': "local[2]",
        'sparkDataLocation': "/Users/crankshaw/Desktop/velox-data",
        'models': {
                'matrixfact': json.dumps(matrixfact_config),
                'newsgroups': json.dumps(newsgroups_config)
                }
        }
