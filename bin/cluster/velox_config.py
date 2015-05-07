import json

matrixfact_config = {
        'onlineUpdateDelayInMillis': 5000,
        'batchRetrainDelayInMillis': 500000,
        'dimensions': 50,
        'modelType': 'MatrixFactorizationModel',
        }

config = {
        'sparkMaster': "local[2]",
        'sparkDataLocation': "/Users/tomerk11/Desktop/velox-data",
        'models': [
            { 'matrixfact': json.dumps(matrixfact_config) },
            ]
        }
