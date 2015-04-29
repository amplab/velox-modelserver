import yaml
import requests
import json
from requests import exceptions
from time import sleep, time, strftime, gmtime, localtime
import os.path
import datetime
import time
import pprint

etcd_port = 4001
hostname = "127.0.0.1"
etcd_host = "http://%s:%d" % (hostname, etcd_port)
base_path = "/v2/keys/cluster_config" 

pp = pprint.PrettyPrinter(indent=4)

# Dict whose keys are hostnames or IP addresses and whose values are the
# partition assignments of those machines. Partition assignments should be
# unique
partitions = {
                '127.0.0.1': 0
             }



matrixfact_config = {
                        'cacheFeatures': 'False',
                        'cachePartialSums': 'False',
                        'cachePredictions': 'False',
                        'dimensions': 50,
                        'modelType': 'MatrixFactorizationModel',
                    }

newsgroups_config = {
                        'cacheFeatures': 'False',
                        'cachePartialSums': 'False',
                        'cachePredictions': 'False',
                        'dimensions': 50,
                        'modelType': 'NewsgroupsModel',
                        'modelLoc': '/Users/crankshaw/veloxms/data/news-classifier-from-tomer'
                    }

sample_config_1 = {
                    # 'hostname': "localhost",
                    'sparkMaster': "ec2-54-161-45-155.compute-1.amazonaws.com",
                    'veloxPartitions': json.dumps(partitions),
                    'models': {
                        'matrixfact': matrixfact_config,
                        'newsgroups': newsgroups_config
                    }
                }

sample_config_2 = {
                    'sparkMaster': "local[2]",
                    'sparkDataLocation': "/Users/tomerk11/Desktop/velox-data",
                    'veloxPartitions': json.dumps(partitions),
                    'models': [
                        { 'matrixfact': json.dumps(matrixfact_config) },
                    ]
                }

def add_kv_to_etcd(key_path, value):

    payload = {'value': str(value)}
    r = requests.put(etcd_host + key_path, data=payload)
    pp.pprint(r.json())


def add_settings(key, cfg):
    if isinstance(cfg, dict):
        for k, v in cfg.items():
            add_settings(key + "/" + k, v)
    elif isinstance(cfg, list):
        for v in cfg:
            add_settings(key, v)
    elif key != '':
        add_kv_to_etcd(key, cfg)
    else:
        print "Error: trying to add config with no key"


def main():
    add_settings(base_path, sample_config_2)






if __name__=='__main__':
    main()

