#!/usr/bin/env python

import sys
import json
import os
import requests
import random
from time import sleep
import subprocess

min_item = 1
max_item = 65133
min_user = 1
max_user = 71567

def test_predict_item(requests_file):

    baseurl = 'http://localhost:8080/%(resource)s/%(item)d/%(user)d'
    # i = 0
    with open(os.path.expanduser(requests_file)) as f:
        for line in f:
            splits = line.split('::')
            u_id = int(splits[0])
            m_id = int(splits[1])
            actual_rating = float(splits[2])
            req_str = baseurl % {'resource': 'predict-item-materialized', 'item': m_id, 'user': u_id}
            r = requests.get(req_str)
            prediction = r.json()
            # i += 1
            # if i > 3:
            #     break

def run_tests(ratings_file, out_file):
    users, items = read_ratings(ratings_file)
    print 'finished reading user and item ids, found %d users and %d items' % (len(users), len(items))
    num_trials = 10000
    # num_trials = 10
    set_sizes = (1, 10, 25, 50, 75, 100, 150, 200, 500, 1000)
    all_results = {}
    for s in set_sizes:
        print "starting set %d" % s
        subprocess.call(['./start_server.sh'], cwd='/home/ubuntu/velox-modelserver')
        print "velox started?"
        sleep(20)
        result = test_base_itemset(s, num_trials, users, items)
        all_results[s] = result
        print result
        subprocess.call(['./stop_server.sh'], cwd='/home/ubuntu/velox-modelserver')
        sleep(5)
    with open(out_file, 'w') as f:
        json.dump(all_results, f)





def test_base_itemset(set_size, numtrials, users, items):
    posturl = 'http://localhost:8080/%(resource)s/%(user)d'
    for i in range(numtrials):
        itemset = random.sample(items, set_size)
        user = random.sample(users, 1)[0]
        # itemset_json = json.dumps(itemset)
        url = posturl % {'resource': 'predict-item-materialized', 'user': user}
        itemset_json = json.dumps({'items': itemset})
        # print itemset_json
        headers = {'Content-type': 'application/json'}
        r = requests.post(url, data=itemset_json, headers=headers)
        # print r.raw.read(10)
        # print r.json()
    # get metrics
    metrics_req = requests.get('http://localhost:8081/metrics')
    base_itemset_timing = metrics_req.json()[u'timers'][u'edu.berkeley.veloxms.resources.PredictFromMaterializedResource.predictionFromBaseSet']
    return base_itemset_timing
    # print 'mean: %f stdev: %f' % (base_itemset_timing[u'mean'], base_itemset_timing[u'stddev'])

def read_ratings(ratings_file):
    users = []
    items = []
    with open(os.path.expanduser(ratings_file)) as f:
        for line in f:
            splits = line.split('::')
            u_id = long(splits[0])
            m_id = long(splits[1])
            users.append(u_id)
            items.append(m_id)

    return (frozenset(users), frozenset(items))


if __name__=='__main__':
    command = sys.argv[1]
    if command == 'baseItems':
        ratings_file = sys.argv[2]
        set_size = int(sys.argv[3])
        num_trials = int(sys.argv[4])
        print "Running base item set test with set size: %d" % set_size
        test_base_itemset(ratings_file, set_size, num_trials)
    elif command == 'runTests':
        ratings_file = sys.argv[2]
        out_file = sys.argv[3]
        run_tests(ratings_file, out_file)
    else:
        print '%s not a valid command' % (command)

    # main(sys.argv[1])
    # min_max(sys.argv[1])
