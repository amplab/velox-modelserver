#!/usr/bin/env python

import sys
import json
import os
import requests

baseurl = 'http://localhost:8080/%(resource)s/%(item)d/%(user)d'

def main(requests_file):

    # i = 0
    with open(os.path.expanduser(requests_file)) as f:
        for line in f:
            splits = line.split('::')
            u_id = int(splits[0])
            m_id = int(splits[1])
            actual_rating = float(splits[2])
            req_str = baseurl % {'resource': 'predict-item', 'item': m_id, 'user': u_id}
            r = requests.get(req_str)
            prediction = r.json()
            # i += 1
            # if i > 3:
            #     break





if __name__=='__main__':
    main(sys.argv[1])
