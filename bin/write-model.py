#!/usr/bin/env python

import numpy as np
import sys


num_users = 200
num_user_features = 45
num_items = 2000
num_item_features = 120



def main(fname):
    user_matrix = np.random.random_integers(0, 2000, (num_users,num_user_features))
    item_matrix = np.random.random_integers(0, 2000, (num_items, num_item_features))
    np.savetxt(fname + "_users.csv", user_matrix, delimiter=',', fmt='%04u')
    np.savetxt(fname + "_items.csv", item_matrix, delimiter=',', fmt='%04u')
    


if __name__=='__main__':
    main(sys.argv[1])
