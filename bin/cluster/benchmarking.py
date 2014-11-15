import argparse
from time import sleep
from datetime import datetime
from velox_common import *


def run_benchmark():
    cluster = get_cluster(args)
    assign_hosts(cluster)

    benchmark_cmd = run_client_bench(cluster, num_reqs=1000, percent_obs=0.2, throttle_reqs=200)
    get_metrics(cluster, 'b1', header=benchmark_cmd)















if __name__ == '__main__':
    run_benchmark()
