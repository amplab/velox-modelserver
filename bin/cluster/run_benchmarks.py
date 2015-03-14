from fabric.api import *
from fabric.colors import green as _green, yellow as _yellow
from fabric.contrib.console import confirm
from fabric.contrib.files import append
from hosts import velox_hosts
from pymongo import MongoClient

from cluster_deploy import *

VELOX_CLIENT_BENCHMARK_CLASS = "edu.berkeley.veloxms.client.VeloxWorkloadDriver"



class ClientResults:
    def __init__(self,
            hostname,
            num_predictions,
            num_observations,
            num_failed,
            num_success,
            total_thru,
            pred_thru,
            obs_thru,
            duration):
        self.hostname = hostname
        self.num_pred = num_predictions
        self.num_obs = num_observations
        self.num_failed = num_failed
        self.num_success = num_success
        self.total_thru = total_thru
        self.pred_thru = pred_thru
        self.obs_thru = obs_thru
        self.duration = duration

class ServerResults:
    def __init__(self,
            hostname,
            metrics_json,
            cache_json,
            cpu_info = None
            # pred_metrics_json,
            # obs_metrics_json,
            # gc_metrics_json
            ):
        self.hostname = hostname
        # self.pred_metrics_json = pred_metrics_json
        # self.obs_metrics_json = obs_metrics_json
        # self.gc_metrics_json = gc_metrics_json
        self.metrics_json = metrics_json
        self.cache_json = cache_json
        # if cpu_info is not None:
        #     self.cpu_info = cpu_info



@task
def run_full_benchmark():
    """Main entry point for benchmarking. Configure run here.

    """
    with settings(warn_only=True):
        local('rm status_file')
    benchmark_name = "micro_caching_mf"
    local("mkdir -p %s" % benchmark_name)
    coll_name = benchmark_name
    users = 1000
    items = 50
    model_dim = [25, 50, 100, 150, 200]
    storage_type = "jvmRandom"
    heap_size = 100
    gc = GARBAGE_COLLECTOR
    num_reqs = 50000
    model_type = "MatrixFactorizationModel"
    percent_training_data = 5.0
    percent_obs = 0.0
    max_conc = 300
    cache_sum = False
    cache_features = False
    cache_preds = [True, False]

    # with open('status_file', 'a') as f:
    #     f.write("\n\nstarting benchmark %s" % benchmark_name)
    #
    # for cp in cache_preds:
    #     for md in model_dim:
    #         bench_cfg = BenchmarkConfig(
    #                 md,
    #                 users,
    #                 items,
    #                 storage_type,
    #                 heap_size,
    #                 gc,
    #                 num_reqs,
    #                 percent_obs,
    #                 max_conc,
    #                 model_type,
    #                 percent_training_data,
    #                 -1,
    #                 cache_sum,
    #                 cache_features,
    #                 cp)
    #         single_benchmark_run(bench_cfg, coll_name)
    #         with open('status_file', 'a') as f:
    #             f.write("%s\n" % str(bench_cfg.__dict__))
    #         sleep(10)

    benchmark_name = "micro_caching_news"
    local("mkdir -p %s" % benchmark_name)
    coll_name = benchmark_name
    model_dim = 20
    doc_length = [50, 75, 100, 150, 200]
    model_type = "NewsgroupsModel"
    percent_obs = 0.0
    cache_sum = False
    cache_features = [False]
    cache_preds = [False]
    # with open('status_file', 'a') as f:
    #     f.write("\n\nstarting benchmark %s\n" % benchmark_name)
    #
    # for cp in cache_preds:
    #     for cf in cache_features:
    #         for dl in doc_length:
    #             bench_cfg = BenchmarkConfig(
    #                     model_dim,
    #                     users,
    #                     items,
    #                     storage_type,
    #                     heap_size,
    #                     gc,
    #                     num_reqs,
    #                     percent_obs,
    #                     max_conc,
    #                     model_type,
    #                     percent_training_data,
    #                     dl,
    #                     cache_sum,
    #                     cf,
    #                     cp)
    #             single_benchmark_run(bench_cfg, coll_name)
    #             with open('status_file', 'a') as f:
    #                 f.write("%s\n" % str(bench_cfg.__dict__))
    #             sleep(10)

    benchmark_name = "micro_updates_news"
    local("mkdir -p %s" % benchmark_name)
    coll_name = benchmark_name
    users = 1000
    items = 100
    model_dim = 20
    doc_length = [50, 75, 100, 150, 200]
    storage_type = "jvmRandom"
    heap_size = 100
    gc = GARBAGE_COLLECTOR
    num_reqs = 10000
    model_type = "NewsgroupsModel"
    percent_training_data = 80.0
    percent_obs = 0.3
    max_conc = 300
    cache_sum = [True, False]
    cache_features = [True, False]
    cache_preds = False
    with open('status_file', 'a') as f:
        f.write("\n\nstarting benchmark %s\n" % benchmark_name)

    for dl in doc_length:
        for cs in cache_sum:
            for cf in cache_features:
                bench_cfg = BenchmarkConfig(
                        model_dim,
                        users,
                        items,
                        storage_type,
                        heap_size,
                        gc,
                        num_reqs,
                        percent_obs,
                        max_conc,
                        model_type,
                        percent_training_data,
                        dl,
                        cs,
                        cf,
                        cache_preds)
                single_benchmark_run(bench_cfg, coll_name)
                with open('status_file', 'a') as f:
                    f.write("%s\n" % str(bench_cfg.__dict__))
                sleep(10)

@task
def insert_into_mongo(result, coll_name):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    db.add_son_manipulator(KeyTransform(".", "_"))
    coll = db[coll_name]
    doc_id = coll.insert(result)
    puts("Successfully inserted into MongoDB with id: %s" % str(doc_id))

@task
def get_commit_hash():
    res = local("git rev-parse HEAD", capture=True)
    # print "RESULT IS: %s" % res

def single_benchmark_run(bench_cfg, coll_name):
    # bench_cfg = BenchmarkConfig(100, 100000, 50000, "jvmRandom", 45,
    #                        GARBAGE_COLLECTOR, 10000, 0.1, 200)
    cmd_restart_velox(benchcfg=bench_cfg, rm_logs='y')
    cmd_run_client_bench(benchcfg=bench_cfg)
    sleep(5)
    server_results, client_results = cmd_get_results()
    git_hash = local("git rev-parse HEAD", capture=True)
    format_str = '%Y-%m-%d-%H-%M-%S'
    localnow = strftime(format_str, localtime())
    all_results = {
            'config': bench_cfg.__dict__,
            'cluster': get_default_cluster(
                          len(velox_hosts.servers),
                          len(velox_hosts.clients)).__dict__,
            'timestamp': time(),
            'commit_hash': git_hash,
            'servers': server_results,
            'clients': client_results,
            'localnow': localnow
            }

    results_base = '%s/results_%s.json'
    with open(results_base % (coll_name, localnow), 'w') as f:
        json.dump(all_results, f, sort_keys=True, indent=4, separators=(',', ': '))

    all_results['date'] = datetime.datetime.utcnow(),
    insert_into_mongo(all_results, coll_name)



@task
@parallel
def cmd_get_results():
    client_results = execute(collect_client_results, role='clients')
    server_results = execute(collect_server_results, role='servers')
    return (server_results, client_results)


@task
def collect_server_results():
    metrics_url = "http://%s:%d/metrics" % (env.host, METRICS_PORT)
    cache_url = "http://%s:%d/cachehits" % (env.host, 8080)
    metrics_req = requests.get(metrics_url)
    result = metrics_req.json()
    cache_req = requests.get(cache_url)
    cache_results = cache_req.json()
    print cache_results
    # metrics = {}
    # timing = metrics_req.json()[u'timers']
    # json.dump(timing_info, f, sort_keys=True, indent=4, separators=(',', ': '))
    server_result = ServerResults(env.host, result, cache_results)
    # server_result = ServerResults(env.host, result, "")
    return server_result.__dict__


@task
def collect_client_results():
    results_file = StringIO()
    with cd(VELOX_ROOT):
        get("client_output.txt", results_file)
    lines = results_file.getvalue().split('\n')
    results = {}
    for l in lines:
        if len(l) > 0:
            splits = l.split(": ")
            field_name = splits[0]
            field_val = splits[1]
            results[field_name] = field_val
    # puts(results)
    client_results = ClientResults(
            env.host,
            int(results['num_pred']),
            int(results['num_obs']),
            int(results['failures']),
            int(results['successes']),
            float(results['total_thru']),
            float(results['pred_thru']),
            float(results['obs_thru']),
            float(results['duration']))
    puts(client_results.__dict__)
    return client_results.__dict__


@task
@parallel
def cmd_run_client_bench(benchcfg=None):
    wait_servers_start()
    if benchcfg == None:
        benchcfg = get_default_bench()

    execute(upload_server_partitions, role='clients')

    execute(start_client, benchcfg, role='clients')
