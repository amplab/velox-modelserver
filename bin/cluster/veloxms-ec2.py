import argparse
from time import sleep
from datetime import datetime
from velox_common import *

# args is argparse.Namespace. returns a Cluster().
def get_cluster(args):
    return Cluster(args.region, args.cluster_id, args.num_servers, args.num_clients)

# sub-commands. args is argparse.Namespace.
def command_launch(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    pprint("Launching velox clusters")
    check_for_instances(cluster)

    if args.no_spot:
        provision_instances(cluster, **kwargs)
    else:
        provision_spot(cluster, **kwargs)

    wait_all_hosts_up(cluster)
    command_claim(args)

def command_claim(args):
    cluster = get_cluster(args)
    pprint("Claiming untagged instances...")
    claim_instances(cluster)

def command_rebuild(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    pprint("Rebuilding velox clusters")
    assign_hosts(cluster)
    kill_veloxms_servers()
    # stopping velox will also kill tachyon right now
    # restart_tachyon()
    rebuild_servers(**kwargs)

def command_terminate(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    terminate_cluster(cluster, **kwargs)

def command_install_ykit(args):
    cluster = get_cluster(args)
    pprint("Installing Yourkit")
    assign_hosts(cluster)
    install_ykit(cluster)

def command_install_tachyon(args):
    cluster = get_cluster(args)
    pprint("Installing Tachyon")
    assign_hosts(cluster)
    install_tachyon(cluster)

def command_restart_veloxms(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    assign_hosts(cluster)
    restart_velox(cluster, HEAP_SIZE_GB, **kwargs)

def command_get_metrics(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    assign_hosts(cluster)
    get_metrics(cluster, 'metrics_result', **kwargs)

def command_runcmd(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    assign_hosts(cluster)
    run_my_command(cluster)

def command_start_velox_servers(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    stop_velox_processes()
    # restart_tachyon()
    assign_hosts(cluster)
    start_servers(cluster, HEAP_SIZE_GB, use_tachyon=True, **kwargs)
    pprint("Velox servers started")

def command_client_bench(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    runid = "CLIENT_BENCH-%s" % (str(datetime.now()).replace(' ', '_').replace(":", '_'))
    pprint("Running simple benchmark")
    assign_hosts(cluster)
    run_client_bench(cluster, **kwargs)
    # get_metrics(cluster, "metrics_%s" % runid, **kwargs)
    pprint("Simple benchmark has completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup velox on EC2')
    ##################################
    ######### global options #########
    ##################################
    parser.add_argument('--cluster_id', '-c', dest='cluster_id', required=True,
                        help='Cluster ID (tag) to use for your instances')
    parser.add_argument('--num_servers', '-ns', dest='num_servers', type=int, required=True,
                        help='Number of server machines per cluster.')
    parser.add_argument('--num_clients', '-nc', dest='num_clients', type=int, required=True,
                        help='Number of client machines per cluster.')
    subparsers = parser.add_subparsers(title='Sub-Commands',
                                       description='Valid Sub-Commands',
                                       help='Sub-Command Help',
                                       dest='subcommand')

    ##################################
    ######### common options #########
    ##################################
    # common cluster config options for ec2
    common_cluster_ec2 = argparse.ArgumentParser(add_help=False)
    common_cluster_ec2.add_argument('--region', '-r', dest='region', default="us-east-1", type=str,
                                    help="EC2 region. [default: %(default)s]")
    # common benchmark options (base)
    common_benchmark = argparse.ArgumentParser(add_help=False)
    common_benchmark.add_argument('--profile', action='store_true',
                                  help='Run JVM with hprof cpu profiling. [default: %(default)s]')
    common_benchmark.add_argument('--profile_depth', dest='profile_depth', default=2, type=int,
                                  help='Stack depth to trace when running profiling. [default: %(default)s]')
    common_benchmark.add_argument('--read_pct', dest='read_pct',
                                  default=0.5, type=float,
                                  help='Percentage of workload operations which are reads. [default: %(default)s]')
    common_benchmark.add_argument('--max_time', dest='max_time',
                                  default=60, type=int,
                                  help='Maximum execution time (in seconds) of the benchmark. [default: %(default)s]')
    common_benchmark.add_argument('--ops', dest='ops',
                                  default=100000, type=int,
                                  help='Number of operations to perform in the benchmark. [default: %(default)s]')

    common_benchmark.add_argument('--heap_size_gb', dest='heap_size',
                                  default=HEAP_SIZE_GB, type=int,
                                  help='Size (in GB) to make the JVM heap. [default: %(default)s]')

    # common benchmark options for ec2 (includes benchmark base)
    common_benchmark_ec2 = argparse.ArgumentParser(add_help=False, parents=[common_benchmark])
    common_benchmark_ec2.add_argument('--output', dest='output_dir', default="./output", type=str,
                                      help='output directory for runs. [default: %(default)s]')
    # common crankshaw options
    common_client_bench = argparse.ArgumentParser(add_help=False)
    common_client_bench.add_argument('--latency', action='store_true',
                                     help='Compute average latency when running THE CRANKSHAW. [default: %(default)s]')
    common_client_bench.add_argument('--test_index', action='store_true',
                                     help='Test index inserts using triggers for THE CRANKSHAW. [default: %(default)s]')
    # common ycsb options
    common_ycsb_bench = argparse.ArgumentParser(add_help=False)
    common_ycsb_bench.add_argument('--skip_rebuild', action='store_true',
                                   help='Skip rebuilding ycsb before running the benchmark. [default: %(default)s]')


    ##################################
    ###### sub-command options #######
    ##################################
    # launch
    parser_launch = subparsers.add_parser('launch', help='Launch EC2 cluster',
                                          parents=[common_cluster_ec2])
    parser_launch.set_defaults(func=command_launch)
    parser_launch.add_argument('--no_spot', dest='no_spot',  action='store_true',
                               help='Don\'t use spot instances. [default: %(default)s]')
    parser_launch.add_argument('--spot_price', dest="spot_price", type=float, default=1.5,
                               help="Spot price in $. [default: %(default)s]")
    parser_launch.add_argument('--instance_type', dest="instance_type", type=str,
                               default=DEFAULT_INSTANCE_TYPE,
                               help="EC2 instance type. [default: %(default)s]")
    parser_launch.add_argument('--placement_group', dest='placement_group', default="VELOX_CLUSTER",
                               help="EC2 placement group. [default: %(default)s]")

    # claim
    parser_claim = subparsers.add_parser('claim', help='Claim non-tagged instances as our own',
                                         parents=[common_cluster_ec2])
    parser_claim.set_defaults(func=command_claim)

    # terminate
    parser_terminate = subparsers.add_parser('terminate', help='Terminate the EC2 cluster and any matching instances',
                                             parents=[common_cluster_ec2])
    parser_terminate.set_defaults(func=command_terminate)

    # rebuild
    parser_rebuild = subparsers.add_parser('rebuild', help='Rebuild veloxms cluster',
                                           parents=[common_cluster_ec2])
    parser_rebuild.set_defaults(func=command_rebuild)
    parser_rebuild.add_argument('--branch', '-b', dest="branch", default="develop",
                        help='Branch to rebuild. [default: %(default)s]')
    parser_rebuild.add_argument('--git_remote', dest="git_remote", default="git@github.com:amplab/velox-modelserver.git",
                        help='Upstream git url. [default: %(default)s]')
    parser_rebuild.add_argument('--deploy_key', dest="deploy_key", default=None,
                        help='Local path to upstream deploy key. [default: %(default)s]')
    parser_rebuild.add_argument('--clone_repo', dest="clone_repo", action='store_true',
                        help='Clone the Velox repo from GitHub. Only needed on fresh clusters. [default: %(default)s]')
    parser_rebuild.add_argument('--build_tachyon', dest="build_tachyon", action='store_true',
                        help='Build tachyon and install to local maven repo. Only needed on fresh clusters. [default: %(default)s]')

    # install_ykit
    parser_install_ykit = subparsers.add_parser('install_ykit', help='Install yourkit',
                                                parents=[common_cluster_ec2])
    parser_install_ykit.set_defaults(func=command_install_ykit)

    # install_tachyon
    parser_install_tachyon = subparsers.add_parser('install_tachyon', help='Install Tachyon on all servers',
                                                parents=[common_cluster_ec2])
    parser_install_tachyon.set_defaults(func=command_install_tachyon)

    ##################################
    ####### benchmark commands #######
    ##################################
    parser_start_velox = subparsers.add_parser('start_velox', help='Start the velox servers',
                                                parents=[common_cluster_ec2])
    parser_start_velox.set_defaults(func=command_start_velox_servers)

    parser_restart_veloxms = subparsers.add_parser('restart_veloxms', help='Restart velox servers',
                                                parents=[common_cluster_ec2])
    parser_restart_veloxms.set_defaults(func=command_restart_veloxms)

    parser_restart_veloxms.add_argument('--rm_logs', dest="rm_logs", action='store_true',
                        help='Clear log files. [default: %(default)s]')

    parser_easy_runcmd = subparsers.add_parser('run_mycommand', help='Shortcut to just run a command on all servers',
                                                parents=[common_cluster_ec2])
    parser_easy_runcmd.set_defaults(func=command_runcmd)

    parser_get_metrics = subparsers.add_parser('get_metrics', help='Get timing metrics info from servers',
                                                parents=[common_cluster_ec2])
    parser_get_metrics.set_defaults(func=command_get_metrics)


    parser_run_bench = subparsers.add_parser('run_benchmark', help='Run simple client benchmark',
                                 parents=[common_cluster_ec2])
    parser_run_bench.set_defaults(func=command_client_bench)

    args = parser.parse_args()
    args.func(args)
