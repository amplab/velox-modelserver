# from fabric.api import env, run, roles, execute, abort, task, local, runs_once, settings, get, put
from fabric.api import *
from fabric.colors import green as _green, yellow as _yellow
from fabric.contrib.console import confirm
from fabric.contrib.files import append
from hosts import velox_hosts
from boto import ec2
import yaml
import requests
import json
from requests import exceptions
from time import sleep, time, strftime, gmtime, localtime
import os.path
from StringIO import StringIO
import datetime
import time
from pymongo import MongoClient
from mongo_utils import *

########################## GLOBAL SETTINGS ################################

### MONGO SETTINGS ###
MONGO_HOST = "ec2-54-161-215-250.compute-1.amazonaws.com"
MONGO_PORT = 27017

# MONGO_DB = "velox"
MONGO_DB = "cs286_final_project"
MONGO_COLLECTION = "crankshaw_results"



### FABRIC SETTINGS ###
env.roledefs = {
        'servers': velox_hosts.servers,
        'clients': velox_hosts.clients,
        'all': velox_hosts.all_hosts,
        'mongo': MONGO_HOST
}

env.user = "ubuntu"
env.key_filename = ["~/.ssh/aws_rsa"]
# env.connection_attempts=10
# env.timeout = 30



class BenchmarkConfig:

    def __init__(self,
            model_dim,
            num_users,
            num_items,
            storage_type,
            heap_size,
            gc,
            num_reqs,
            percent_obs,
            max_conc_reqs,
            model_type,
            percent_training_data,
            doc_length,
            cache_partial_sum,
            cache_features,
            cache_predictions):
            # commit_hash):
        # Model/server settings
        self.model_dim = model_dim
        self.model_type = model_type
        self.num_users = num_users
        self.num_items = num_items
        self.storage_type = storage_type
        self.heap_size = heap_size
        self.gc = gc

        # Workload settings
        self.num_reqs = num_reqs
        self.percent_obs = percent_obs
        self.max_conc_reqs = max_conc_reqs # aka throttle_reqs
        self.percent_training_data = percent_training_data
        self.doc_length = doc_length
        self.cache_partial_sum = cache_partial_sum
        self.cache_features = cache_features
        self.cache_predictions = cache_predictions
        # self.commit_hash = commit_hash
        self.pre_cache = cache_partial_sum

### AWS SETTING ###
class Cluster:
    """Stores description of a running ec2 cluster

    This is a convenience class to keep track of various
    attributes of an ec2 cluster (currently only spot instance
    clusters are supported).

    Attributes:
        region (str): AWS region of cluster
        cluster_id (str): tag to identify instances in this cluster
        instance_type (str): ec2 instance type
        ami (str): AMI used to initiate instances
        spot_price (double): max spot price 
        security_group (str): name of the security group
        num_servers (int): The number of Velox servers in this cluster
        num_clients (int): The number of client machines in this cluster
    """



    # region = 'us-east-1'
    # cluster_id = 'crankshaw-veloxms',
    # instance_type = 'r3.2xlarge',
    # ami = 'ami-10119778',
    # spot_price = 1.5,
    # security_group = 'veloxms'
    def __init__(self,
            region,
            cluster_id,
            instance_type,
            ami,
            spot_price,
            security_group,
            num_servers,
            num_clients):
        self.region = region
        self.cluster_id = cluster_id
        self.instance_type = instance_type
        self.ami = ami
        self.spot_price = spot_price
        self.security_group = security_group
        self.num_servers = num_servers
        self.num_clients = num_clients


class Host:
    def __init__(self, ip, regionName, cluster_id, instanceid, status):
        self.ip = ip
        self.regionName = regionName
        self.cluster_id = cluster_id
        self.instanceid = instanceid
        self.status = status

### VELOX SETTINGS ###
# HEAP_SIZE_GB = 45
VELOX_SERVER_JAR = "veloxms-core/target/veloxms-core-0.0.1-SNAPSHOT.jar"
VELOX_CLIENT_JAR = "veloxms-client/target/veloxms-client-0.0.1-SNAPSHOT.jar"

VELOX_SERVER_CLASS = "edu.berkeley.veloxms.VeloxEntry"


VELOX_ROOT = "/home/ubuntu/velox-modelserver"

NGRAM_FILE = "data/20_news_ngrams.txt"

METRICS_PORT = 8081

GARBAGE_COLLECTOR = "UseConcMarkSweepGC"

### Benchmark settings ###


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

# JSONEncoder().encode(analytics)

###########################################################################

@task
# @roles('servers')
@parallel
def upload_lib():
    run("mkdir -p ~/velox_lib")
    put("../../lib/*", "~/velox_lib")



@task
@roles('servers', 'clients')
@parallel
def kill_everything():
    # TODO Figure out how to stop etcd
    with settings(warn_only=True):
        run("pkill -9 java")
        sleep(2)
        run("pkill -9 java")

        sudo("killall etcd")
        sleep(2)
        sudo("killall etcd")



@task
# @roles('servers', 'clients')
def cmd_install_ykit():
    """Install yourkit on all servers

    Default roles are servers, clients
    """
    execute(install_ykit, roles=['servers', 'clients'])

@task
# @roles('servers', 'clients')
# @runs_once
@parallel
def cmd_build_velox(
        git_remote="git@github.com:amplab/velox-modelserver.git",
        branch="develop",
        with_tachyon='n',
        with_ykit='n',
        with_deploy_key='n',
        localkey="personalrepo-veloxms-deploy"):
    """Build velox on all machines.

    Pull latest updates from the specified repo and branch and rebuild velox
    on all machines. Roles default to servers and clients.

    Arguments:
        git_remote (str, optional): The git remote repo to pull code from.
            Must have specified a deploy key with appropriate permissions.
            Defaults to "git@github.com:amplab/velox-modelserver.git".
        branch (str, optional): The branch of the repo to build.
            Defaults to "develop".
        with_tachyon (str, optional): y/n option of whether to build tachyon
            as well as velox. Defaults to 'n'. Specify 'y' to build tachyon.
        with_ykit (str, optional): y/n option of whether to install yourkit 
            as well as velox. Defaults to 'n'. Specify 'y' to install ykit.
        with_deploy_key (str, optional): y/n option of whether to upload a
            an ssh key to get permissions to pull from the git repo. Needs
            to be done every time you pull from a new repo.
            Defaults to 'n'. Specify 'y' to upload.
        localkey (str, optional): The name of the local ssh key to use
            as a deploy key (assumes the key is located in ~/.ssh/). Only
            used if with_deploy_key == 'y'. Default to "personalrepo-veloxms-deploy".

    """
    if with_deploy_key.lower() == 'y':
        execute(upload_deploy_key(localkey), roles=['clients', 'servers'])
    if with_tachyon.lower() == 'y':
        execute(install_tachyon, roles=['clients', 'servers'])
    if with_ykit.lower() == 'y':
        execute(install_ykit, roles=['clients', 'servers'])

    
    execute(build_velox, git_remote, branch, roles=['clients', 'servers'])
        


@task
@parallel
def install_ykit():
    with hide('stdout', 'stderr'):
        ykit_version = 14114
        run("wget http://www.yourkit.com/download/yjp-2014-build-%d-linux.tar.bz2" % ykit_version)
        run("tar -xvf yjp-2014-build-%d-linux.tar.bz2" % ykit_version)
        run("mv yjp-2014-build-%d yourkit" % ykit_version)

@task
@parallel
def install_etcd():
    with hide('stdout', 'stderr'):
        with settings(warn_only=True):
            if run("test -d ~/etcd").failed:
                run("curl -L  https://github.com/coreos/etcd/releases/download/v2.0.5/etcd-v2.0.5-linux-amd64.tar.gz -o etcd-v2.0.5-linux-amd64.tar.gz")
                run("tar xzvf etcd-v2.0.5-linux-amd64.tar.gz; mv etcd-v2.0.5-linux-amd64 etcd")
        put("../etcd_utils/etcd.conf", "/etc/init/etcd.conf", use_sudo=True)
        put("../etcd_utils/start_etcd.sh", "~/start_etcd.sh", mirror_local_mode=True)

@task
@parallel
def cmd_start_new_etcd_cluster():
    token = "velox_etcd_%d" % int(time.time())
    for part in range(len(velox_hosts.servers)):
        execute(start_etcd, part, velox_hosts.servers, token, host=velox_hosts.servers[part])



@task
# @parallel
def start_etcd(partition, all_peers, cluster_token):
    """
        partition is the partition number of this velox server
        all_peers is a list of all servers to create the etcd cluster with
        cluster_token is a unique name to give the cluster (can just be cluster_TIME or something"

    """

    # cmd = (
    #         "~/etcd/etcd -name velox%(partition)d -initial-advertise-peer-urls http://%(hostname)s:2380 "
    #         "-listen-peer-urls http://%(hostname)s:2380 "
    #         "-initial-cluster-token %(cluster_token)s "
    #         "-initial-cluster %(init_cluster)s "
    #         "-initial-cluster-state new "
    #         "-listen-client-urls http://%(hostname)s:4001 "
    #         )
    hostname = all_peers[partition]
    init_cluster = ""
    for i in range(len(all_peers)):
        init_cluster += "velox%d=http://%s:2380," %(i, all_peers[i])
    # cmd_args = {"partition": partition,
    #             "hostname": hostname,
    #             "init_cluster": init_cluster,
    #             "cluster_token": cluster_token
    #             }

    run("echo export ETCD_PARTITION=%s > ~/etcd_variables.sh" % partition)
    run("echo export HOSTNAME=%s >> ~/etcd_variables.sh" % hostname)
    run("echo export ETCD_CLUSTER_TOKEN=%s >> ~/etcd_variables.sh" % cluster_token)
    run("echo export ETCD_CLUSTER=%s >> ~/etcd_variables.sh" % init_cluster)
    
    sudo("start etcd")


    # with hide('stdout', 'stderr'):
    #     run(cmd % cmd_args)

@task
@parallel
def cmd_stop_etcd():
    execute(stop_etcd, role='servers')


@task
def stop_etcd():
    sudo("stop etcd")
    


@task
@parallel
def install_tachyon():
    with hide('stdout', 'stderr'):
        puts("Installing tachyon")
        run("rm -rf ~/tachyon")
        with cd("~/"):
            run("git clone https://github.com/dcrankshaw/tachyon.git")
        with cd("~/tachyon"):
            run("git checkout velox-build")
            run("mvn package -DskipTests")
            run("mvn install:install-file -Dfile=core/target/tachyon-0.6.0-SNAPSHOT-jar-with-dependencies.jar "
                "-DgroupId=org.tachyonproject -DartifactId=tachyon-parent "
                "-Dversion=0.6.0-SNAPSHOT -Dpackaging=jar")
@task
@parallel
def build_velox(
        git_remote="git@github.com:amplab/velox-modelserver.git",
        branch="develop"):
    # check for tachyon, if not install it
    with hide('stdout', 'stderr'):
        with settings(warn_only=True):
            if run("test -d ~/tachyon").failed:
                abort("Please install Tachyon first")
            if run("test -d ~/velox-modelserver").failed:
                # puts("Cloning Velox on %s" % env.host_string)
                run("git clone %s" % git_remote)
        with cd("~/velox-modelserver"):
            run("git stash")
            with settings(warn_only=True):
                run("git clean -f")
                run("git remote rm vremote")
            run("git remote add vremote %s" % git_remote)
            run("git checkout master")
            with settings(warn_only=True):
                run("git branch -D veloxbranch")
            run("git fetch vremote")
            run("git checkout -b veloxbranch vremote/%s" % branch)
            run("git reset --hard vremote/%s" % branch)
            run("mvn package")

        # re-upload server_partitions after rebuilding
        put("../../conf/server_partitions.txt", "~/velox-modelserver/conf/server_partitions.txt")
        with cd(VELOX_ROOT):
            run("mkdir -p lib/")
            run("cp ~/velox_lib/* lib/")

        with settings(warn_only=True):
            if local("test -f ../../%s" % NGRAM_FILE).succeeded:
                with cd(VELOX_ROOT):
                    put("../../%s" % NGRAM_FILE, NGRAM_FILE)

# @roles('clients')



@task
def upload_server_partitions():
    with cd(VELOX_ROOT):
        put("../../conf/server_partitions.txt", "conf/server_partitions.txt")


@task
@parallel
def start_client(benchcfg):
    base_cmd = ("pkill -9 java; "
                # "nohup "
                "java -XX:+%(gc)s -Xmx%(heap_size)dg -Xms%(heap_size)dg "
                "-Dlog4j.configuration=file:%(velox_home)s/conf/log4j.properties "
                "-cp %(velox_home)s/%(client_jar)s:conf/log4j.properties "
                "%(client_class)s "
                "--numRequests %(num_reqs)d "
                "--veloxURLFile %(velox_home)s/conf/server_partitions.txt "
                "--numUsers %(num_users)d "
                "--numItems %(num_items)d "
                "--numPartitions %(num_partitions)d "
                "--percentObs %(percent_obs)f "
                # "--connTimeout %(conn_timeout)d "
                "--throttleRequests %(max_conc_reqs)d "
                "--model %(model_type)s "
                "--ngramFile %(velox_home)s/%(ngram_file)s "
                "--docLength %(doc_length)d "
                # " & sleep 5; exit 0"
                )

    cmd_args = {'heap_size': benchcfg.heap_size,
                'velox_home': VELOX_ROOT,
                'num_reqs': benchcfg.num_reqs,
                'num_users': benchcfg.num_users,
                'num_items': benchcfg.num_items,
                'num_partitions': len(velox_hosts.servers),
                'percent_obs': benchcfg.percent_obs,
                # 'conn_timeout': 10000,
                'max_conc_reqs': benchcfg.max_conc_reqs,
                'client_jar': VELOX_CLIENT_JAR,
                'client_class': VELOX_CLIENT_BENCHMARK_CLASS,
                'gc': benchcfg.gc,
                'model_type': benchcfg.model_type,
                'ngram_file': NGRAM_FILE,
                'doc_length': benchcfg.doc_length
                }
    cmd_str = base_cmd % cmd_args
    # with hide('stdout', 'stderr'):
    run(cmd_str)


# wait until servers have finished configuring
def wait_servers_start():
    url = 'http://%s:8080/predict/matrixfact'
    payload = json.dumps({'uid': 10, 'context': 4})
    headers = {'Content-type': 'application/json'}
    puts("Waiting for servers to come up")
    while True:
        try:
            for s in velox_hosts.servers:
                r = requests.post(url % s, payload, headers=headers)
            # in case the other server needs to finish, sleep an extra 10 seconds
            sleep(2)
            break
        except exceptions.ConnectionError: 
            puts("Sleeping another 10 seconds...")
            sleep(10)

def get_default_bench():
    return BenchmarkConfig(50, 1000, 500, "jvmRandom", 10,
                           GARBAGE_COLLECTOR, 200, 0.2, 200,
                           "MatrixFactorizationModel", 2.0, 1, False, False, False)


# TODO separate out a VeloxConfig needed to start velox from the benchmark config
# @roles('servers')
@task
@parallel
# @runs_once
def cmd_restart_velox(benchcfg=None, rm_logs='n', profile='n'):
    prof = False
    if profile.lower() == 'y':
        prof = True
    if benchcfg == None:
        benchcfg = get_default_bench()
    with settings(warn_only=True):
        execute(stop_velox_server, role='servers')
        sleep(2)
        execute(stop_velox_server, role='servers')
        if rm_logs.lower() == 'y':
            execute(remove_logs, role='servers')

    # update config.yml to match settings
    config_loc = "../../conf/config.yml"
    config = {}
    with open("%s.template" % config_loc, 'r') as template: #, open("/tmp/config.yml", "w") as new_conf:
        config = yaml.load(template)
    # fill in model-specific configuration
    config["models"] = {}
    if benchcfg.model_type is "MatrixFactorizationModel":
        config = add_matrix_factor_to_config(config, benchcfg)
    elif benchcfg.model_type is "NewsgroupsModel":
        config = add_newsgroups_to_config(config, benchcfg)
    with open(config_loc, 'w') as new_conf:
        yaml.dump(config, new_conf, default_flow_style=False)

    execute(start_velox_server, benchcfg, profile=prof, role='servers')

def add_matrix_factor_to_config(config_dict, benchcfg):
    config_dict["models"]["matrixfact"] = {
            "modelType": "MatrixFactorizationModel",
            "modelSize": benchcfg.model_dim,
            "partitionFile": "/home/ubuntu/velox-modelserver/conf/server_partitions.txt",
            "cachePartialSums": benchcfg.cache_partial_sum,
            "cacheFeatures": benchcfg.cache_features,
            "cachePredictions": benchcfg.cache_predictions,

            "storage": {
                "items": {
                    "storageType": "jvmRandomItems",
                    "numItems": benchcfg.num_items
                    },
                "users": {
                    "storageType": "jvmRandomUsers",
                    "totalNumUsers": benchcfg.num_users,
                    "numPartitions": len(velox_hosts.servers)
                    },
                "ratings": {
                    "storageType": "jvmRandomObservations",
                    "totalNumUsers": benchcfg.num_users,
                    "numItems": benchcfg.num_items,
                    "numPartitions": len(velox_hosts.servers),
                    "percentOfItems": benchcfg.percent_training_data,
                    }
                }
            }
    return config_dict

def add_newsgroups_to_config(config_dict, benchcfg):
    config_dict["models"]["newsgroups"] = {
            "modelType": "NewsgroupsModel",
            "modelSize": benchcfg.model_dim,
            "partitionFile": "/home/ubuntu/velox-modelserver/conf/server_partitions.txt",
            "cachePartialSums": benchcfg.cache_partial_sum,
            "cacheFeatures": benchcfg.cache_features,
            "cachePredictions": benchcfg.cache_predictions,
            "modelLoc": "/home/ubuntu/velox-modelserver/data/news-classifier-from-tomer",
            "storage": {
                "users": {
                    "storageType": "jvmRandomUsers",
                    "totalNumUsers": benchcfg.num_users,
                    "numPartitions": len(velox_hosts.servers)
                    },
                "ratings": {
                    "storageType": "jvmRandomDocs",
                    "ngramFile": "%s/%s" % (VELOX_ROOT, NGRAM_FILE),
                    "percentOfItems": benchcfg.percent_training_data,
                    "docLength": benchcfg.doc_length,
                    "totalNumUsers": benchcfg.num_users,
                    "numItems": benchcfg.num_items,
                    "numPartitions": len(velox_hosts.servers)
                    }
                }
            }
    return config_dict
    
@task
def remove_logs():
    run("rm /home/ubuntu/velox-modelserver/logs/*")


@task
def start_velox_server(benchcfg, profile=False):

    config_loc = "../../conf/config.yml"
    with cd(VELOX_ROOT):
        # put("../../%s" % NGRAM_FILE, NGRAM_FILE)


        put("../../conf/config.yml", "conf/config.yml")
    pstr = ""
    if profile:
        pstr += "-agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so"

    # TODO: http://www.ics.uzh.ch/~dpotter/howto/daemonize,
    # https://github.com/robertcboll/x-dropwizard-daemon
    server_cmd_template = (
                        "nohup "
                        " java %(pstr)s -XX:+%(gc)s -Xms%(heap_size)dg -Xmx%(heap_size)dg "
                        "-Dlog4j.configuration=file:%(log4j_file)s "
                        "-Ddw.hostname=$VELOX_HOSTNAME "
                        # "-cp %(velox_root)s/%(server_jar)s:%(velox_root)s/lib/* "
                        "-cp %(velox_root)s/%(server_jar)s "
                        "%(server_class)s server "
                        "%(velox_root)s/conf/config.yml "
                        "& sleep 5; exit 0"
                        )
    cmd_args = {'pstr': pstr,
                'heap_size': benchcfg.heap_size,
                'log4j_file': "%s/conf/log4j.properties" % VELOX_ROOT,
                'gc': benchcfg.gc,
                # 'host': host,
                # "sid": sid,
                'velox_root': VELOX_ROOT,
                'server_class': VELOX_SERVER_CLASS,
                'server_jar': VELOX_SERVER_JAR}
    server_cmd = server_cmd_template % cmd_args
    with prefix("source ~/ec2_variables.sh"):
        with hide('stdout', 'stderr'):
            run(server_cmd)



@task
@parallel
def stop_velox_server():
    run("pkill -9 -f VeloxEntry")
    sleep(2)


@task
@parallel
def upload_deploy_key(localkey):
    sshconfig_str = """
Host github.com
    IdentityFile /home/ubuntu/.ssh/%s
    StrictHostKeyChecking no

""" % localkey

    put("~/.ssh/%s" % localkey, "/home/ubuntu/.ssh/")
    append("/home/ubuntu/.ssh/config", sshconfig_str)
    sudo("chmod 600 /home/ubuntu/.ssh/*")



def get_default_cluster(servers, clients, instance_type='r3.4xlarge'):
    default_cluster = Cluster('us-east-1',
                              'crankshaw-veloxms',
                              instance_type,
                              'ami-10119778',
                              1.0,
                              'veloxms',
                              servers,
                              clients)

    return default_cluster


# Setup a cluster
@task
@runs_once
def launch_cluster(
        num_servers='2',
        num_clients='2',
        localkey="personalrepo-veloxms-deploy",
        instance_type='r3.2xlarge'):
    servers = int(num_servers)
    clients = int(num_clients)
    default_cluster = get_default_cluster(servers, clients, instance_type)

    num_instances = servers + clients
    if not confirm("Spinning up %d instances in %s, okay?" % (num_instances, default_cluster.region)):
        abort("Aborting at user request")
    puts("Setting up security group")
    setup_security_group(default_cluster)

    conn = ec2.connect_to_region(default_cluster.region)
    puts("Requesting spot instances")
    reservations = conn.request_spot_instances(
            default_cluster.spot_price,
            default_cluster.ami,
            count=num_instances,
            instance_type=default_cluster.instance_type,
            security_groups=[default_cluster.security_group])
    wait_all_hosts_up(default_cluster, num_instances)

    # claim instances
    puts("Claiming instances")
    instances = get_instances(default_cluster.region, None)
    # puts(instances)
    hosts = []
    instance_ids = []
    for i in instances:
        hosts.append(i.ip)
        instance_ids.append(i.instanceid)
    conn.create_tags(instance_ids, {'cluster': default_cluster.cluster_id})

    # assign hosts
    # local("mkdir -p hosts")
    server_ips = []
    client_ips = []
    puts(hosts)
    for host in hosts:
        if len(server_ips) < servers:
            server_ips.append(host)
        else:
            client_ips.append(host)
    with open('hosts/velox_hosts.py', 'w') as hosts_file:
        all_servers = ', '.join('"%s"' % s for s in server_ips)
        all_clients = ', '.join('"%s"' % c for c in client_ips)
        all_hosts = ', '.join('"%s"' % h for h in hosts)
        hosts_file.write('servers = [%s]\n' % all_servers)
        hosts_file.write('clients = [%s]\n' % all_clients)
        hosts_file.write('all_hosts = [%s]\n' % all_hosts)
    puts("Importing new roledefs")
    reload(velox_hosts)
    env.roledefs = {
            'servers': velox_hosts.servers,
            'clients': velox_hosts.clients,
            'all': velox_hosts.all_hosts,
            'mongo': MONGO_HOST
    }

    # try connecting
    num_attempts = 10
    while True:
        try: 
            result = execute(touch, roles=["servers", "clients"])
            break
        except SystemExit, e:
            num_attempts -= 1
            if num_attempts == 0:
                abort("Could not connect")
            puts("Couldn't connect. Trying %d more times..." % num_attempts)
            sleep(15)

    # set up partitioning info
    partitions_file = "../../conf/server_partitions.txt"
    with open(partitions_file, "w") as f:
        lines = []
        part = 0
        for s in server_ips:
            lines.append("%s: %d\n" % (s, part))
            part += 1
        f.writelines(lines)

    # install tachyon, yourkit, and clone velox
    execute(upload_deploy_key, localkey, role='all')

    puts("installing Tachyon")
    execute(install_tachyon, role='all')
    puts("installing etcd")
    execute(install_etcd, role='servers')

    puts("building velox")
    execute(build_velox,
            git_remote="git@github.com:dcrankshaw/velox-modelserver.git",
            branch="develop",
            role='all')
    for h in velox_hosts.all_hosts:
        execute(set_hostname, h, host=h)

    execute(upload_lib, role='all')
    # puts("installing ykit")
    # execute(install_ykit, role='all')


# can get public ip_addr on instance via:
# curl http://169.254.169.254/latest/meta-data/public-ipv4
@task
def set_hostname(hostname):
    run("echo export VELOX_HOSTNAME=%s >> ~/ec2_variables.sh" % hostname)
    run("source ~/ec2_variables.sh")
    put("../../conf/server_partitions.txt", "~/velox-modelserver/conf/server_partitions.txt")

@task
def touch():
    return run("touch ~/text.txt")

## Cluster management
def setup_security_group(cluster):
    conn = ec2.connect_to_region(cluster.region)
    try :
        if len(filter(lambda x: x.name == cluster.security_group, conn.get_all_security_groups())) != 0:
            conn.delete_security_group(name=cluster.security_group)
        group = conn.create_security_group(cluster.security_group, "VeloxMS EC2 all-open SG")
        group.authorize('tcp', 0, 65535, '0.0.0.0/0')
    except Exception as e:
        print("Oops; couldn't create a new security group (%s). This is probably fine: %s"
                % (cluster.security_group, str(e)))

def wait_all_hosts_up(cluster, num_instances):
    print("Waiting for instances in %s to start..." % cluster.region)
    while True:
        numInstancesInRegion = get_num_running_instances(cluster.region, None)
        if numInstancesInRegion >= num_instances:
            break
        else:
            print("Got %d of %d hosts; sleeping..." % (numInstancesInRegion, num_instances))
        sleep(10)
    print("All instances in %s alive!" % cluster.region)

    # Since ssh takes some time to come up
    print("Waiting for instances to warm up... ")
    sleep(20)
    print("Awake!")

def get_num_running_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status == "running"])

# Passing cluster_id=None will return all hosts without a tag.
def get_instances(regionName, cluster_id):
    hosts = []

    conn = ec2.connect_to_region(regionName)

    filters={'instance-state-name':'running'}
    if cluster_id is not None:
        filters['tag:cluster'] = cluster_id
    reservations = conn.get_all_instances(filters=filters)
    instances = []
    for reservation in reservations:
        instances += reservation.instances
    for i in instances:
        if cluster_id is None and len(i.tags) != 0:
            continue
        hosts.append(Host(str(i.public_dns_name), regionName, cluster_id, str(i.id), str(i.state)))
    return hosts

