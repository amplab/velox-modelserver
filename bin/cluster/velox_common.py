# Common helper functions

import os
from os import system
import subprocess
from time import sleep
from boto import ec2
from os.path import expanduser
import yaml
import requests
import json

KEY_NAME = "veloxms-aws-deploy"
VOLUME_SIZE = 50
VELOX_BASE_DIR="/home/ubuntu/velox-modelserver"

METRICS_PORT = 8081

HEAP_SIZE_GB = 45
VELOX_SERVER_JAR = "veloxms-core/target/veloxms-core-0.0.1-SNAPSHOT.jar"
VELOX_CLIENT_JAR = "veloxms-client/target/veloxms-client-0.0.1-SNAPSHOT.jar"

VELOX_SERVER_CLASS = "edu.berkeley.veloxms.VeloxApplication"

VELOX_CLIENT_BENCHMARK_CLASS = "edu.berkeley.veloxms.client.VeloxWorkloadDriver"

GARBAGE_COLLECTOR = "UseConcMarkSweepGC"

VELOX_SECURITY_GROUP = "veloxms"

DEFAULT_INSTANCE_TYPE = "r3.2xlarge"

# VELOX_SERVER_CLASS = "edu.berkeley.velox.server.VeloxServer"
# VELOX_CLIENT_BENCH_CLASS = "edu.berkeley.velox.benchmark.ClientBenchmark"


SNAPSHOTs = {'us-east-1': 'snap-de003767'}

AMIs = {'us-east-1': 'ami-10119778'}

def run_cmd(hosts, cmd, user="ubuntu", time=1000):
    cmd = "pssh -i -t %d -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (time, user, hosts, cmd)
    print cmd
    # print "You may need to install pssh (sudo pip install pssh)"
    system(cmd)

def run_cmd_single(host, cmd, user="ubuntu", time = None):
    cmd = "ssh -o StrictHostKeyChecking=no %s@%s \"%s\"" % (user, host, cmd)
    print cmd
    system(cmd)

def run_cmd_single_bg(host, cmd, user="ubuntu", time = None):
    cmd = "ssh -o StrictHostKeyChecking=no %s@%s \"%s\" &" % (user, host, cmd)
    print cmd
    system(cmd)

def start_cmd_disown(host, cmd, user="ubuntu"):
    run_cmd_single_bg(host, cmd+" & disown", user)


def start_cmd_disown_nobg(host, cmd, user="ubuntu"):
    run_cmd_single_bg(host, cmd+" disown", user)

def run_process_single(host, cmd, user="ubuntu", stdout=None, stderr=None):
    subprocess.call("ssh %s@%s \"%s\"" % (user, host, cmd),
                    stdout=stdout, stderr=stderr, shell=True)

def upload_file(hosts, local_path, remote_path, user="ubuntu"):
    system("cp %s /tmp" % (local_path))
    script = local_path.split("/")[-1]
    system("pscp -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt /tmp/%s %s" % (user, hosts, script, remote_path))

def upload_file_single(host, local, remote, user="ubuntu"):
    system("scp -o StrictHostKeyChecking=no '%s' %s@%s:%s" % (local, user, host, remote))

def run_script(hosts, script, user="ubuntu"):
    upload_file(hosts, script.split(" ")[0], "/tmp", user)
    run_cmd(hosts, "bash /tmp/%s" % (script.split("/")[-1]), user)

def fetch_file_single(host, remote, local, user="ubuntu"):
    system("scp -o StrictHostKeyChecking=no %s@%s:%s %s" % (user, host, remote, local))

def fetch_file_single_compressed(host, remote, local, user="ubuntu"):
    print("scp -C -o StrictHostKeyChecking=no %s@%s:%s '%s'" % (user, host, remote, local))

    system("scp -C -o StrictHostKeyChecking=no %s@%s:%s '%s'" % (user, host, remote, local))

def fetch_file_single_compressed_bg(host, remote, local, user="ubuntu"):
    system("scp -C -o StrictHostKeyChecking=no %s@%s:%s '%s' &" % (user, host, remote, local))

def get_host_ips(hosts):
    return open("hosts/%s.txt" % (hosts)).read().split('\n')[:-1]

def sed(file, find, repl):
    iOpt = ''
    print 'sed -i -e %s \'s/%s/%s/g\' %s' % (iOpt, escape(find), escape(repl), file)
    system('sed -i -e %s \'s/%s/%s/g\' %s' % (iOpt, escape(find), escape(repl), file))

def escape(path):
    return path.replace('/', '\/')

def get_node_ips():
    ret = []
    system("ec2-describe-instances > /tmp/instances.txt")
    system("ec2-describe-instances --region us-west-2 >> /tmp/instances.txt")
    for line in open("/tmp/instances.txt"):
        line = line.split()
        if line[0] != "INSTANCE" or line[5] != "running":
            continue
        # addr, externalip, internalip, ami
        ret.append((line[3], line[13], line[14], line[1]))
    return ret

def get_matching_ip(host):
    for h in get_node_ips():
        if h[0] == host:
            return h[1]

def pprint(str):
    print '\033[94m%s\033[0m' % str



## EC2 stuff

def run_cmd_in_velox(hosts, cmd, user='ubuntu'):
    run_cmd(hosts, "cd %s; %s" % (VELOX_BASE_DIR, cmd), user)

class Cluster:
    def __init__(self, regionName, clusterID, numServers, numClients):
        self.regionName = regionName
        self.clusterID = clusterID
        self.numServers = numServers
        self.servers = []
        self.numClients = numClients
        self.clients = []

    def allocateHosts(self, hosts):
        for host in hosts:
            if len(self.servers) < self.numServers:
                self.servers.append(host)
            elif len(self.clients) < self.numClients:
                self.clients.append(host)

        assert len(self.getAllHosts()) == self.getNumHosts(), "Don't have exactly as many hosts as I expect!" \
                                                          " (expect: %d, have: %d)" \
                                                          % (self.getNumHosts(), len(self.getAllHosts()))

    def getAllHosts(self):
        return self.servers + self.clients

    def getNumHosts(self):
        return self.numServers + self.numClients

class Host:
    def __init__(self, ip, regionName, cluster_id, instanceid, status):
        self.ip = ip
        self.regionName = regionName
        self.cluster_id = cluster_id
        self.instanceid = instanceid
        self.status = status


# Passing cluster_id=None will return all hosts without a tag.
def get_instances(regionName, cluster_id):
    system("rm -f instances.txt")
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

def get_zone(conn, cluster_id):
    # conn = ec2.connect_to_region(regionName)

    filters={'instance-state-name':'running'}
    if cluster_id is not None:
        filters['tag:cluster'] = cluster_id
    reservations = conn.get_all_instances(filters=filters)

    instances = []
    for reservation in reservations:
        instances += reservation.instances

    zone = instances[0].placement
    return zone


def mount_instance_disks():
    print "Mounting instance volumes..."
    mount_cmd = ("sudo rm -rf /mnt*; "
                 "sudo mkdir /mnt; "
                 "sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 /dev/xvdb 2> /dev/null; "
                 "sudo mount -o defaults,noatime,nodiratime /dev/xvdb /mnt; "
                 "sudo mkdir -p /mnt/tachyon-files; "
                 "sudo chmod -R a+w /mnt*; ")
    run_cmd("all-hosts", mount_cmd)

def build_tachyon_maven():
    install_tachyon_cmd = ("rm -rf ~/tachyon; "
                           "git clone https://github.com/dcrankshaw/tachyon.git; "
                           "cd tachyon; "
                           "git checkout velox-build; "
                           "mvn package -DskipTests; "
                           "mvn install:install-file "
                           "-Dfile=core/target/tachyon-0.6.0-SNAPSHOT-jar-with-dependencies.jar "
                           "-DgroupId=org.tachyonproject -DartifactId=tachyon-parent "
                           "-Dversion=0.6.0-SNAPSHOT -Dpackaging=jar;")
    # only need to run Tachyon on servers, but it should be built and installed
    # on all hosts because it is needed to build Velox
    print "Installing tachyon..."
    run_cmd("all-hosts", install_tachyon_cmd)


# must have already called assign_hosts()
def install_tachyon(cluster, generate_key=True):
    # Need to format and mount instance volumes
    # mount_instance_disks()

    build_tachyon_maven()

    # set slaves file in Tachyon
    upload_file("all-hosts", "hosts/all-servers.txt", "/home/ubuntu/tachyon/conf/slaves")
    tachyon_master = cluster.servers[0].ip

    # set tachyon variables
    # TODO automatically compute memory based on instance mem:
    # cat /proc/meminfo | grep MemTotal | awk '{print $2}' gets instance mem in kB
    s3_access = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret = os.environ['AWS_SECRET_ACCESS_KEY']
    if (s3_access is None or s3_secret is None):
        pprint("No S3 access key or secret key provided")
        sys.exit(1)

    tachyon_vars_cmd = ("echo export VELOX_TACHYON_MASTER=%s > ~/ec2-variables.sh; "
                        "echo export VELOX_TACHYON_MEM=%dGB >> ~/ec2-variables.sh "
                        "echo export S3_ACCESS_KEY=%s >> ~/ec2-variables.sh "
                        "echo export S3_SECRET_KEY=%s >> ~/ec2-variables.sh "
                        % (tachyon_master, 20, s3_access, s3_secret))
    run_cmd("all-servers", tachyon_vars_cmd)

    if (generate_key):
        print "Generating cluster's SSH key on master..."
        key_setup = ("[ -f ~/.ssh/id_rsa ] || (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys); ")
        run_cmd_single(tachyon_master, key_setup)
        # TODO there are much better ways to do this...
        fetch_file_single(tachyon_master, "~/.ssh/id_rsa", "hosts/")
        fetch_file_single(tachyon_master, "~/.ssh/id_rsa.pub", "hosts/")
        fetch_file_single(tachyon_master, "~/.ssh/authorized_keys", "hosts/")
        upload_file("all-hosts", "hosts/id_rsa", "/home/ubuntu/.ssh/id_rsa")
        upload_file("all-hosts", "hosts/id_rsa.pub", "/home/ubuntu/.ssh/id_rsa.pub")
        upload_file("all-hosts", "hosts/authorized_keys", "/home/ubuntu/.ssh/authorized_keys")

    restart_tachyon()


def restart_tachyon():
    print "Starting Tachyon..."
    start_tachyon_cmd = ("rm /home/ubuntu/tachyon/logs/*; "
                         "/home/ubuntu/tachyon/bin/tachyon format; "
                         "sleep 5; "
                         "/home/ubuntu/tachyon/bin/tachyon-start.sh all SudoMount")

    run_cmd("all-servers", start_tachyon_cmd)
    # TODO how to test to make sure Tachyon was successfully started?

def get_spot_request_ids(regionName):
    system("rm -f instances.txt")
    global AMIs

    conn = ec2.connect_to_region(regionName)
    return [str(i.id) for i in conn.get_all_spot_instance_requests()]

def get_num_running_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status == "running"])

def get_num_nonterminated_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status != "terminated"])

def make_instancefile(name, hosts):
    f = open("hosts/" + name, 'w')
    for host in hosts:
        f.write("%s\n" % (host.ip))
    f.close

def check_for_instances(cluster):
    numRunningAnywhere = 0
    numUntagged = 0
    numRunning = get_num_nonterminated_instances(cluster.regionName, cluster.clusterID)
    numRunningAnywhere += numRunning
    numUntagged += get_num_nonterminated_instances(cluster.regionName, None)

    if numRunningAnywhere > 0:
        pprint("NOTICE: You appear to have %d instances up already." % numRunningAnywhere)
        f = raw_input("Continue without terminating them? ")
        if f != "Y" and f != "y":
            exit(-1)

    if numUntagged > 0:
        pprint("NOTICE: You appear to have %d UNTAGGED instances up already." % numUntagged)
        f = raw_input("Continue without terminating/claiming them? ")
        if f != "Y" and f != "y":
            exit(-1)



def terminate_cluster(cluster, placement_group="velox", **kwargs):
    allHosts = get_instances(cluster.regionName, cluster.clusterID) + get_instances(cluster.regionName, None)
    instance_ids = [h.instanceid for h in allHosts]
    spot_request_ids = get_spot_request_ids(cluster.regionName)

    conn = ec2.connect_to_region(cluster.regionName)

    if len(instance_ids) > 0:
        pprint('Terminating instances (tagged & untagged) in %s...' % cluster.regionName)
        conn.terminate_instances(instance_ids)
    else:
        pprint('No instances to terminate in %s, skipping...' % cluster.regionName)

    if len(spot_request_ids) > 0:
        pprint('Cancelling spot requests in %s...' % cluster.regionName)
        conn.cancel_spot_instance_requests(spot_request_ids)
    else:
        pprint('No spot requests to cancel in %s, skipping...' % cluster.regionName)

    #conn.delete_placement_group(placement_group)


def provision_spot(
        cluster,
        instance_type=DEFAULT_INSTANCE_TYPE,
        spot_price=1.5,
        placement_group="veloxms",
        **kwargs):
    global AMIs

    num_instances = cluster.numServers + cluster.numClients
    setup_security_group(cluster.regionName)

    f = raw_input("spinning up %d spot instances in %s; okay? (y/N) " %
                  (num_instances, cluster.regionName))

    if f != "Y" and f != "y":
        exit(-1)

    conn = ec2.connect_to_region(cluster.regionName)
    '''
    try:
        conn.create_placement_group(placement_group)
    except:
        print "Placement group exception "+placement_group
    '''
    reservations = conn.request_spot_instances(spot_price,
                                               AMIs[cluster.regionName],
                                               count=num_instances,
                                               instance_type=instance_type,
                                               security_groups=[VELOX_SECURITY_GROUP])
    #                                           placement_group=placement_group)

def provision_instances(cluster, instance_type=DEFAULT_INSTANCE_TYPE, placement_group="velox", **kwargs):
    global AMIs

    num_instances = cluster.numServers + cluster.numClients
    setup_security_group(cluster.regionName)

    f = raw_input("spinning up %d instances in %s; okay? (y/N) " %
                  (num_instances, cluster.regionName))

    if f != "Y" and f != "y":
        exit(-1)

    conn = ec2.connect_to_region(cluster.regionName)
    '''
    try:
        conn.create_placement_group(placement_group)
    except:
        print "Placement group exception "+placement_group
    '''
    reservations = conn.run_instances(AMIs[cluster.regionName],
                                      min_count=num_instances,
                                      max_count=num_instances,
                                      instance_type=instance_type,
                                      security_groups=[VELOX_SECURITY_GROUP])
    #                                   placement_group=placement_group)

def wait_all_hosts_up(cluster):
    pprint("Waiting for instances in %s to start..." % cluster.regionName)
    num_hosts = cluster.numServers + cluster.numClients
    while True:
        numInstancesInRegion = get_num_running_instances(cluster.regionName, None)
        if numInstancesInRegion >= num_hosts:
            break
        else:
            pprint("Got %d of %d hosts; sleeping..." % (numInstancesInRegion, num_hosts))
        sleep(5)
    pprint("All instances in %s alive!" % cluster.regionName)

    # Since ssh takes some time to come up
    pprint("Waiting for instances to warm up... ")
    sleep(10)
    pprint("Awake!")

def claim_instances(cluster):
    instances = get_instances(cluster.regionName, None)
    instanceString = ' '.join([host.instanceid for host in instances])
    pprint("Claiming %s..." % instanceString)
    conn = ec2.connect_to_region(cluster.regionName)
    instances = [i.instanceid for i in instances]
    reservations = conn.create_tags(instances, {"cluster": cluster.clusterID})
    pprint("Claimed!")

def setup_security_group(region, group_name=VELOX_SECURITY_GROUP):
    conn = ec2.connect_to_region(region)
    try :
        if len(filter(lambda x: x.name == group_name, conn.get_all_security_groups())) != 0:
            conn.delete_security_group(name=group_name)
        group = conn.create_security_group(group_name, "VeloxMS EC2 all-open SG")
        group.authorize('tcp', 0, 65535, '0.0.0.0/0')
    except Exception as e:
        pprint("Oops; couldn't create a new security group (%s). This is probably fine: %s"
                % (group_name, str(e)))


# Assigns hosts to clusters (and specifically as servers, clients)
# Also logs the assignments in the hosts/ files.
def assign_hosts(cluster):
    system("mkdir -p hosts")

    hosts = get_instances(cluster.regionName, cluster.clusterID)
    pprint("Assigning %d hosts to %s:% s... " % (len(hosts), cluster.regionName, cluster.clusterID))

    cluster.allocateHosts(hosts[:cluster.getNumHosts()])

    # Finally write the instance files for the regions and everything.
    make_instancefile("all-hosts.txt", cluster.getAllHosts())
    make_instancefile("all-servers.txt", cluster.servers)
    make_instancefile("all-clients.txt", cluster.clients)

    pprint("Assigned all %d hosts!" % len(hosts))



def stop_velox_processes():
    pprint("Terminating java processes...")
    run_cmd("all-hosts", "killall java; pkill java")
    sleep(5)
    pprint('Termination command sent.')

def install_ykit(cluster):
    run_cmd("all-hosts", "wget http://www.yourkit.com/download/yjp-2013-build-13074-linux.tar.bz2")
    run_cmd("all-hosts", "tar -xvf yjp-2013-build-13074-linux.tar.bz2")
    run_cmd("all-hosts", "mv yjp-2013-build-13074 yourkit")
    # master = cluster.clients[0].ip
    # run_cmd_single(master, "wget http://www.yourkit.com/download/yjp-2013-build-13072-linux.tar.bz2")
    # run_cmd("all-hosts", "scp ubuntu@%s:yjp-2013-build-13072-linux.tar.bz2 yjp-2013.tar.bz2" %  master)
    # run_cmd("all-hosts", "tar -xvf yjp-2013.tar.bz2")

    


def rebuild_servers(git_remote, branch, deploy_key=None, clone_repo=False, build_tachyon=False, **kwargs):

    if build_tachyon:
        build_tachyon_maven()


    if deploy_key:
        pprint("Adding deploy key...")
        upload_file("all-hosts", deploy_key, "/home/ubuntu/.ssh")
        run_cmd("all-hosts", "printf 'Host github.com\n\tIdentityFile /home/ubuntu/.ssh/%s\n\tStrictHostKeyChecking no\n' > /home/ubuntu/.ssh/config; chmod go-r /home/ubuntu/.ssh/*" % (deploy_key.split("/")[-1]))

    pprint('Rebuilding clients and servers...')
    if clone_repo:
        clone_cmd = ("cd ~/; "
                     "rm -rf /home/ubuntu/velox-modelserver; "
                     "git clone git@github.com:dcrankshaw/velox-modelserver.git")
        pprint("Cloning velox-modelserver repository...")
        run_cmd('all-hosts', clone_cmd)
    run_cmd_in_velox('all-hosts',
                ("git stash; "
                 "git remote rm vremote; "
                 "git remote add vremote %s; "
                 "git checkout master; "
                 "git branch -D veloxbranch; "
                 "git fetch vremote; "
                 "git checkout -b veloxbranch vremote/%s; "
                 "git reset --hard vremote/%s; "
                 "mvn package; "
                 ) % (git_remote, branch, branch))
    pprint('Rebuilt to %s/%s!' % (git_remote, branch))


def kill_veloxms_servers():
    kill_servers_cmd = "pkill -f VeloxApplication"
    run_cmd("all-hosts", kill_servers_cmd)

def run_my_command(cluster):
    cmd = "killall java; pkill -9 java"
    run_cmd("all-hosts", cmd)

def get_metrics(cluster, file_name, **kwargs):
    url = "http://%s:%d/metrics"
    timing_info = {}
    for sid in range(0, cluster.numServers):
        server = cluster.servers[sid]
        metrics_req = requests.get(url % (server.ip, METRICS_PORT))
        timing = metrics_req.json()[u'timers']
        timing_info[server.ip] = timing
    with open("hosts/%s.json" % file_name, 'w') as f:
        json.dump(timing_info, f, sort_keys=True, indent=4, separators=(',', ': '))


def restart_velox(cluster, heap_size, rm_logs=False, **kwargs):

    kill_veloxms_servers()
    sleep(2)

    velox_root = "/home/ubuntu/velox-modelserver"
    log4j_file = "/home/ubuntu/velox-modelserver/conf/log4j.properties"

    # s3_access = os.environ['AWS_ACCESS_KEY_ID']
    # s3_secret = os.environ['AWS_SECRET_ACCESS_KEY']

    upload_file("all-hosts", "../../conf/config.yml", "/home/ubuntu/velox-modelserver/conf/config.yml")

    if rm_logs:
        print "Clearing log files"
        run_cmd("all-servers", "rm /home/ubuntu/velox-modelserver/logs/*; ")

#     pstr = ""
#     if profile:
#         # pstr += "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.server.txt" % (profile_depth)
#         pstr += "-agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so"
    start_server_cmd = ("java -XX:+(gc)s -Xms%(heap_size)dg -Xmx%(heap_size)dg "
                        "-Dlog4j.configuration=file:%(log4j_file)s "
                        "-Ddw.modelStorage.partition=%(sid)d "
                        # "-Dfs.s3n.awsAccessKeyId=%s "
                        # "-Dfs.s3n.awsSecretAccessKey=%s "
                        "-cp %(velox_root)s/%(server_jar)s "
                        "%(server_class)s server "
                        "%(velox_root)s/conf/config.yml ")
    cmd_args = {"heap_size": heap_size,
                "log4j_file": log4j_file,
                'gc': GARBAGE_COLLECTOR,
                # "sid": sid,
                "velox_root": velox_root,
                "server_class": VELOX_SERVER_CLASS,
                "server_jar": VELOX_SERVER_JAR}

    for sid in range(0, cluster.numServers):
        cmd_args["sid"] = sid
        this_server_cmd = start_server_cmd % cmd_args
        server = cluster.servers[sid]
        pprint("Starting velox modelserver on [%s]" % server.ip)
        start_cmd_disown(server.ip, this_server_cmd)


# TODO yourkit profiling support
def start_servers(cluster, heap_size, use_tachyon=False, **kwargs):
    config_loc = "../../conf/config.yml"

    if use_tachyon:
        restart_tachyon()
        tachyon_master = cluster.servers[0].ip
        config = {}
        with open(config_loc, 'r') as template: #, open("/tmp/config.yml", "w") as new_conf:
            config = yaml.load(template)
        with open(config_loc, 'w') as new_conf:
            config["modelStorage"]["address"] = "tachyon://%s:19998" % tachyon_master
            yaml.dump(config, new_conf, default_flow_style=False)
        upload_file("all-hosts", config_loc, "/home/ubuntu/velox-modelserver/conf/config.yml")
        pprint("updated velox config")

    restart_velox(cluster, heap_size)

def run_client_bench(cluster, **kwargs):

    base_cmd = ("pkill -9 java; "
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
                "--numThreads %(parallelism)d "
                "--connTimeout %(conn_timeout)d "
                "--throttleRequests %(throttle_reqs)d "
                "--statusTime %(status_time)d "
                )

    cmd_args = {'heap_size': HEAP_SIZE_GB,
                'velox_home': '/home/ubuntu/velox-modelserver',
                'num_reqs': 25000,
                'num_users': 100000,
                'num_items': 50000,
                'num_partitions': 2,
                'percent_obs': 0.00,
                'parallelism': 10,
                'conn_timeout': 30000,
                'throttle_reqs': 2000,
                'status_time': 10,
                'client_jar': VELOX_CLIENT_JAR,
                'client_class': VELOX_CLIENT_BENCHMARK_CLASS,
                'gc': GARBAGE_COLLECTOR
                }
    # override defaults
    cmd_args.update(kwargs)
    partitions_file = "../../conf/server_partitions.txt"
    with open(partitions_file, "w") as f:
        lines = []
        for sid in range(0, cluster.numServers):
            lines.append("%d: %s\n" % (sid, cluster.servers[sid].ip))
        f.writelines(lines)
    upload_file("all-clients",
                partitions_file,
                "/home/ubuntu/velox-modelserver/conf/server_partitions.txt")

    cmd_str = base_cmd % cmd_args

    run_cmd_in_velox("all-clients", cmd_str)
    # run_cmd_single(cluster.clients[0].ip, cmd_str)


def mkdir(d):
    system("mkdir -p %s" % d)

# def fetch_logs(cluster, runid, output_dir, **kwargs):
#     log_dir = "%s/%s/" % (output_dir, runid)
#     mkdir(log_dir)
#     for server in cluster.servers:
#         s_dir = log_dir+"/S"+server.ip
#         mkdir(s_dir)
#         fetch_file_single_compressed(server.ip, VELOX_BASE_DIR+"/*.log", s_dir)
#         fetch_file_single_compressed(server.ip, VELOX_BASE_DIR+"/external/ycsb/*.log", s_dir)
#
#     for client in cluster.clients:
#         c_dir = log_dir+"/C"+client.ip
#         mkdir(c_dir)
#         fetch_file_single_compressed(client.ip, VELOX_BASE_DIR+"/*.log", c_dir)
#         fetch_file_single_compressed(client.ip, VELOX_BASE_DIR+"/external/ycsb/*.log", c_dir)

