# Common helper functions

from os import system
import subprocess
from time import sleep
from boto import ec2
from os.path import expanduser

KEY_NAME = "velox"
VELOX_INTERNAL_PORT_START = 8080
VELOX_FRONTEND_PORT_START = 9000

ZOOKEEPER_PORT = 2181

VELOX_BASE_DIR="/home/ubuntu/velox"

HEAP_SIZE_GB = 240
VELOX_JAR_LOCATION = "assembly/target/scala-2.10/velox-assembly-0.1.jar"
VELOX_SECURITY_GROUP = "velox"

DEFAULT_INSTANCE_TYPE = "cr1.8xlarge"

VELOX_SERVER_CLASS = "edu.berkeley.velox.server.VeloxServer"
VELOX_CLIENT_BENCH_CLASS = "edu.berkeley.velox.benchmark.ClientBenchmark"

AMIs = {'us-west-2': 'ami-8885e5b8',
        'us-east-1': 'ami-b7dbe3de'}

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


def provision_spot(cluster, instance_type=DEFAULT_INSTANCE_TYPE, spot_price=1.5, placement_group="velox", **kwargs):
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
        group = conn.create_security_group(group_name, "Velox EC2 all-open SG")
        group.authorize('tcp', 0, 65535, '0.0.0.0/0')
    except Exception as e:
        pprint("Oops; couldn't create a new security group (%s). This is probably fine: " + str(e) % (group_name))


# Assigns hosts to clusters (and specifically as servers, clients)
# Also logs the assignments in the hosts/ files.
def assign_hosts(cluster):
    system("mkdir -p hosts")

    hosts = get_instances(cluster.regionName, cluster.clusterID)
    pprint("Assigning %d hosts to %s:% s... " % (len(hosts), cluster.regionName, cluster.clusterID))

    cluster.allocateHosts(hosts[:cluster.getNumHosts()])
    frontend_servers = []
    internal_servers = []
    sid = 0
    for server in cluster.servers:
        frontend_servers.append("%s:%d" % (server.ip, VELOX_FRONTEND_PORT_START+sid))
        internal_servers.append("%s:%d" % (server.ip, VELOX_INTERNAL_PORT_START+sid))
        sid += 1
    cluster.internal_cluster_str = ",".join(internal_servers)
    cluster.frontend_cluster_str = ",".join(frontend_servers)

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

def rebuild_servers(git_remote, branch, deploy_key=None, **kwargs):
    if deploy_key:
        upload_file("all-hosts", deploy_key, "/home/ubuntu/.ssh")
        run_cmd("all-hosts", "echo 'IdentityFile /home/ubuntu/.ssh/%s' >> /home/ubuntu/.ssh/config; chmod go-r /home/ubuntu/.ssh/*" % (deploy_key.split("/")[-1]))

    pprint('Rebuilding clients and servers...')
    run_cmd_in_velox('all-hosts',
                     ("git remote rm vremote; "
                      "git remote add vremote %s; "
                      "git checkout master; "
                      "git branch -D veloxbranch; "
                      "git fetch vremote; "
                      "git checkout -b veloxbranch vremote/%s; "
                      "git reset --hard vremote/%s; "
                      "sbt/sbt assembly; "
                      # "cd external/ycsb; "
                      # "./package-ycsb.sh"
                      ) % (git_remote, branch, branch))
    pprint('Rebuilt to %s/%s!' % (git_remote, branch))

def start_servers_with_zk(cluster, heap_size, network_service, buffer_size, sweep_time, profile=False, profile_depth=2, reset_zk=True, **kwargs):
    HEADER = "cd /home/ubuntu/velox/; rm *.log;"

    pstr = ""
    if profile:
        # pstr += "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.server.txt" % (profile_depth)
        pstr += "-agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so"
    # kill any java processes
    run_cmd("all-hosts", "pkill -9 java")
    # this kills zookeeper as well so we have to restart it
    # TODO: figure out a better way to shut down velox cluster
    start_zookeeper_cluster(cluster, reset_zk)

    base_cmd = (HEADER + "java %(pstr)s -XX:+UseParallelGC -Xms%(heap_size)dG "
               "-Xmx%(heap_size)dG -cp %(jar_loc)s %(server_class)s "
               "-p %(internal_port)d -f %(frontend_port)d --network_service %(net_service)s "
               "--buffer_size %(buf_size)d --sweep_time %(sweep_time)d "
               "--ip_address %(ip_addr)s --num_servers %(num_servers)d "
               "-z %(zk_servers)s 1>server.log 2>&1 & ")
    zk_servers = ",".join(["%s:%d" % (s.ip, ZOOKEEPER_PORT) for s in cluster.servers])

    cmd_args = {'pstr': pstr,
                'heap_size': heap_size,
                'jar_loc': VELOX_JAR_LOCATION,
                'server_class': VELOX_SERVER_CLASS,
                'net_service': network_service,
                'buf_size': buffer_size,
                'sweep_time': sweep_time,
                'num_servers': cluster.numServers,
                'zk_servers': zk_servers}


    for sid in range(0, cluster.numServers):
        cmd_args['frontend_port'] = VELOX_FRONTEND_PORT_START+sid
        cmd_args['internal_port'] = VELOX_INTERNAL_PORT_START+sid
        cmd_args['ip_addr'] = cluster.servers[sid].ip
        server_cmd = base_cmd % cmd_args
        server = cluster.servers[sid]
        pprint("Starting velox server with zookeeper on [%s]" % server.ip)
        start_cmd_disown_nobg(server.ip, server_cmd)

def kill_velox_local():
    system("ps ax | grep Velox | grep java |  sed \"s/[ ]*//\" | cut -d ' ' -f 1 | xargs kill")

def start_servers_local(num_servers, network_service, buffer_size, sweep_time, profile=False, profile_depth=2, **kwargs):
    kill_velox_local()

    serverConfigStr = ",".join(["localhost:"+str(VELOX_INTERNAL_PORT_START+id) for id in range(0, num_servers)])

    base_cmd = ("java %(pstr)s -XX:+UseParallelGC -Xms28m -Xmx512m -cp %(jar_loc)s %(server_class)s "
                "-p %(internal_port)d "
                "-f %(frontend_port)d "
                "--network_service %(net_service)s "
                "--buffer_size %(buf_size)d "
                "--ip_address %(ip_addr)s "
                "--sweep_time %(swp_time)d "
                "-z localhost:%(zk_port)d "
                "--num_servers %(num_servers)d 1> /tmp/server-%(sid)d.log 2>&1 &")

    cmd_args = {'jar_loc': VELOX_JAR_LOCATION,
                'server_class': VELOX_SERVER_CLASS,
                'net_service': network_service,
                'buf_size': buffer_size,
                'ip_addr': "localhost",
                'swp_time': sweep_time,
                'zk_port': ZOOKEEPER_PORT,
                'num_servers': num_servers}

    for sid in range(0, num_servers):
        if profile:
            cmd_args['pstr'] = "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.server-%d.txt" % (profile_depth, sid)
        else:
            cmd_args['pstr'] = ""
        cmd_args['internal_port'] = VELOX_INTERNAL_PORT_START+sid
        cmd_args['frontend_port'] = VELOX_FRONTEND_PORT_START+sid
        cmd_args['sid'] = sid
        server_cmd = base_cmd % cmd_args
        print server_cmd
        system(server_cmd)

    pprint("Started servers! Logs in /tmp/server-*.log")

def client_bench_local_single(num_servers, network_service, buffer_size, sweep_time, profile, profile_depth, parallelism, read_pct, ops, max_time, latency, test_index, **kwargs):
    clientConfigStr = ",".join(["localhost:"+str(VELOX_FRONTEND_PORT_START+id) for id in range(0, num_servers)])
    if profile:
        pstr = "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.client.txt" % profile_depth
    else:
        pstr = ""
    base_cmd = ("java %(pstr)s -XX:+UseParallelGC -Xms512m -Xmx2G -cp %(jar_loc)s %(server_class)s "
              # "-m %(client_str)s --parallelism %(parallelism)d --pct_reads %(read_pct)f --ops %(num_ops)d "
              "--parallelism %(parallelism)d --pct_reads %(read_pct)f --ops %(num_ops)d "
              "--timeout %(timeout)d --network_service %(net_service)s --buffer_size %(buf_size)d "
              "--sweep_time %(sweep_time)d --latency %(latency)s "
              "--test_index %(test_index)s "
              "-z localhost:%(zk_port)d "
              "--num_servers %(num_servers)d "
              "--run --load")

    cmd_args = {'pstr': pstr,
                'jar_loc': VELOX_JAR_LOCATION,
                'server_class': VELOX_CLIENT_BENCH_CLASS,
                # 'client_str': clientConfigStr,
                'parallelism': parallelism,
                'read_pct': read_pct,
                'num_ops': ops,
                'timeout': max_time,
                'net_service': network_service,
                'buf_size': buffer_size,
                'sweep_time': sweep_time,
                'latency': latency,
                'test_index': test_index,
                'zk_port': ZOOKEEPER_PORT,
                'num_servers': num_servers}
    runcmd = base_cmd % cmd_args
    print runcmd
    system(runcmd)

#  -agentlib:hprof=cpu=samples,interval=20,depth=3,monitor=y
def run_velox_client_bench(cluster, network_service, buffer_size, sweep_time, profile, profile_depth, parallelism, read_pct, ops, max_time, latency, test_index, heap_size=HEAP_SIZE_GB, **kwargs):
    pstr = ""

    if profile:
        pstr += "-agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so"
        #hprof = "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.client.txt" % (profile_depth)

    zk_servers = ",".join(["%s:%d" % (s.ip, ZOOKEEPER_PORT) for s in cluster.servers])

    base_cmd = ("pkill -9 java; "
                "java %(pstr)s -XX:+UseParallelGC -Xms%(heap_size)dG -Xmx%(heap_size)dG -cp %(jar_loc)s %(server_class)s "
                "--parallelism %(parallelism)d --pct_reads %(read_pct)f --ops %(num_ops)d "
                "--timeout %(timeout)d --network_service %(net_service)s --buffer_size %(buf_size)d "
                "--sweep_time %(sweep_time)d --latency %(latency)s "
                "--test_index %(test_index)s "
                "--num_servers %(num_servers)d "
                "-z %(zk_servers)s --run 2>&1 | tee client.log"
                )


    cmd_args = {'pstr': pstr,
                'jar_loc': VELOX_JAR_LOCATION,
                'server_class': VELOX_CLIENT_BENCH_CLASS,
                'heap_size': heap_size,
                # 'client_str': cluster.frontend_cluster_str
                'parallelism': parallelism,
                'read_pct': read_pct,
                'num_ops': ops,
                'timeout': max_time,
                'net_service': network_service,
                'buf_size': buffer_size,
                'sweep_time': sweep_time,
                'latency': latency,
                'test_index': test_index,
                'zk_servers': zk_servers,
                'num_servers': cluster.numServers}

    cmd = base_cmd % cmd_args

    print "loading table"
    run_cmd_single(cluster.clients[0].ip, "cd " + VELOX_BASE_DIR + ";" + cmd.replace("--run", "--load"))

    run_cmd_in_velox("all-clients", cmd)

def run_ycsb_local(num_servers, parallelism, read_pct, ops, max_time, skip_rebuild, valuesize=1, recordcount=10000, request_distribution="zipfian", workload="workloads/workloada", **kwargs):
    clientConfigStr = ",".join(["localhost:"+str(VELOX_FRONTEND_PORT_START+id) for id in range(0, num_servers)])

    ycsb_cmd = (("cd external/ycsb; "
                 "bin/ycsb run velox "
                 "-s "
                 "-P %s "
                 "-threads %d "
                 "-p readproportion=%s "
                 "-p updateproportion=%s "
                 "-p fieldlength=%d "
                 "-p fieldcount=1 "
                 "-p recordcount=%d "
                 "-p operationcount=%d "
                 "-p requestdistribution=%s "
                 "-p maxexecutiontime=%d "
                 "-p cluster=%s") %
                (workload, parallelism, read_pct, 1-read_pct, valuesize, recordcount, ops, request_distribution, max_time, clientConfigStr))

    if not skip_rebuild:
        pprint("Rebuilding YCSB")
        system("cd external/ycsb; ./package-ycsb.sh")
        pprint("YCSB rebuilt!")

    pprint("Loading YCSB on single client...")
    load_cmd = ycsb_cmd.replace(" run ", " load ")
    print("cd external/ycsb; "+load_cmd)
    system(load_cmd)
    pprint("YCSB loaded!")

    pprint("Running YCSB")
    print("cd external/ycsb; "+ycsb_cmd)
    system(ycsb_cmd)
    pprint("YCSB complete!")

def run_ycsb(cluster, parallelism, read_pct, ops, max_time, skip_rebuild, valuesize=1, recordcount=10000, request_distribution="zipfian", workload="workloads/workloada", **kwargs):
    ycsb_cmd = (("pkill -9 java;"
                 "cd /home/ubuntu/velox/external/ycsb; "
                 "bin/ycsb run velox "
                 "-s "
                 "-P %s "
                 "-threads %d "
                 "-p readproportion=%s "
                 "-p updateproportion=%s "
                 "-p fieldlength=%d "
                 "-p fieldcount=1 "
                 "-p recordcount=%d "
                 "-p operationcount=%d "
                 "-p requestdistribution=%s "
                 "-p maxexecutiontime=%d "
                 "-p cluster=%s > run_out.log 2> run_err.log") %
                (workload, parallelism, read_pct, 1-read_pct, valuesize, recordcount, ops, request_distribution, max_time, cluster.frontend_cluster_str))

    if not skip_rebuild:
        pprint("Rebuilding YCSB")
        run_cmd("all-clients", "cd /home/ubuntu/velox/external/ycsb; ./package-ycsb.sh")
        pprint("YCSB rebuilt!")

    pprint("Loading YCSB on single client...")
    load_cmd = ycsb_cmd.replace(" run ", " load ").replace("run_", "load_")
    run_cmd_single(cluster.clients[0].ip, "cd /home/ubuntu/velox/external/ycsb; "+load_cmd)
    pprint("YCSB loaded!")

    pprint("Running YCSB")
    run_cmd_in_velox("all-clients", ycsb_cmd)
    pprint("YCSB complete!")

def mkdir(d):
    system("mkdir -p %s" % d)

def fetch_logs(cluster, runid, output_dir, **kwargs):
    log_dir = "%s/%s/" % (output_dir, runid)
    mkdir(log_dir)
    for server in cluster.servers:
        s_dir = log_dir+"/S"+server.ip
        mkdir(s_dir)
        fetch_file_single_compressed(server.ip, VELOX_BASE_DIR+"/*.log", s_dir)
        fetch_file_single_compressed(server.ip, VELOX_BASE_DIR+"/external/ycsb/*.log", s_dir)

    for client in cluster.clients:
        c_dir = log_dir+"/C"+client.ip
        mkdir(c_dir)
        fetch_file_single_compressed(client.ip, VELOX_BASE_DIR+"/*.log", c_dir)
        fetch_file_single_compressed(client.ip, VELOX_BASE_DIR+"/external/ycsb/*.log", c_dir)

# deletes zookeeper data and log. Zookeeper comes back up with clean state.
def start_zookeeper_cluster(cluster, reset=False):
    if reset:
        run_cmd("all-servers", "rm -rf /tmp/zookeeper/version-2; rm -rf /tmp/zookeeper_log/version-2")
    run_cmd("all-servers", "/home/ubuntu/zookeeper-3.4.5/bin/zkServer.sh start")


# deletes zookeeper data and log. Zookeeper comes back up with clean state.
def start_zookeeper_cluster_local(reset=False):
    if reset:
        system("rm -rf /tmp/zookeeper/version-2; rm -rf /tmp/zookeeper_log/version-2")
    system("/tmp/zookeeper-3.4.5/bin/zkServer.sh start")


def install_zookeeper_cluster(cluster, conf, dl=True, delete=True):
    # mirror = "http://www.webhostingjams.com/mirror/apache/zookeeper/current/zookeeper-3.4.5.tar.gz"
    zk_src_path = "external/zookeeper/zookeeper-3.4.5.tar.gz"
    system("cp %s conf/zoo.cfg" % conf)
    with open("conf/zoo.cfg", 'a') as cfg:
        for i in range(cluster.numServers):
            cfg.write("server.%d=%s:2888:3888\n" % (i + 1, cluster.servers[i].ip))



    if delete:
        run_cmd("all-hosts", "/home/ubuntu/zookeeper-3.4.5/bin/zkServer.sh stop; "
                "rm -rf /home/ubuntu/zookeeper-3.4.5; "
                "rm -rf /tmp/zookeeper; "
                "rm -rf /tmp/zookeeper_log; "
                "rm /home/ubuntu/zookeeper-3.4.5.tar.gz; ")

    if dl:
        upload_file("all-servers", zk_src_path, "/home/ubuntu/")

        # deploy_command = (("wget -P /home/ubuntu %s; "
        deploy_command = ("sudo apt-get install -y openjdk-7-jdk; "
                           # make sure that data and log directories exist or ZooKeeper won't start
                          "mkdir -p /tmp/zookeeper; "
                          "mkdir -p /tmp/zookeeper_log; "
                          "tar zxvf ~/zookeeper-3.4.5.tar.gz;")
        run_cmd("all-servers", deploy_command)

    upload_file("all-servers", "conf/zoo.cfg", "/home/ubuntu/zookeeper-3.4.5/conf/")
    # set zookeeper server id - has to be in datDir directory
    for i in range(cluster.numServers):
        run_cmd_single(cluster.servers[i].ip,
                       "echo %d > /tmp/zookeeper/myid" % (i + 1))

    system("rm conf/zoo.cfg")
        


def install_zookeeper_cluster_local(conf, install=True, delete=True):
    # mirror = "http://www.webhostingjams.com/mirror/apache/zookeeper/current/zookeeper-3.4.5.tar.gz"
    zk_src_path = "external/zookeeper/zookeeper-3.4.5.tar.gz"
    system("cp %s conf/zoo.cfg" % conf)

    if delete:
        system("/tmp/zookeeper-3.4.5/bin/zkServer.sh stop; "
                "rm -rf /tmp/zookeeper-3.4.5; "
                "rm -rf /tmp/zookeeper; "
                "rm -rf /tmp/zookeeper_log; ")

    if install:
        # deploy_command = (("wget -P /home/ubuntu %s; "
        deploy_command = (# make sure that data and log directories exist or ZooKeeper won't start
                          "mkdir -p /tmp/zookeeper; "
                          "mkdir -p /tmp/zookeeper_log; "
                          "tar zxvf external/zookeeper/zookeeper-3.4.5.tar.gz -C /tmp 2> /dev/null")
        system(deploy_command)

    system("mv conf/zoo.cfg /tmp/zookeeper-3.4.5/conf/")
    # set zookeeper server id - has to be in datDir directory
    system("echo 1 > /tmp/zookeeper/myid")


