#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from fabric.api import *
from fabric.colors import green as _green, yellow as _yellow
from fabric.contrib.console import confirm
from fabric.contrib.files import append
from hosts import velox_hosts
from boto import ec2
import json
# from time import sleep, time, strftime, gmtime, localtime
import time
import os
import os.path
from datetime import datetime
import sys
import requests
from requests import exceptions
from velox_config import config

#####

# user needs to set VELOX_CLUSTER_KEY environment variable

#####


env.roledefs = { 'servers': velox_hosts.servers }
env.user = "ubuntu"
env.key_filename = os.getenv('VELOX_CLUSTER_KEY')

### VELOX SETTINGS ###
# HEAP_SIZE_GB = 45
VELOX_SERVER_JAR = "veloxms-core/target/veloxms-core-0.0.1-SNAPSHOT.jar"
VELOX_SERVER_CLASS = "edu.berkeley.veloxms.VeloxEntry"
VELOX_GARBAGE_COLLECTOR = "UseConcMarkSweepGC"
VELOX_HVM_AMI = 'ami-10119778'
VELOX_HEAP_SIZE_GB = 2

ETCD_PORT = 4001
ETCD_BASE_PATH = "/v2/keys/cluster_config"


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
        security_group (str): name of the security group
        size (int): The number of Velox servers in this cluster
    """

    def __init__(self,
            region,
            cluster_id,
            instance_type,
            ami,
            security_group,
            cluster_size):
        self.region = region
        self.cluster_id = cluster_id
        self.instance_type = instance_type
        self.ami = ami
        self.security_group = security_group
        self.size = cluster_size


# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/ and
# Apache Spark's ec2 launch script
# Last Updated: 2014-06-20
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
EC2_INSTANCE_TYPES = {
    "c1.medium":   "pvm",
    "c1.xlarge":   "pvm",
    "c3.2xlarge":  "pvm",
    "c3.4xlarge":  "pvm",
    "c3.8xlarge":  "pvm",
    "c3.large":    "pvm",
    "c3.xlarge":   "pvm",
    "cc1.4xlarge": "hvm",
    "cc2.8xlarge": "hvm",
    "cg1.4xlarge": "hvm",
    "cr1.8xlarge": "hvm",
    "hi1.4xlarge": "pvm",
    "hs1.8xlarge": "pvm",
    "i2.2xlarge":  "hvm",
    "i2.4xlarge":  "hvm",
    "i2.8xlarge":  "hvm",
    "i2.xlarge":   "hvm",
    "m1.large":    "pvm",
    "m1.medium":   "pvm",
    "m1.small":    "pvm",
    "m1.xlarge":   "pvm",
    "m2.2xlarge":  "pvm",
    "m2.4xlarge":  "pvm",
    "m2.xlarge":   "pvm",
    "m3.2xlarge":  "hvm",
    "m3.large":    "hvm",
    "m3.medium":   "hvm",
    "m3.xlarge":   "hvm",
    "r3.2xlarge":  "hvm",
    "r3.4xlarge":  "hvm",
    "r3.8xlarge":  "hvm",
    "r3.large":    "hvm",
    "r3.xlarge":   "hvm",
    "t1.micro":    "pvm",
    "t2.medium":   "hvm",
    "t2.micro":    "hvm",
    "t2.small":    "hvm",
}

############################################################
# CLUSTER LAUNCH
def wait_for_ssh_ready():
    start_time = datetime.now()
    time.sleep(3)  # seconds
    while True:
        try:
            with hide('everything', 'aborts'):
                result = execute(is_ssh_available, role='servers')
                if all(result.values()):
                    break
        except SystemExit, e:
            print "still waiting"
            time.sleep(10)  # seconds
    end_time = datetime.now()
    puts("Cluster is now in ssh-ready state. Took {tt} seconds".format(tt=(end_time-start_time).seconds))



def setup_security_group(conn, cluster):
    try :
        if len(filter(lambda x: x.name == cluster.security_group, conn.get_all_security_groups())) != 0:
            conn.delete_security_group(name=cluster.security_group)
        group = conn.create_security_group(cluster.security_group, "VeloxMS EC2 all-open SG")
        group.authorize('tcp', 0, 65535, '0.0.0.0/0')
    except Exception as e:
        print("Oops; couldn't create a new security group (%s). This is probably fine: %s"
                % (cluster.security_group, str(e)))



@task
# @parallel
def is_ssh_available():
    with settings(warn_only=True):
        with hide('stdout', 'stderr'):
            return not run("uname").failed


# Convenience command for launching an ec2 cluster of spot instances
# and installing basic dependencies
# Note that only hvm instance types are supported by the AMI, if you would like to use
# a pvm instance type please launch the cluster some other way, or supply your own AMI.
# This script launches spot instances.
@task
def launch_ec2_cluster(cluster_name,
                       cluster_size,
                       localkey,
                       keyname, # name of AWS key pair
                       spot_price=None,
                       instance_type='r3.2xlarge',
                       ami=VELOX_HVM_AMI):
    if env.key_filename is None:
        abort("Please provide a valid keypair file: export VELOX_CLUSTER_KEY=...")
    if EC2_INSTANCE_TYPES[instance_type] == 'pvm' and ami == VELOX_HVM_AMI:
        abort("Instance type is incompatible with ami")


    cluster = Cluster('us-east-1',
                      cluster_name,
                      instance_type,
                      ami,
                      'veloxms',
                      int(cluster_size))


    if not confirm("Spinning up %d instances in %s, okay?" % (cluster.size, cluster.region)):
        abort("Aborting at user request")
    puts("Setting up security group")
    conn = ec2.connect_to_region(cluster.region)
    setup_security_group(conn, cluster)
    ###############
    try:
        image = conn.get_all_images(image_ids=[cluster.ami])[0]
    except:
        abort("Could not find AMI " + cluster.ami)
    if spot_price is not None:
        puts("Requesting spot instances")
        spot_requests = conn.request_spot_instances(
                float(spot_price),
                cluster.ami,
                count=cluster.size,
                key_name=keyname,
                instance_type=cluster.instance_type,
                security_groups=[cluster.security_group])
        my_req_ids = [req.id for req in spot_requests]
        puts("Waiting for instances in %s to start..." % cluster.region)
        # time.sleep(10)
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == cluster.size:
                    print("All %d instances granted" % cluster.size)
                    reservations = conn.get_all_reservations(active_instance_ids)
                    nodes = []
                    for r in reservations:
                        nodes += r.instances
                    break
                else:
                    print("%d of %d instances granted, waiting longer" % (
                        len(active_instance_ids), cluster.size))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            abort("Warning, some requests may have already been granted")
    else:
        nodes_res = image.run(
                key_name=keyname,
                security_groups=[cluster.security_group],
                instance_type=cluster.instance_type,
                min_count=cluster.size,
                max_count=cluster.size)
        time.sleep(10)
        node_ids = [n.id for n in nodes_res.instances]
        print node_ids
        nodes = conn.get_only_instances(node_ids)
        print nodes
        while any(n.public_dns_name == '' for n in nodes):
            print "waiting for metadata to propagate"
            time.sleep(5)
            nodes = conn.get_only_instances(node_ids)
            print "NEW NODES:", nodes

    server_ips = [n.public_dns_name for n in nodes]
    print server_ips
    for n in nodes:
        n.add_tag(
            key='Name',
            value='{cn}-velox-{iid}'.format(cn=cluster.cluster_id, iid=n.id))

    # save ip_addresses of cluster
    with open('hosts/velox_hosts.py', 'w') as hosts_file:
        all_servers = ', '.join('"%s"' % s for s in server_ips)
        hosts_file.write('servers = [%s]\n' % all_servers)
    env.roledefs['servers'] = server_ips
    reload(velox_hosts)
    puts("Waiting for cluster to enter ssh-ready state")
    wait_for_ssh_ready()

    puts("installing etcd")
    execute(install_etcd, role='servers')

    puts("starting etcd")
    start_new_etcd_cluster()

    set_hostnames()

    ####### TODO remove once Velox is open source
    execute(upload_deploy_key, localkey, role='servers')
    puts("Building Velox")
    execute(build_velox,
            git_remote="git@github.com:amplab/velox-modelserver.git",
            branch="develop",
            role='servers')

@task
def set_hostnames():
    for h in env.roledefs['servers']:
        execute(set_hostname, h, host=h)


# END CLUSTER LAUNCH
############################################################


@task
def install_velox_local(etcd_loc):
    velox_root_dir = path.abspath("../..") # this script is in velox-modelserver/bin/cluster
    server_ips = ["127.0.0.1"]
    env.roledefs['servers'] = server_ips
    with open('hosts/velox_hosts.py', 'w') as hosts_file:
        all_servers = ', '.join('"%s"' % s for s in server_ips)
        hosts_file.write('servers = [%s]\n' % all_servers)
    pl = sys.platform.lower()
    if pl == "linux" or pl == "linux2":
        install_etcd_local(etcd_loc, "linux")
    elif pl == "darwin":
        install_etcd_local(etcd_loc, "darwin")
    else:
        abort("{pl} is unsupported".format(pl=sys.platform))
    with lcd(velox_root_dir):
        local("mvn package")
    local("export VELOX_HOSTNAME=127.0.0.1")




@task
def install_etcd_local(etcd_loc, platform):
    """
        etcd_loc(str) path to the directory where etcd will be installed. If
                      etcd dir already exists at this location, we assume that
                      it has already been installed and attempt to start it running.

        platform(str) either linux or darwin. Needed to determine which version of etcd to install.

    """
    etcd_version = "2.0.10"
    with hide('stdout', 'stderr'):
        with settings(warn_only=True):
            with lcd(etcd_loc):
                if local("test -d etcd").failed:
                    if platform == 'darwin':
                        local("curl -L  https://github.com/coreos/etcd/releases/download/v{ev}/etcd-v{ev}-{pl}-amd64.zip -o etcd-v{ev}-{pl}-amd64.zip".format(ev=etcd_version, pl=platform))
                        local("unzip etcd-v{ev}-{pl}-amd64.zip; mv etcd-v{ev}-{pl}-amd64 etcd".format(ev=etcd_version, pl=platform))
                    elif platform == 'linux':
                        local("curl -L  https://github.com/coreos/etcd/releases/download/v{ev}/etcd-v{ev}-{pl}-amd64.tar.gz -o etcd-v{ev}-{pl}-amd64.tar.gz".format(ev=etcd_version, pl=platform))
                        local("tar xzvf etcd-v{ev}-{pl}-amd64.tar.gz; mv etcd-v{ev}-{pl}-amd64 etcd".format(ev=etcd_version, pl=platform))
                    else:
                        abort("Unsupported platform")
                # local("etcd/etcd &> /dev/null &")
                local("etcd/etcd &")

@task
@parallel
def build_velox(git_remote, branch):
    with hide('stdout', 'stderr'):
        with settings(warn_only=True):
            if run("test -d ~/velox-modelserver").failed:
                # puts("Cloning Velox on %s" % env.host_string)
                run("git clone %s" % git_remote)
        with cd("~/velox-modelserver"):
            run("git stash")
            with settings(warn_only=True):
                run("git clean -f")
                run("git remote rm vremote")
            run("git remote add vremote %s" % git_remote)
            run("git checkout develop")
            with settings(warn_only=True):
                run("git branch -D veloxbranch")
            run("git fetch vremote")
            run("git checkout -b veloxbranch vremote/%s" % branch)
            run("git reset --hard vremote/%s" % branch)
            run("mvn package")


# can get public ip_addr on instance via:
# curl http://169.254.169.254/latest/meta-data/public-ipv4
@task
def set_hostname(hostname):
    run("echo export VELOX_HOSTNAME=%s >> ~/ec2_variables.sh" % hostname)
    run("source ~/ec2_variables.sh")

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

@task
def start_new_etcd_cluster():
    cluster_ips = env.roledefs['servers']
    token = "velox_etcd_%d" % int(time.time()) # unique ID for etcd cluster
    for part in range(len(cluster_ips)):
        execute(start_etcd, part, cluster_ips, token, host=cluster_ips[part])

@task
def start_etcd(partition, all_peers, cluster_token):
    """
        partition(int) the partition number of this velox server
        all_peers(list) a list of all server ip addresses to create the etcd cluster with
        cluster_token(str) a unique name to give the cluster
    """

    hostname = all_peers[partition]
    init_cluster = ""
    for i in range(len(all_peers)):
        init_cluster += "velox%d=http://%s:2380," %(i, all_peers[i])

    # set environment variables needed for daemon start script
    run("echo export ETCD_PARTITION=%s > ~/etcd_variables.sh" % partition)
    run("echo export HOSTNAME=%s >> ~/etcd_variables.sh" % hostname)
    run("echo export ETCD_CLUSTER_TOKEN=%s >> ~/etcd_variables.sh" % cluster_token)
    run("echo export ETCD_CLUSTER=%s >> ~/etcd_variables.sh" % init_cluster)
    sudo("start etcd")

@task
@parallel
def install_etcd():
    """
    Installs etcd to be able to run as an upstart daemon.

    This assumes upstart is being used to run the daemon (this is the case for ubuntu 14.04)
    """
    etcd_version = "2.0.10"
    with hide('stdout', 'stderr'):
        with settings(warn_only=True):
            if run("test -d ~/etcd").failed:
                run("curl -L  https://github.com/coreos/etcd/releases/download/v{ev}/etcd-v{ev}-linux-amd64.tar.gz -o etcd-v{ev}-linux-amd64.tar.gz".format(ev=etcd_version))
                run("tar xzvf etcd-v{ev}-linux-amd64.tar.gz; mv etcd-v{ev}-linux-amd64 etcd".format(ev=etcd_version))
        put("../etcd_utils/etcd.conf", "/etc/init/etcd.conf", use_sudo=True)
        put("../etcd_utils/start_etcd.sh", "~/start_etcd.sh", mirror_local_mode=True)



@task
def start_velox(start_local="n"):
    upload_config_to_etcd()
    if start_local.lower() == "y":
        velox_root_dir = path.abspath("../..")
        server_cmd = ("java -XX:+{gc} -Xms{hs}g -Xmx{hs}g "
                      "-Ddw.hostname=127.0.0.1 -cp {vr}/{jar} {cls} server & sleep 5; exit 0"
                      ).format(gc=VELOX_GARBAGE_COLLECTOR,
                               hs=VELOX_HEAP_SIZE_GB,
                               vr=velox_root_dir,
                               jar=VELOX_SERVER_JAR,
                               cls=VELOX_SERVER_CLASS)
        local(server_cmd)
    else:
        if env.key_filename is None:
            abort("Please provide a valid keypair file: export VELOX_CLUSTER_KEY=...")
        velox_root_dir = "~/velox-modelserver" # assumes cluster deployed using this script
        execute(start_velox_node, velox_root_dir, role='servers')

@task
def stop_velox():
    # with hide('everything'):
    if env.key_filename is None:
        abort("Please provide a valid keypair file: export VELOX_CLUSTER_KEY=...")
    execute(kill_velox_node, role='servers')

@parallel
def kill_velox_node():
    with settings(warn_only=True):
        with hide('everything'):
            run("killall java")
            time.sleep(2)
            run("killall java")

    

@task
@parallel
def start_velox_node(velox_root_dir):
    server_cmd = ("source ec2_variables.sh; nohup java -XX:+{gc} -Xms{hs}g -Xmx{hs}g "
                  "-Ddw.hostname=$VELOX_HOSTNAME -cp {vr}/{jar} {cls} server & sleep 5; exit 0"
                 ).format(gc=VELOX_GARBAGE_COLLECTOR,
                          hs=VELOX_HEAP_SIZE_GB,
                          vr=velox_root_dir,
                          jar=VELOX_SERVER_JAR,
                          cls=VELOX_SERVER_CLASS)
    run(server_cmd)



def add_kv_to_etcd(etcd_host, key_path, value):
    payload = {'value': str(value)}
    r = requests.put(etcd_host + key_path, data=payload)
    # pp.pprint(r.json())


def add_settings(etcd_host, key, cfg):
    if isinstance(cfg, dict):
        for k, v in cfg.items():
            add_settings(etcd_host, key + "/" + k, v)
    elif isinstance(cfg, list):
        for v in cfg:
            add_settings(etcd_host, key, v)
    elif key != '':
        add_kv_to_etcd(etcd_host, key, cfg)
    else:
        print "Error: trying to add config with no key"

@task
def upload_config_to_etcd():
    # partitions = {}
    # for idx, s in enumerate(velox_hosts.servers):
    #     partitions[s] = idx
    config['veloxPartitions'] = json.dumps(velox_hosts.servers)
    etcd_host = "http://{host}:{port}".format(host=velox_hosts.servers[0], port=ETCD_PORT)
    add_settings(etcd_host, ETCD_BASE_PATH, config)

