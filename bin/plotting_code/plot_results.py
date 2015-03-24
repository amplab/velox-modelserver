from fabric.api import *
from pymongo import MongoClient
import pymongo
from mongo_utils import *
import datetime
import json
from time import sleep, time, strftime, gmtime, localtime
import os.path
import pprint
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from hosts import velox_hosts

from velox_fabric import MONGO_HOST, MONGO_DB, MONGO_PORT, BenchmarkConfig


MONGO_HOST = "ec2-54-161-215-250.compute-1.amazonaws.com"
MONGO_PORT = 27017

MONGO_DB = "velox"

def convert_hostname(h):
    return unicode(h.replace(".", "_"))


@task
def plot_vary_percent_obs(coll_name):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    coll = db[coll_name]
    filter = {
            # 'config.percent_obs': {"$lt": 1.0},
            'config.model_dim': 50,
            # 'config.percent_obs': 0.0,
            'config.max_conc_reqs': 200,
            'config.num_reqs': 50000
            }
    results = coll.find(filter).sort([
        ("config.percent_obs", pymongo.ASCENDING),
        ("config.max_conc_reqs", pymongo.ASCENDING)
        ])
    pp = pprint.PrettyPrinter(indent=4)
    xs = []
    thru_ys = {}
    dur_ys = {}

    for c in velox_hosts.clients:
        thru_ys[convert_hostname(c)] = []
        dur_ys[convert_hostname(c)] = []

    pred_lat_p50 = {}
    pred_lat_p99 = {}
    obs_lat_mean = {}
    for s in velox_hosts.servers:
        pred_lat_p99[convert_hostname(s)] = []
        pred_lat_p50[convert_hostname(s)] = []
        obs_lat_mean[convert_hostname(s)] = []
    i = 0
    for r in results:
        print ""
        print "obs: %f, reqs: %d" % (r[u'config'][u'percent_obs'], r[u'config'][u'max_conc_reqs'])
        percent_obs = r[u'config'][u'percent_obs']
        xs.append(percent_obs)
        servers = r[u'servers']
        for s, val in servers.iteritems():
            pred = val[u'metrics_json'][u'timers'][u"matrixfact/predict/"]
            obs = val[u'metrics_json'][u'timers'][u"matrixfact/observe/"]
            if percent_obs < 1.0:
                pred_lat_p50[s].append(pred[u'p50'])
                pred_lat_p99[s].append(pred[u'p99'])
            if percent_obs > 0.0:
                obs_lat_mean[s].append(obs[u'mean'])
            # obs.pop(u'duration_units', None)
            # obs.pop(u'm15_rate', None)
            # obs.pop(u'm1_rate', None)
            # obs.pop(u'm5_rate', None)
            # obs.pop(u'mean_rate', None)
            # obs.pop(u'rate_units', None)
            # obs.pop(u'p75', None)
            # obs.pop(u'p98', None)
            # obs.pop(u'p999', None)
            # pp.pprint(pred)
            # print "\n"
        clients = r[u'clients']
        for host, val in clients.iteritems():
            total_thru = val[u'total_thru']
            duration = val[u'duration']
            thru_ys[host].append(total_thru)
            dur_ys[host].append(duration)
            print "%f, %f seconds" % (total_thru, duration)
        i += 1
    print i

    plot_run(xs, thru_ys, "thruput", "percent_obs", "total thruput (ops/s)", logscale=False)
    plot_run(xs, dur_ys, "duration", "percent_obs", "duration (s)", logscale=False)


    plot_run(xs[:-1], pred_lat_p50, "pred_lat_p50", "percent_obs", "p50 pred latency (s)", logscale=False)
    plot_run(xs[:-1], pred_lat_p99, "pred_lat_p99", "percent_obs", "p99 pred latency (s)", logscale=False)
    plot_run(xs[1:], obs_lat_mean, "obs_lat_mean", "percent_obs", "mean obs latency (s)", logscale=False)


@task
def plot_vary_model_dim(coll_name):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    coll = db[coll_name]
    filter = {
            # 'config.percent_obs': {"$lt": 1.0},
            'config.percent_obs': 0.0,
            # 'config.max_conc_reqs': 200,
            'config.num_reqs': 50000
            }
    results = coll.find(filter).sort([
        ("config.percent_obs", pymongo.ASCENDING),
        ("config.max_conc_reqs", pymongo.ASCENDING)
        ])
    pp = pprint.PrettyPrinter(indent=4)
    xs = []
    thru_ys = {}
    dur_ys = {}

    for c in velox_hosts.clients:
        thru_ys[convert_hostname(c)] = []
        dur_ys[convert_hostname(c)] = []

    pred_lat_p50 = {}
    pred_lat_p99 = {}
    for s in velox_hosts.servers:
        pred_lat_p99[convert_hostname(s)] = []
        pred_lat_p50[convert_hostname(s)] = []
    i = 0
    for r in results:
        dim = r[u'config'][u'model_dim']
        print "\ndim: %d" % dim
        xs.append(dim)
        servers = r[u'servers']
        for s, val in servers.iteritems():
            pred = val[u'metrics_json'][u'timers'][u"matrixfact/predict/"]
            pred_lat_p50[s].append(pred[u'p50']*1000)
            pred_lat_p99[s].append(pred[u'p99']*1000)
        clients = r[u'clients']
        for host, val in clients.iteritems():
            total_thru = val[u'total_thru']
            duration = val[u'duration']
            thru_ys[host].append(total_thru)
            dur_ys[host].append(duration)
            print "%f, %f seconds" % (total_thru, duration)
        i += 1
    print i

    plot_run(xs, thru_ys, "thruput", "model_dim", "total thruput (ops/s)", figdir="no_obs", logscale=False)
    plot_run(xs, dur_ys, "duration", "model_dim", "duration (s)", figdir="no_obs", logscale=False)


    plot_run(xs, pred_lat_p50, "pred_lat_p50", "percent_obs", "p50 pred latency (ms)", figdir="no_obs", logscale=False)
    plot_run(xs, pred_lat_p99, "pred_lat_p99", "percent_obs", "p99 pred latency (ms)", figdir="no_obs", logscale=False)

@task
def plot_vary_model_doc_length(coll_name):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    coll = db[coll_name]
    filter = {
            # 'config.percent_obs': {"$lt": 1.0},
            # 'config.percent_obs': 0.0,
            'config.percent_obs': 0.1,
            'config.max_conc_reqs': 300,
            # 'config.num_reqs': 50000
            }
    results = coll.find(filter).sort([
        ("config.percent_obs", pymongo.ASCENDING),
        ("config.max_conc_reqs", pymongo.ASCENDING)
        ])
    pp = pprint.PrettyPrinter(indent=4)
    xs = []
    thru_ys = {}
    dur_ys = {}

    for c in velox_hosts.clients:
        thru_ys[convert_hostname(c)] = []
        dur_ys[convert_hostname(c)] = []

    pred_lat_p50 = {}
    pred_lat_p99 = {}
    obs_lat_mean = {}
    for s in velox_hosts.servers:
        pred_lat_p99[convert_hostname(s)] = []
        pred_lat_p50[convert_hostname(s)] = []
        obs_lat_mean[convert_hostname(s)] = []
    i = 0
    for r in results:
        doc_length = r[u'config'][u'doc_length']
        print "\ndoc length: %d" % doc_length
        xs.append(doc_length)
        servers = r[u'servers']
        for s, val in servers.iteritems():
            pred = val[u'metrics_json'][u'timers'][u"newsgroups/predict/"]
            obs = val[u'metrics_json'][u'timers'][u"newsgroups/observe/"]
            pred_lat_p50[s].append(pred[u'p50']*1000)
            pred_lat_p99[s].append(pred[u'p99']*1000)
            obs_lat_mean[s].append(obs[u'mean'])
        clients = r[u'clients']
        for host, val in clients.iteritems():
            total_thru = val[u'total_thru']
            duration = val[u'duration']
            thru_ys[host].append(total_thru)
            dur_ys[host].append(duration)
            print "%f, %f seconds" % (total_thru, duration)
        i += 1
    print i


    out_dir = "news_groups_plots_01_obs"
    plot_run(xs, thru_ys, "thruput", "doc_length", "total thruput (ops/s)", figdir=out_dir, logscale=False)
    plot_run(xs, dur_ys, "duration", "doc_length", "duration (s)", figdir=out_dir, logscale=False)


    plot_run(xs, pred_lat_p50, "pred_lat_p50", "doc_length", "p50 pred latency (ms)", figdir=out_dir, logscale=False)
    plot_run(xs, pred_lat_p99, "pred_lat_p99", "doc_length", "p99 pred latency (ms)", figdir=out_dir, logscale=False)
    plot_run(xs, obs_lat_mean, "obs_lat_mean", "doc_length", "mean obs latency (s)", figdir=out_dir, logscale=False)

@task
def plot_cache_partial_sums(coll_name="crankshaw_cache_partial_sums"):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    coll = db[coll_name]
    filter = {
            'config.doc_length': {"$lte": 100},
            # 'config.percent_obs': 0.0,
            'config.percent_obs': 0.1,
            # 'config.max_conc_reqs': 300,
            # 'config.cache_partial_sum': False
            # 'config.num_reqs': 50000
            }
    results = coll.find(filter)
        # .sort([
        # ("config.percent_obs", pymongo.ASCENDING),
        # ("config.max_conc_reqs", pymongo.ASCENDING)
        # ])
    pp = pprint.PrettyPrinter(indent=4)
    xs = []
    thru_ys = {'cache': {}, 'no_cache': {}}
    dur_ys = {'cache': {}, 'no_cache': {}}

    for c in velox_hosts.clients:
        thru_ys['cache'][convert_hostname(c)] = []
        thru_ys['no_cache'][convert_hostname(c)] = []
        dur_ys['cache'][convert_hostname(c)] = []
        dur_ys['no_cache'][convert_hostname(c)] = []

    pred_lat_p50 = {'cache': {}, 'no_cache': {}}
    pred_lat_p99 = {'cache': {}, 'no_cache': {}}
    obs_lat_mean = {'cache': {}, 'no_cache': {}}
    for s in velox_hosts.servers:
        pred_lat_p99['cache'][convert_hostname(s)] = []
        pred_lat_p99['no_cache'][convert_hostname(s)] = []
        pred_lat_p50['cache'][convert_hostname(s)] = []
        pred_lat_p50['no_cache'][convert_hostname(s)] = []
        obs_lat_mean['cache'][convert_hostname(s)] = []
        obs_lat_mean['no_cache'][convert_hostname(s)] = []
    i = 0
    for r in results:
        doc_length = r[u'config'][u'doc_length']
        is_cached = r[u'config'][u'cache_partial_sum']
        print "\ndoc length: %d" % doc_length

        if is_cached:
            if not u'pre_cache' in r[u'config']:
                continue
            xs.append(doc_length)
        servers = r[u'servers']
        for s, val in servers.iteritems():
            pred = val[u'metrics_json'][u'timers'][u"newsgroups/predict/"]
            obs = val[u'metrics_json'][u'timers'][u"newsgroups/observe/"]
            if is_cached:
                pred_lat_p50['cache'][s].append(pred[u'p50']*1000)
                pred_lat_p99['cache'][s].append(pred[u'p99']*1000)
                obs_lat_mean['cache'][s].append(obs[u'mean'])
            else:
                pred_lat_p50['no_cache'][s].append(pred[u'p50']*1000)
                pred_lat_p99['no_cache'][s].append(pred[u'p99']*1000)
                obs_lat_mean['no_cache'][s].append(obs[u'mean'])

        clients = r[u'clients']
        for host, val in clients.iteritems():
            total_thru = val[u'total_thru']
            duration = val[u'duration']
            if is_cached:
                thru_ys['cache'][host].append(total_thru)
                dur_ys['cache'][host].append(duration)
            else:
                thru_ys['no_cache'][host].append(total_thru)
                dur_ys['no_cache'][host].append(duration)

            print "%f, %f seconds" % (total_thru, duration)
        i += 1
    print i


    out_dir = "figs_test_caching"
    plot_runs(xs, thru_ys, "thruput", "doc_length", "total thruput (ops/s)", figdir=out_dir, logscale=False)
    plot_runs(xs, dur_ys, "duration", "doc_length", "duration (s)", figdir=out_dir, logscale=False)


    plot_runs(xs, pred_lat_p50, "pred_lat_p50", "doc_length", "p50 pred latency (ms)", figdir=out_dir, logscale=False)
    plot_runs(xs, pred_lat_p99, "pred_lat_p99", "doc_length", "p99 pred latency (ms)", figdir=out_dir, logscale=False)
    plot_runs(xs, obs_lat_mean, "obs_lat_mean", "doc_length", "mean obs latency (s)", figdir=out_dir, logscale=False)

def plot_runs(xs, ys, title, xlabel, ylabel, figdir="figures", config=None, logscale=False):
    """Plot result of single benchmark.

    Arguments:
        config (dict): dict that specifies everything held constnat
        xs (list): list of x values varied
        ys (dict): dictionary containing the thruput or latency lists
            for each machine
        title (str): title of plot
    """
    colors=['blue', 'green', 'red', 'orange']
    format_strs = ['o-', 's-', '^--', 'v--']
    fig, ax = plt.subplots()
    i = 0
    print xs
    print ""
    print ys
    print "\n\n\n\n\n"
    for cc in ys.keys():
        j = 0
        for machine, vals in ys[cc].iteritems():
            print vals
            kwargs = dict(label=cc, color=colors[j])
            ax.plot(xs, vals, format_strs[i], **kwargs)
            j += 1
        i += 1
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    if logscale:
        ax.set_yscale('log')
    ax.set_xlabel(xlabel)
    legend_loc = 0
    ax.legend(loc=legend_loc, prop={'size': 12})
    (ymin, ymax) = ax.get_ylim()
    ax.set_ylim(0, ymax)
    local("mkdir -p %s" % figdir)
    plt.savefig("%s/%s.pdf" % (figdir, title))



def plot_run(xs, ys, title, xlabel, ylabel, figdir="figures", config=None, logscale=False):
    """Plot result of single benchmark.

    Arguments:
        config (dict): dict that specifies everything held constnat
        xs (list): list of x values varied
        ys (dict): dictionary containing the thruput or latency lists
            for each machine
        title (str): title of plot
    """
    colors=['blue', 'green', 'red', 'orange']
    format_strs = ['o-', 's-', '^--', 'v--']
    fig, ax = plt.subplots()
    i = 0
    print ys
    print ""
    print xs
    print ""
    for machine, vals in ys.iteritems():
        print vals
        kwargs = dict(label=machine, color=colors[i])
        ax.plot(xs, vals, format_strs[i], **kwargs)
        i += 1
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    if logscale:
        ax.set_yscale('log')
    ax.set_xlabel(xlabel)
    legend_loc = 0
    ax.legend(loc=legend_loc, prop={'size': 12})
    (ymin, ymax) = ax.get_ylim()
    ax.set_ylim(0, ymax)
    local("mkdir -p %s" % figdir)
    plt.savefig("%s/%s.pdf" % (figdir, title))








