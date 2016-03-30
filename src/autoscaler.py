#!/usr/bin/env python

import os
import sys
import time
import signal
import requests
import threading
import traceback
from multiprocessing import Process

from config import Config
from mesos import Mesos
from marathon import Marathon
from httpserver import HttpServer

MARATHON_POLL_INTERVAL = 5
MARATHON_SAMPLE_SIZE = 4

MARATHON_MIN_TASK_COUNT = 2         # minimum number of tasks to keep
MARATHON_MIN_CPU_THRESHOLD = 0.10   # percent of CPU to tell if task is idle
MARATHON_MIN_MEM_THRESHOLD = 0.10   # percent of memory to tell if task is idle
MARATHON_MAX_CPU_THRESHOLD = 0.90   # percent of CPU a task should use
MARATHON_MAX_MEM_THRESHOLD = 0.75   # percent of memory a task should use
MARATHON_APP_MEM_SCALE = 0.5        # scale app memory by this amount

MAX_CPUS_ALLOC = 0.5  # Max percentage of free CPU to give out to a single task
MAX_MEM_ALLOC = 0.5  # Max percentage of free Mem to give out to a single task


def get_task_statistics(task_id, host):
    # Get the performance Metrics for all the tasks for the Marathon App
    # specified by connecting to the Mesos Agent and then making a REST call
    # against Mesos statistics
    # Return to Statistics for the specific task for the marathon_app

    response = requests.get('http://' + host + ':5051/monitor/statistics.json')\
                     .json()

    for task in response:
        executor_id = task['executor_id']
        if (executor_id == task_id):
            task_stats = task['statistics']
            return task_stats


def get_timestamp(task_stats):
    # Get timestamp from Mesos>0.25 or use local timstamp
    timestamp = 0
    if 'timestamp' in task_stats:
        timestamp = task_stats['timestamp']
    else:
        timestamp = time.time()
    return timestamp


def task_in_prior_sample(samples, app, task_id, trailing_sample_index):
    if samples[trailing_sample_index] and \
       app in samples[trailing_sample_index] and \
       task_id in samples[trailing_sample_index][app]['tasks'] and \
       samples[trailing_sample_index][app]['tasks'][task_id]:

        return True

    else:
        return False


def get_cpu_util(samples, app, task_id, trailing_sample_index, cpus_time,
                 timestamp):
    cpu_util = 0

    # Make sure have a sample of app and task data and
    # then calculate CPU utilization as follows:
    # (prior cpus_time - current cpus_time) / (elapased time)
    if task_in_prior_sample(samples, app, task_id, trailing_sample_index):
        cpu_util = (cpus_time - samples[trailing_sample_index][app]['tasks']
                    [task_id]['cpus_time']) / (timestamp -
                                               samples[trailing_sample_index]
                                               [app]['tasks'][task_id]
                                               ['timestamp'])
    return cpu_util


"""
Returns number of samples collected for a task. Max number of samples collected
is MARATHON_SAMPLE_SIZE
"""


def get_sample_count(samples, app, task_id, trailing_sample_index):
    sample_count = 0

    if task_in_prior_sample(samples, app, task_id, trailing_sample_index) and \
       'sample_count' in samples[trailing_sample_index][app]['tasks'][task_id]:
        sample_count = min(samples[trailing_sample_index][app]['tasks'][task_id]
                           ['sample_count'], MARATHON_SAMPLE_SIZE)

    return sample_count


def get_avg_resource_util(samples, app, task_id, trailing_sample_index,
                          resource_util, resource_name):
    avg_resource_util = resource_util
    prior_avg_resource_util = 0.0
    sample_count = 1
    if task_in_prior_sample(samples, app, task_id, trailing_sample_index) and \
       resource_name in samples[trailing_sample_index][app]['tasks'][task_id]:
        sample_count = float(get_sample_count(samples, app, task_id,
                                              trailing_sample_index) + 1)
        prior_avg_resource_util = \
            samples[trailing_sample_index][app]['tasks'][task_id][resource_name]

        avg_resource_util = ((1/sample_count) * resource_util) +\
            (((sample_count - 1)/sample_count) * prior_avg_resource_util)

    return avg_resource_util


def app_reached_min_mem_threshold(app_avg_mem_util):
    if app_avg_mem_util <= MARATHON_MIN_MEM_THRESHOLD:
        return True
    else:
        return False


def app_reached_min_cpu_threshold(app_avg_cpu_util):
    # If task CPU utilization is within +20% of MARATHON_MAX_CPU_THRESHOLD
    # then scale the task. e.g. 90%-110%, 190%-210%, 290%-310%, etc.
    if app_avg_cpu_util <= MARATHON_MIN_CPU_THRESHOLD:
        return True
    else:
        return False


def app_reached_max_mem_threshold(app_avg_mem_util):
    if app_avg_mem_util >= MARATHON_MAX_MEM_THRESHOLD:
        return True
    else:
        return False


def app_reached_max_cpu_threshold(app_avg_cpu_util):
    # If task CPU utilization is within +20% of MARATHON_MAX_CPU_THRESHOLD
    # then scale the task. e.g. 90%-110%, 190%-210%, 290%-310%, etc.
    if app_avg_cpu_util > 0.5 and (
       app_avg_cpu_util % 1.0 >= MARATHON_MAX_CPU_THRESHOLD or
       app_avg_cpu_util % 1.0 <= 1 - MARATHON_MAX_CPU_THRESHOLD):
        return True
    else:
        return False


def allocate_app_mem(app_details):
    if float(app_details['mem'] * len(app_details['tasks'])) /\
            mesos.get_mem_free() < MAX_MEM_ALLOC:
        return app_details['mem'] * 2
    else:
        return app_details['mem']


def mesos_cpus_available(app_details):
    if float(app_details['cpus']) / mesos.get_cpus_free() < MAX_CPUS_ALLOC:
        return True
    else:
        return False


def reset_sample_count(app_details):
    for task_id, task_details in app_details['tasks'].items():
        task_details['sample_count'] = 0


def scaleup_marathon_app(app, app_details):

    if app_reached_max_mem_threshold(app_details['app_avg_mem_util']):
        offered_mem = allocate_app_mem(app_details)
        if offered_mem > app_details['mem']:
            if marathon.scale_app_mem(app, offered_mem):
                reset_sample_count(app_details)
        return

    if app_reached_max_cpu_threshold(app_details['app_avg_cpu_util']):
        if mesos_cpus_available(app_details):
            if marathon.scale_app_instances(app, app_details['task_count'] + 1):
                reset_sample_count(app_details)


def scaledown_marathon_app(app, app_details):

    if app_reached_min_mem_threshold(app_details['app_avg_mem_util']) and\
            app_details['task_count'] > MARATHON_MIN_TASK_COUNT:
        if marathon.scale_app_mem(app, int(app_details['mem'] *
                                           MARATHON_APP_MEM_SCALE)):
            reset_sample_count(app_details)
        return

    if app_reached_min_cpu_threshold(app_details['app_avg_cpu_util']) and\
            app_details['task_count'] > MARATHON_MIN_TASK_COUNT:
        if marathon.scale_app_instances(app, app_details['task_count'] - 1):
            reset_sample_count(app_details)


def compute_app_averages(marathon_apps):
    for app, app_details in marathon_apps.items():
        if not app_details:
            continue
        app_sum_cpu_util = 0.0
        app_sum_mem_util = 0.0
        app_avg_cpu_util = 0.0
        app_avg_mem_util = 0.0
        sample_count = 0
        for task_id, task_details in app_details['tasks'].items():
            if not task_details:
                continue
            app_sum_cpu_util = app_sum_cpu_util +\
                (task_details['sample_count'] * task_details['avg_cpu_util'])
            app_sum_mem_util = app_sum_mem_util +\
                (task_details['sample_count'] * task_details['avg_mem_util'])
            sample_count = sample_count + task_details['sample_count']

        if sample_count is not 0:
            app_avg_cpu_util = (app_sum_cpu_util / sample_count)
            app_avg_mem_util = (app_sum_mem_util / sample_count)

        app_details['app_avg_cpu_util'] = app_avg_cpu_util
        app_details['app_avg_mem_util'] = app_avg_mem_util


def scale_marathon_apps(marathon_apps):

    for app, app_details in marathon_apps.items():
        if not app_details or\
                app_details['max_samples_in_app'] < MARATHON_SAMPLE_SIZE:
            continue
        # scale up cpu or mem resources if the app is hitting allocation limit
        scaleup_marathon_app(app, app_details)
        # scale down cpu or mem resources if they are underutilized
        scaledown_marathon_app(app, app_details)


def print_stats(apps_details):

    print "\nApp count: {0}".format(len(apps_details))
    for app, app_details in apps_details.items():
        print "Name: {0:<24.24} Instances: {1:<5} CPU: {2:<10.2%}\
Avg CPU: {3:<10.2%} Mem: {4:<10.2%} Avg Mem: {5:<10.2%}"\
            .format(app, app_details['task_count'],
                    app_details['cpu_util'],
                    app_details['app_avg_cpu_util'],
                    app_details['mem_util'],
                    app_details['app_avg_mem_util'])


def signal_handler(signm, frame):
    print "Got signal " + str(signm) + ", exiting now"
    httpserver_process.terminate()
    sys.exit(0)


def marathon_poll():
    samples = [None] * MARATHON_SAMPLE_SIZE
    sample_index = 0
    trailing_sample_index = 0

    while True:
        try:
            time.sleep(MARATHON_POLL_INTERVAL)
            marathon_apps = marathon.get_all_apps()
            if not marathon_apps:
                continue

            # print "Currently deployed apps: " + str(marathon_apps)

            apps_details = {}
            for app in marathon_apps:
                apps_details[app] = marathon.get_app_details(app)

            if not apps_details:
                continue

            for app, app_details in apps_details.items():
                if not app_details:
                    apps_details[app]['tasks'] = None
                    continue
                apps_details[app]['cpu_util'] = 0.0
                apps_details[app]['mem_util'] = 0.0
                apps_details[app]['max_samples_in_app'] = 0
                for task_id, task_details in app_details['tasks'].items():
                    task_stats = get_task_statistics(task_id,
                                                     task_details['host'])
                    if not task_stats:
                        apps_details[app]['tasks'][task_id] = None
                        continue

                    timestamp = get_timestamp(task_stats)

                    cpus_time = (task_stats['cpus_system_time_secs'] +
                                 task_stats['cpus_user_time_secs'])

                    cpu_util = get_cpu_util(samples, app, task_id,
                                            trailing_sample_index, cpus_time,
                                            timestamp)

                    mem_rss_bytes = int(task_stats['mem_rss_bytes'])
                    mem_limit_bytes = int(task_stats['mem_limit_bytes'])
                    mem_util = (float(mem_rss_bytes) /
                                float(mem_limit_bytes))

                    apps_details[app]['tasks'][task_id]['timestamp'] = timestamp
                    apps_details[app]['tasks'][task_id]['cpus_time'] = cpus_time
                    apps_details[app]['tasks'][task_id]['cpu_util'] = cpu_util
                    apps_details[app]['tasks'][task_id]['mem_rss_bytes'] = \
                        mem_rss_bytes
                    apps_details[app]['tasks'][task_id]['mem_limit_bytes'] = \
                        mem_limit_bytes
                    apps_details[app]['tasks'][task_id]['mem_util'] = mem_util
                    apps_details[app]['tasks'][task_id]['sample_count'] = \
                        get_sample_count(samples, app, task_id,
                                         trailing_sample_index) + 1
                    apps_details[app]['tasks'][task_id]['avg_cpu_util'] = \
                        get_avg_resource_util(samples, app, task_id,
                                              trailing_sample_index, cpu_util,
                                              'avg_cpu_util')
                    apps_details[app]['tasks'][task_id]['avg_mem_util'] = \
                        get_avg_resource_util(samples, app, task_id,
                                              trailing_sample_index, mem_util,
                                              'avg_mem_util')
                    apps_details[app]['cpu_util'] = \
                        apps_details[app]['cpu_util'] + cpu_util
                    apps_details[app]['mem_util'] = \
                        apps_details[app]['mem_util'] + mem_util
                    apps_details[app]['max_samples_in_app'] = \
                        max(apps_details[app]['tasks'][task_id]['sample_count'],
                            apps_details[app]['max_samples_in_app'])

                apps_details[app]['cpu_util'] = \
                    apps_details[app]['cpu_util'] / len(app_details['tasks'])
                apps_details[app]['mem_util'] = \
                    apps_details[app]['mem_util'] / len(app_details['tasks'])
                apps_details[app]['task_count'] = len(app_details['tasks'])

            compute_app_averages(apps_details)
            print_stats(apps_details)

            trailing_sample_index = sample_index

            # store collected sample
            samples[sample_index] = apps_details

            scale_marathon_apps(samples[sample_index])

            # increment rotating index
            sample_index = (sample_index + 1) % MARATHON_SAMPLE_SIZE

        except Exception:
            traceback.print_exc()
            time.sleep(MARATHON_POLL_INTERVAL)


def update_config_with_env(config):
    if os.getenv('MARATHON_URL'):
        config.marathon_url = os.getenv('MARATHON_URL')
    if os.getenv('MARATHON_USER'):
        config.marathon_user = os.getenv('MARATHON_USER')
    if os.getenv('MARATHON_PASS'):
        config.marathon_pass = os.getenv('MARATHON_PASS')
    if os.getenv('MESOS_URL'):
        config.mesos_url = os.getenv('MESOS_URL')
    if os.getenv('MESOS_USER'):
        config.mesos_user = os.getenv('MESOS_USER')
    if os.getenv('MESOS_PASS'):
        config.mesos_pass = os.getenv('MESOS_PASS')


if __name__ == "__main__":
    config = Config()
    config.load()
    update_config_with_env(config)

    httpserver = HttpServer(debug=config.debug, listen_port=os.getenv('PORT0'))
    httpserver_process = Process(target=httpserver.start)
    httpserver_process.start()

    marathon = Marathon(config.marathon_url, marathon_user=config.marathon_user,
                        marathon_pass=config.marathon_pass)

    mesos = Mesos(config.mesos_url, mesos_user=config.mesos_user,
                  mesos_pass=config.mesos_pass)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGABRT, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    marathon_poll_thread = threading.Thread(target=marathon_poll)
    marathon_poll_thread.daemon = True
    marathon_poll_thread.start()
    while threading.active_count():
        time.sleep(5)

    sys.exit(0)
