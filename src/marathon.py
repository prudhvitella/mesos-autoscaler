import sys
import json
import time
import requests


class Marathon(object):
    def __requests_get(self, uri):
        response = None
        if self.marathon_user:
            # print "User " + self.marathon_user + "Pass " + self.marathon_pass
            # print "URI " + self.marathon_host + uri
            response = requests.get(self.marathon_host + uri,
                                    verify=False,
                                    auth=(self.marathon_user,
                                          self.marathon_pass))
        else:
            response = requests.get(self.marathon_host + uri,
                                    verify=False)

        # print "Response: " + str(response)
        return response.json()

    def __requests_put(self, uri, data, headers):
        if self.marathon_user:
            # print "User " + self.marathon_user + "Pass " + self.marathon_pass
            # print "URI " + self.marathon_host + uri
            response = requests.put(self.marathon_host + uri, data,
                                    headers=headers,
                                    verify=False,
                                    auth=(self.marathon_user,
                                          self.marathon_pass))
        else:
            response = requests.put(self.marathon_host + uri,
                                    data,
                                    headers=headers,
                                    verify=False)
            # print "Response:" + str(response)
        return response

    def get_all_apps(self):
        response = self.__requests_get('/v2/apps')

        if response['apps'] == []:
            print "No Apps found on Marathon"
        else:
            apps = []
            for i in response['apps']:
                appid = i['id'].strip('/')
                apps.append(appid)
            # print "Found the following App LIST on Marathon = " + str(apps)
            self.apps = apps
            return apps

    def get_app_details(self, marathon_app):
        response = self.__requests_get('/v2/apps/' + marathon_app)

        if (response['app']['tasks'] == []):
            print "No task data on Marathon for App !" + marathon_app
        else:
            app_instances = response['app']['instances']
            self.appinstances = app_instances
            # print "App '" + marathon_app + "' has " +\
            #    str(self.appinstances) + " instances deployed"
            app_task_dict = {}
            app_task_dict['tasks'] = {}
            app_task_dict['mem'] = response['app']['mem']
            app_task_dict['cpus'] = response['app']['cpus']

            for task in response['app']['tasks']:
                task_id = task['id']
                hostid = task['host']

                # print "task = " + str(task_id) + " running on " + str(hostid)

                app_task_dict['tasks'][str(task_id)] = {}
                app_task_dict['tasks'][str(task_id)]['host'] = str(hostid)

            return app_task_dict

    def wait_until_deployed(self, response):
        deployed = False
        while not deployed:
            deployments = self.__requests_get('/v2/deployments')
            if deployments == []:
                deployed = True
                return
            for deployment in deployments:
                if deployment['id'] == response['deploymentId']:
                    time.sleep(5)
                    continue
            deployed = True

    def scale_app_instances(self, marathon_app, target_instance_count):

        data = {'instances': target_instance_count}
        json_data = json.dumps(data)
        headers = {'Content-type': 'application/json'}
        response = self.__requests_put('/v2/apps/' + marathon_app, json_data,
                                       headers=headers)
        if response.status_code != 200:
            sys.stderr.write("Unable to scale, got response: " +
                             str(response.status_code) + "\n")
            return False

        print "Scaling " + marathon_app + " with instances = " +\
            str(target_instance_count)

        # Wait until app changes are deployed
        self.wait_until_deployed(response.json())
        print "Scaled " + marathon_app

        return True

    def scale_app_mem(self, marathon_app, target_mem_size):

        data = {'mem': target_mem_size}
        json_data = json.dumps(data)
        headers = {'Content-type': 'application/json'}
        response = self.__requests_put('/v2/apps/' + marathon_app, json_data,
                                       headers=headers)
        if response.status_code != 200:
            sys.stderr.write("Unable to scale, got response: " +
                             str(response.status_code) + "\n")
            return False

        print "Scaling " + marathon_app + " with mem = " + str(target_mem_size)

        # Wait until app changes are deployed
        self.wait_until_deployed(response.json())
        print "Scaled " + marathon_app

        return True

    def __init__(self, marathon_host, marathon_user=None, marathon_pass=None):
        self.name = marathon_host
        self.marathon_user = marathon_user
        self.marathon_pass = marathon_pass
        self.marathon_host = (marathon_host)
