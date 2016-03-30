import requests


class Mesos(object):
    def __requests_get(self, uri):
        response = None
        if self.mesos_user:
            # print "User " + self.mesos_user + "Pass " + self.mesos_pass
            # print "URI " + self.mesos_host + uri
            response = requests.get(self.mesos_host + uri,
                                    verify=False,
                                    auth=(self.mesos_user,
                                          self.mesos_pass))
        else:
            response = requests.get(self.mesos_host + uri,
                                    verify=False)

        # print "Response: " + str(response)
        return response.json()

    def __requests_put(self, uri, data, headers):
        if self.mesos_user:
            # print "User " + self.mesos_user + "Pass " + self.mesos_pass
            # print "URI " + self.mesos_host + uri
            response = requests.put(self.mesos_host + uri, data,
                                    headers=headers,
                                    verify=False,
                                    auth=(self.mesos_user,
                                          self.mesos_pass))
        else:
            response = requests.put(self.mesos_host + uri,
                                    data,
                                    headers=headers,
                                    verify=False)
            # print "Response:" + str(response)
        return response.json()

    def get_mem_free(self):
        metrics = self.__requests_get('/metrics/snapshot')
        return metrics['master/mem_total'] - metrics['master/mem_used']

    def get_cpus_free(self):
        metrics = self.__requests_get('/metrics/snapshot')
        return metrics['master/cpus_total'] - metrics['master/cpus_used']

    def __init__(self, mesos_host, mesos_user=None, mesos_pass=None):
        self.name = mesos_host
        self.mesos_user = mesos_user
        self.mesos_pass = mesos_pass
        self.mesos_host = (mesos_host)
