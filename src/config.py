import ConfigParser


class Config:
    DEFAULT_CONFIG = "autoscaler.conf"

    def load(self):
        self.debug = self.config.getboolean('general', 'debug')
        self.marathon_url = self.config.get('general', 'marathon_url')
        self.marathon_user = self.config.get('general', 'marathon_user')
        self.marathon_pass = self.config.get('general', 'marathon_pass')
        self.mesos_url = self.config.get('general', 'mesos_url')
        self.mesos_user = self.config.get('general', 'mesos_user')
        self.mesos_pass = self.config.get('general', 'mesos_pass')

    def __init__(self, filename=DEFAULT_CONFIG):
        self.configFile = filename
        self.config = ConfigParser.SafeConfigParser({'mesos_user': '',
                                                     'mesos_pass': '',
                                                     'marathon_user': '',
                                                     'marathon_pass': ''})
        self.config.read(filename)
