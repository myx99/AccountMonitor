import configparser


# Public Configurations
config_path = "..\\Configs\\"
# config_path = "/home/PerformanceAnalysis/Performance/AccountMonitor/Configs/"
configfile = "config.ini"
path = config_path + configfile
# print(path)


class GlobalConfig(object):

    def __init__(self):
        # print("Loaded config file")
        self.cp = configparser.ConfigParser()
        self.cp.read(path)

    def getConfig(self, section, key):
        # print("Retrieved string type configurations")
        return self.cp.get(section, key)

    def getIntConfig(self, section, key):
        # print("Retrieved int type configurations")
        return self.cp.getint(section, key)

    def getBooleanConfig(self, section, key):
        # print("Retrieved boolean type configurations")
        return self.cp.getboolean(section, key)


if __name__ == '__main__':
    m = GlobalConfig()
    host = m.getConfig('product', 'list')
    port = m.getIntConfig("mysql", "port")
    user = m.getConfig("mysql", "user")
    password = m.getConfig("mysql", "passwd")
    dbname = m.getConfig("mysql", "db")
    unicode = m.getBooleanConfig("mysql", "use_unicode")
    charset = m.getConfig("mysql", "charset")
    print(host, port, user, password, dbname, unicode, charset)





