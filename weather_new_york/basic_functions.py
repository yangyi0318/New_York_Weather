import configparser
import os

class Database_Config(object):
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),'data/dbconfig.properties'))
    # print(config.get('DatabaseSection','database.username'))
    @staticmethod
    def get_host():
        return Database_Config.config.get('DatabaseSection', 'database.host')

    @staticmethod
    def get_username():
        return Database_Config.config.get('DatabaseSection', 'database.username')

    @staticmethod
    def get_password():
        return Database_Config.config.get('DatabaseSection', 'database.password')

    @staticmethod
    def get_database():
        return Database_Config.config.get('DatabaseSection', 'database.database')

def read_post_codes():
    post_code_file = os.path.join(os.path.dirname(__file__),'data/post_codes.txt')
    post_codes = []
    with open(post_code_file,'r') as f:
        for line in f.readlines():
            if line.startswith('#'):
                continue
            post_codes.append(line.strip())
    return post_codes

def read_accu_keys():
    '''
    :return:list of locations keys, element is tuple (post_code,accu_location_key,lat,lon) 
    '''
    keys_file = os.path.join(os.path.dirname(__file__), 'data/accu_keys.txt')
    keys = []
    with open(keys_file,'r') as f:
        for line in f.readlines():
            split_line=line.strip().split(',')
            keys.append(tuple(split_line))
    return keys

def temperature_f2c(temperature_f):
    return float('%.1f' % ((float(temperature_f)-32)*0.5556))