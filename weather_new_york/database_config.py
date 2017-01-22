import configparser


config = configparser.ConfigParser()
config.read('dbconfig.properties')
# print(config.get('DatabaseSection','database.username'))
def get_host():
    return config.get('DatabaseSection', 'database.host')

def get_username():
    return config.get('DatabaseSection', 'database.username')

def get_password():
    return config.get('DatabaseSection', 'database.password')

def get_database():
    return config.get('DatabaseSection', 'database.database')
