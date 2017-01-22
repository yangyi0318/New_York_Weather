import datetime
from pytz import timezone
import urllib.request
import urllib.error
import json
import pymysql
import os
import database_config
import read_new_york_places

eastern = timezone(('US/Eastern'))
places = read_new_york_places.read_places()
insert_sql = '''
insert into dark_sky (code,forecast_time,temperature,humidity,dewpoint,outlook,visibility,insert_time) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')
'''

def make_request_url(lat,lon):
    base = 'https://api.darksky.net/forecast/daf636210aff52e2766b9f01ed93831b/'
    base+=str(lat)
    base+=','
    base+=str(lon)
    return base

def ts_to_eastern(ts):
    return datetime.datetime.fromtimestamp(ts,eastern)

def f_to_c(f):
    f = float(f)
    c = (f-32) * 0.5556
    c = '%.1f' % c
    return str(c)

def process_request(request_url,db,code,insert_time,error_file):
    try:
        res = urllib.request.urlopen(request_url)
        res_string = str(res.read(),'utf-8')
        json_obj = json.loads(res_string)
        hourly_data = json_obj['hourly']['data']
        for data in hourly_data:
            ts = int(data['time'])
            forecast_time = ts_to_eastern(ts)
            outlook = data['summary']
            temp = data['temperature']
            temp = f_to_c(temp)
            dewPoint = data['dewPoint']
            dewPoint = f_to_c(dewPoint)
            humidity = data['humidity']
            humidity = int(float(humidity) * 100)
            humidity = str(humidity)
            visibibilty = data['visibility']
            sql_temp = insert_sql % (code,forecast_time,temp,humidity,dewPoint,outlook,visibibilty,insert_time)
            try:
                cursor = db.cursor()
                cursor.execute(sql_temp)
                db.commit()
            except:
                db.rollback()
                return 0
        return 1
    except urllib.error.HTTPError as e:
        print('HTTPERROR')
        return 0

right_url = 'https://api.darksky.net/forecast/daf636210aff52e2766b9f01ed93831b/42.74722,-73.79912'
wrong_url = 'https://api.darksky.net/forecast/daf636210aff52e2766b9f01ed93831b/42.74722,'
db = pymysql.connect(database_config.get_host(), database_config.get_username(), database_config.get_password(), database_config.get_database())
insert_time = datetime.datetime.now()
error_file = error_file = open(os.path.join(os.path.dirname(__file__), 'dark_sky.log'),'a')
error_file.write('begin to crawl aeris at {}\n'.format(insert_time.__str__()))
counter = 0
error = 0
for place in places.values():
    code = place.get_code()
    counter+=1
    print('processing dark sky {}, {}/{}'.format(code, counter, len(places.values())))
    lat = place.get_lat()
    lon = place.get_lon()
    request_url = make_request_url(lat,lon)
    request_status = process_request(request_url,db,code,insert_time,error_file)
    if request_status == 0:
        error += 1
db.close()
error_file.write('finish crawling at {}, error {}\n'.format(insert_time.__str__(),error))
error_file.write('\n')
error_file.close()