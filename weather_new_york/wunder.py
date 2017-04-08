from urllib import request
import json
import datetime
import read_new_york_places
import time
import pymysql
import os

places = read_new_york_places.read_places()
insert_sql = '''
insert into wunder (code,forecast_time,temperature,dewpoint,outlook,humidity,insert_time)
VALUES ('%s','%s','%s','%s','%s','%s','%s')
'''

def make_request_url(lat,lon):
    base = 'http://api.wunderground.com/api/f643f2dbf298c022/hourly10day/q/'
    base+=str(lat)
    base+=','
    base+=str(lon)
    base+='.json'
    return base


def process_request(request_url,db,code,insert_time,error_file):
    res = request.urlopen(request_url)
    res_string = str(res.read(),'utf-8')
    json_obj = json.loads(res_string)
    json_response = json_obj['response']
    isError = 'error' in json_response
    if isError:
        error_type = json_obj['response']['error']['type']
        error_desc = json_obj['response']['error']['description']
        error_file.write('{\n')
        error_file.write('time:{} \n'.format(insert_time.__str__()))
        error_file.write('code: {}\n'.format(code))
        error_file.write('request url: {}\n'.format(request_url))
        error_file.write('error type: {}\n'.format(error_type))
        error_file.write('error desc: {}\n'.format(error_desc))
        error_file.write('}\n')
        return 0

    hourly_forecast = json_obj['hourly_forecast']
    for a_hourly_forecast in hourly_forecast:
        hour = int(a_hourly_forecast['FCTTIME']['hour'])
        miniute = int(a_hourly_forecast['FCTTIME']['min'])
        sec = int(a_hourly_forecast['FCTTIME']['sec'])
        day = int(a_hourly_forecast['FCTTIME']['mday'])
        month = int(a_hourly_forecast['FCTTIME']['mon'])
        year = int(a_hourly_forecast['FCTTIME']['year'])
        temp = a_hourly_forecast['temp']['metric']
        dewpoint = a_hourly_forecast['dewpoint']['metric']
        outlook = a_hourly_forecast['wx']
        humidity = a_hourly_forecast['humidity']
        forecast_time = datetime.datetime(year,month,day,hour,miniute,sec)
        sql_string_temp = insert_sql % (code,forecast_time,temp,dewpoint,outlook,humidity,insert_time)
        try:
            cursor = db.cursor()
            cursor.execute(sql_string_temp)
            db.commit()
        except:
            db.rollback()
            return 0
    return 1

db = pymysql.connect(database_config.get_host(), database_config.get_username(), database_config.get_password(),
                         database_config.get_database())
insert_time = datetime.datetime.now()
counter = 0
error = 0
error_file = open(os.path.join(os.path.dirname(__file__), 'wunder.log'),'a')
error_file.write('begin to crawl wunder at {}'.format(insert_time.__str__()))
for place in places.values():
    counter += 1
    code = place.get_code()
    print('processing wunder {}, {}/{}'.format(code,counter,len(places.values())))
    lat = place.get_lat()
    lon = place.get_lon()
    request_url = make_request_url(lat,lon)
    # res = request.urlopen(request_url)
    # print(request_url)
    # print(str(res.read(),'utf-8'))
    # temp = 'http://api.wunderground.com/api/f643f2dbf298c022/conditions/q/CA/-123.json'
    status_code = process_request(request_url,db,code,insert_time,error_file)
    if status_code == 0:
        error+=1
    if counter % 9 == 0:
        time.sleep(100)
db.close()
error_file.write('finish crawling at {}, error {}\n'.format(insert_time.__str__(),error))
error_file.write('\n')
error_file.close()


# res = request.urlopen('http://api.wunderground.com/api/f643f2dbf298c022/hourly10day/q/42.74722,-73.79912.json')
# res_string = str(res.read(),'utf-8')
# json_obj = json.loads(res_string)
# json_response = json_obj['response']
# print('error' in json_response)
# hourly_forecast = json_obj['hourly_forecast']
# counter = 0
# for a_hourly_forecast in hourly_forecast:
#     # FCTTIME = print(a_hourly_forecast['FCTTIME'])
#     hour = int(a_hourly_forecast['FCTTIME']['hour'])
#     miniute = int(a_hourly_forecast['FCTTIME']['min'])
#     sec = int(a_hourly_forecast['FCTTIME']['sec'])
#     day = int(a_hourly_forecast['FCTTIME']['mday'])
#     month = int(a_hourly_forecast['FCTTIME']['mon'])
#     year = int(a_hourly_forecast['FCTTIME']['year'])
#     temp = a_hourly_forecast['temp']['metric']
#     dewpoint = a_hourly_forecast['dewpoint']['metric']
#     outlook = a_hourly_forecast['wx']
#     humidity = a_hourly_forecast['humidity']
#     dt = datetime.datetime(year,month,day,hour,miniute,sec)
#     print(dt)
#     print()
