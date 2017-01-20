import datetime
from urllib import request
import json
import pymysql
import os
import read_new_york_places


date_form = '''%Y-%m-%dT%H:%M:%S'''
places = read_new_york_places.read_places()
insert_sql = '''
insert into aeris (code,forecast_time,temperature,dewpoint,outlook,humidity,insert_time) VALUES ('%s','%s','%s','%s','%s','%s','%s')
'''

def make_request_url(lat,lon):
    base = 'http://api.aerisapi.com/forecasts/'
    base+=str(lat)
    base+=','
    base+=str(lon)
    base+='?client_id=86iV2gubesh9UEujSUYfj&client_secret=m9aGtUHzVZ1KfvZ4hF3OnzYVdVtbi22BowAfell1&filter=1hr&limit=200'
    return base

def process_request(request_url,db,code,insert_time,error_file):
    res = request.urlopen(request_url)
    res_string = str(res.read(),'utf-8')
    json_obj = json.loads(res_string)
    isError = json_obj['error'] != None
    if isError:
        error_file.write('{\n')
        error_file.write('time:{} \n'.format(insert_time.__str__()))
        error_file.write('code: {}\n'.format(code))
        error_file.write('request url: {}\n'.format(request_url))
        error_file.write('error code: {}\n'.format(json_obj['error']['code']))
        error_file.write('error desc: {}\n'.format(json_obj['error']['description']))
        error_file.write('}\n')
        return 0
    else:
        periods = json_obj['response'][0]['periods']
        print(len(periods))
        for period in periods:
            valid_time = period['validTime']
            forecast_time = make_date_from_string(valid_time)
            temp = period['tempC']
            humidity = period['humidity']
            dewpoint = period['dewpointC']
            outlook = period['weather']
            sql_temp = insert_sql % (code,forecast_time,temp,dewpoint,outlook,humidity,insert_time)
            try:
                cursor = db.cursor()
                cursor.execute(sql_temp)
                db.commit()
            except:
                print('sql error')
                db.rollback()
                return 0
        return 1

def make_date_from_string(date_string):
    date_string = date_string.replace('-05:00','')
    return datetime.datetime.strptime(date_string,date_form)


error_url = 'http://api.aerisapi.com/forecasts/40.78333,?client_id=86iV2gubesh9UEujSUYfj&client_secret=m9aGtUHzVZ1KfvZ4hF3OnzYVdVtbi22BowAfell1&filter=1hr&limit=5'
right_url = 'http://api.aerisapi.com/forecasts/40.78333,-73.96667?client_id=86iV2gubesh9UEujSUYfj&client_secret=m9aGtUHzVZ1KfvZ4hF3OnzYVdVtbi22BowAfell1&filter=1hr&limit=200'
# adate = datetime.datetime.fromtimestamp(1484798400)
# dstring = '2017-01-18T23:00:00-05:00'
# dstring = dstring.replace('-05:00','')
# form = '''%Y-%m-%dT%H:%M:%S'''
# print(dstring)
# print(datetime.datetime.strptime(dstring,form))
db = pymysql.connect('localhost', 'root', 'Aa19890318', 'weather')
insert_time = datetime.datetime.now()
error_file = error_file = open(os.path.join(os.path.dirname(__file__), 'aeris.log'),'a')
error_file.write('begin to crawl aeris at {}\n'.format(insert_time.__str__()))
counter = 0
error = 0
for place in places.values():
    code = place.get_code()
    counter+=1
    print('processing aeris online {}, {}/{}'.format(code, counter, len(places.values())))
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