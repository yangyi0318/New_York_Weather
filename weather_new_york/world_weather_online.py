import datetime
from urllib import request
import json
import pymysql
import os
import read_new_york_places


places = read_new_york_places.read_places()
insert_sql = '''
insert into world_weather_online (code,forecast_time,temperature,dewpoint,outlook,humidity,insert_time) VALUES ('%s','%s','%s','%s','%s','%s','%s')
'''

def make_request_url(lat,lon):
    base = 'http://api.worldweatheronline.com/premium/v1/weather.ashx?key=fd24212fbee540bb93e04810170501&q='
    base+=str(lat)
    base+=','
    base+=str(lon)
    base+='&num_of_days=5&tp=1&format=json'
    return base

def process_request(request_url,db,code,insert_time,error_file):
    res = request.urlopen(request_url)
    res_string = str(res.read(),'utf-8')
    json_obj = json.loads(res_string)
    isError = 'error' in json_obj['data']
    if isError:
        print(json_obj['data']['error'][0]['msg'])
        error_file.write('{\n')
        error_file.write('time:{} \n'.format(insert_time.__str__()))
        error_file.write('code: {}\n'.format(code))
        error_file.write('request url: {}\n'.format(request_url))
        error_file.write('error desc: {}\n'.format(json_obj['data']['error'][0]['msg']))
        error_file.write('}\n')
        return 0
    else:
        weahter_tag = json_obj['data']['weather']
        for weather_day in weahter_tag:
            date = weather_day['date']
            date_split = date.split('-')
            year = int(date_split[0])
            month = int(date_split[1])
            day = int(date_split[2])
            hourly = weather_day['hourly']
            for weather_hour in hourly:
                hour_time = int(int(weather_hour['time'])/100)
                forecast_time = datetime.datetime(year,month,day,hour_time,0,0)
                temp = weather_hour['tempC']
                windspeedKmph = weather_hour['windspeedKmph']
                winddirDegree = weather_hour['winddirDegree']
                winddir16Point = weather_hour['winddir16Point']
                humidity = weather_hour['humidity']
                visibility = weather_hour['visibility']
                pressure = weather_hour['pressure']
                dewpoint = weather_hour['DewPointC']
                outlook = weather_hour['weatherDesc'][0]['value']
                sql_temp = insert_sql %(code,forecast_time,temp,dewpoint,outlook,humidity,insert_time)
                try:
                    cursor = db.cursor()
                    cursor.execute(sql_temp)
                    db.commit()
                except:
                    db.rollback()
                    return 0
        return 1

error_request_url = 'http://api.worldweatheronline.com/premium/v1/weather.ashx?key=fd24212fbee540bb93e04810170501&q=-73.79912&num_of_days=5&tp=1&format=json'
right_request_url = 'http://api.worldweatheronline.com/premium/v1/weather.ashx?key=fd24212fbee540bb93e04810170501&q=42.74722,-73.79912&num_of_days=5&tp=1&format=json'


db = pymysql.connect('localhost', 'root', 'Aa19890318', 'weather')
insert_time = datetime.datetime.now()
error_file = error_file = open(os.path.join(os.path.dirname(__file__), 'world_weather_online.log'),'a')
error_file.write('begin to crawl world weather online at {}\n'.format(insert_time.__str__()))
counter = 0
error = 0
for place in places.values():
    code = place.get_code()
    counter+=1
    print('processing world weather online {}, {}/{}'.format(code, counter, len(places.values())))
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