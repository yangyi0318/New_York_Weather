import datetime
from urllib import request
import json
import pymysql
import os
import basic_functions
import logging


insert_sql = '''
insert into world_weather_online (code,forecast_time,temperature,dewpoint,outlook,humidity,insert_time) VALUES ('%s','%s','%s','%s','%s','%s','%s')
'''

def make_request_url(lat,lon):
    base = 'http://api.worldweatheronline.com/premium/v1/weather.ashx?key=0fdf3447358d4a6fbae215852170904&q='
    base+=str(lat)
    base+=','
    base+=str(lon)
    base+='&num_of_days=5&tp=1&format=json'
    return base

def process_request(request_url,post_code):
    try:
        res = request.urlopen(request_url)
        res_string = str(res.read(),'utf-8')
    except Exception as e:
        logging.error('request error while processing %s, error: %s' % (request_url,e))
        return
    json_obj = json.loads(res_string)
    isError = 'error' in json_obj['data']
    if isError:
        logging.error('error processing %s error desc %s' % (request_url,
                                                                           json_obj['data']['error'][0]['msg']))
        return
    else:
        results = []
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
                temp = float(weather_hour['tempC'])
                feels_temp = float(weather_hour['FeelsLikeC'])
                humidity = float(weather_hour['humidity'])
                dewpoint = float(weather_hour['DewPointC'])
                weather_text = weather_hour['weatherDesc'][0]['value']
                results.append((post_code,forecast_time,temp,feels_temp,humidity,dewpoint,weather_text))
        return results


if __name__ == '__main__':

    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log/worldweatheronline.log'),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    location_keys = basic_functions.read_accu_keys()

    sql_query = '''
                insert into worldweatheronline_forecast (post_code,observation_time,temperature,feel_temperature,
                         humidity,dewpoint,weather_text) VALUES ('%s','%s',%f,%f,%f,%f,'%s')
                on duplicate KEY UPDATE 
                  temperature = VALUES (temperature),
                  feel_temperature = VALUES (feel_temperature),
                  humidity = VALUES (humidity),
                  dewpoint = VALUES (dewpoint),
                  weather_text = VALUES (weather_text)
                '''

    db = pymysql.connect(basic_functions.Database_Config.get_host(),
                         basic_functions.Database_Config.get_username(),
                         basic_functions.Database_Config.get_password(),
                         basic_functions.Database_Config.get_database())
    counter = 0
    error = 0
    for key_tuple in location_keys:
        post_code = key_tuple[0]
        lat = key_tuple[2]
        lon = key_tuple[3]
        counter+=1
        print('processing world weather online {}, {}/{}'.format(post_code, counter, len(location_keys)))
        request_url = make_request_url(lat,lon)
        results = process_request(request_url,post_code)
        if results is None:
            continue
        try:
            for result in results:
                db.cursor().execute(
                    sql_query % (result[0], result[1], result[2], result[3], result[4], result[5], result[6]))
                db.commit()
        except Exception as e:
            logging.error('sql error %s', e)
            db.rollback()
        # if counter == 2:
        #     break


    db.close()