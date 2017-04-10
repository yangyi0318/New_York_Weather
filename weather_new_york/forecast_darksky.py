import datetime
from urllib import request
import logging
import urllib.error
import json
import pymysql
import os
import basic_functions



def make_request_url(lat,lon):
    base = 'https://api.darksky.net/forecast/daf636210aff52e2766b9f01ed93831b/'
    base+=str(lat)
    base+=','
    base+=str(lon)
    return base

# def ts_to_eastern(ts):
#     return datetime.datetime.fromtimestamp(ts,eastern)

# def f_to_c(f):
#     f = float(f)
#     c = (f-32) * 0.5556
#     c = '%.1f' % c
#     return str(c)

def process_request(request_url,post_code):
    try:
        res = request.urlopen(request_url)
        res_string = str(res.read(),'utf-8')
    except Exception as e:
        logging.error('request error while processing %s, error: %s' % (request_url,e))
        return
    json_obj = json.loads(res_string)
    hourly_data = json_obj['hourly']['data']
    results = []
    for data in hourly_data:
        ts = int(data['time'])
        forecast_time = basic_functions.ts_to_eastern(ts)
        forecast_time=forecast_time.replace(tzinfo=None)
        weather_text = data['summary']
        temp = data['temperature']
        temp = basic_functions.temperature_f2c(temp)
        feels_temp = data['apparentTemperature']
        feels_temp = basic_functions.temperature_f2c(feels_temp)
        dewPoint = data['dewPoint']
        dewPoint = basic_functions.temperature_f2c(dewPoint)
        humidity = data['humidity']
        humidity = float(float(humidity) * 100)
        results.append((post_code,forecast_time,temp,feels_temp,humidity,dewPoint,weather_text))
    return results

if __name__ == '__main__':

    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log/darksky_forecast.log'),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    location_keys = basic_functions.read_accu_keys()

    sql_query = '''
            insert into darksky_forecast (post_code,observation_time,temperature,feel_temperature,
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
    for key_tuple in location_keys:
        post_code = key_tuple[0]
        lat = key_tuple[2]
        lon = key_tuple[3]
        counter+=1
        print('processing dark sky {}, {}/{}'.format(post_code, counter, len(location_keys)))
        request_url = make_request_url(lat,lon)
        results = process_request(request_url,post_code)
        try:
            for result in results:
                db.cursor().execute(
                    sql_query % (result[0], result[1], result[2], result[3], result[4], result[5], result[6]))
                db.commit()
        except Exception as e:
            logging.error('sql error %s', e)
            db.rollback()
    db.close()
    logging.info('crawl finished')