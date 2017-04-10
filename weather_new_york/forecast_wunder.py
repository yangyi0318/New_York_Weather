from urllib import request
import json
import datetime
import read_new_york_places
import time
import pymysql
import os
import basic_functions

import logging



def make_request_url(post_code):
    base = 'http://api.wunderground.com/api/f643f2dbf298c022/hourly10day/q/'
    base+= post_code
    base+='.json'
    return base


def process_request(request_url,post_code):
    try:
        res = request.urlopen(request_url)
        res_string = str(res.read(),'utf-8')
    except Exception as e:
        logging.error('request error while processing %s, error: %s' % (request_url,e))
        return
    json_obj = json.loads(res_string)
    json_response = json_obj['response']
    isError = 'error' in json_response
    if isError:
        error_type = json_obj['response']['error']['type']
        error_desc = json_obj['response']['error']['description']
        logging.error('error processing %s error type %s error desc %s' %(request_url,
                                                                          error_type,error_desc))
        return
    results = []
    hourly_forecast = json_obj['hourly_forecast']
    for a_hourly_forecast in hourly_forecast:
        hour = int(a_hourly_forecast['FCTTIME']['hour'])
        miniute = int(a_hourly_forecast['FCTTIME']['min'])
        sec = int(a_hourly_forecast['FCTTIME']['sec'])
        day = int(a_hourly_forecast['FCTTIME']['mday'])
        month = int(a_hourly_forecast['FCTTIME']['mon'])
        year = int(a_hourly_forecast['FCTTIME']['year'])
        temp = float(a_hourly_forecast['temp']['metric'])
        feel_temp = float(a_hourly_forecast['feelslike']['metric'])
        dewpoint = float(a_hourly_forecast['dewpoint']['metric'])
        weather_text = a_hourly_forecast['wx']
        humidity = float(a_hourly_forecast['humidity'])
        forecast_time = datetime.datetime(year,month,day,hour,miniute,sec)
        results.append((post_code,forecast_time,temp,feel_temp,humidity,dewpoint,weather_text))
    return results

if __name__ == '__main__':

    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log/wunder_forecast.log'),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    location_keys = basic_functions.read_accu_keys()

    sql_query = '''
        insert into wunder_forecast (post_code,observation_time,temperature,feel_temperature,
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
        counter += 1
        post_code = key_tuple[0]
        print('processing wunder {}, {}/{}'.format(post_code,counter,len(location_keys)))
        request_url = make_request_url(post_code)
        print(request_url)
        results = process_request(request_url,post_code)
        if results is None:
            continue
        try:
            for result in results:
                db.cursor().execute(sql_query %(result[0],result[1],result[2],result[3],result[4],result[5],result[6]))
                db.commit()
        except Exception as e:
            logging.error('sql error %s', e)
            db.rollback()
        # break
        if counter % 9 == 0:
            print('start sleeping 100 secs')
            time.sleep(100)
    db.close()
    logging.info('crawl finished')



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
