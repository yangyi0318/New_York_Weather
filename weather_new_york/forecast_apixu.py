import basic_functions
from urllib import request
from urllib import parse
import os
import json
import datetime
import pymysql
import logging


def make_request_url(post_code):
    base = 'http://api.apixu.com/v1/forecast.json?key=bf942c2d1cff42a2949232505170904&days=3&q='
    base+=post_code
    return base

def process_request(request_url,post_code):
    try:
        res = request.urlopen(request_url)
        res_string = str(res.read(), 'utf-8')
        print(res_string)
    except Exception as e:
        logging.error('process request %s, post code %s, network error: %s \n' %(request_url,post_code,e))
        print(e)
    json_obj = json.loads(res_string)
    results = []
    isError = json_obj.get('error') is not None
    if isError:
        logging.error('api error processing request %s, error code %s, message %s'
                      % (request_url,
                         request_url, json_obj.get('error').get('code'),
                         request_url, json_obj.get('error').get('message')))
        return
    try:

        forecast_days = json_obj.get('forecast').get('forecastday')
        for forecast_day in forecast_days:
            hours_data = forecast_day.get('hour')
            for hour_data in hours_data:
                time = hour_data['time']
                obs_python_obj = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M')
                temp = float(hour_data['temp_c'])
                feels_temp = float(hour_data['feelslike_c'])
                humidity = float(hour_data['humidity'])
                dewpoint = float(hour_data['dewpoint_c'])
                weather_text = hour_data['condition']['text']
                results.append((post_code,obs_python_obj,temp,feels_temp,humidity,dewpoint,weather_text))
    except Exception as e:
        logging.error('process request %s, %post code %s, json error: %s \n' %(request_url,post_code,e))
    return results

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log/apixu.log'),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    location_keys = basic_functions.read_accu_keys()
    sql_query = '''
    insert into apixu_forecast (post_code,observation_time,temperature,feel_temperature,
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
        counter+=1
        post_code = key_tuple[0]
        location_key = key_tuple[0]
        res_str = make_request_url(post_code)
        results = process_request(res_str,post_code)
        try:
            for result in results:
                db.cursor().execute(
                    sql_query % (result[0],result[1],result[2],result[3],result[4],result[5],result[6])
                )
            db.commit()

        except Exception as e:
            logging.error('sql error %s',e)
            db.rollback()
        # break
    db.close()
    logging.info('crawl finished')

