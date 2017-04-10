import datetime
from urllib import request
import json
import pymysql
import os
import logging
import basic_functions


date_form = '''%Y-%m-%dT%H:%M:%S'''


def make_request_url(post_code):
    base = 'http://api.aerisapi.com/forecasts/'
    base+=post_code
    base+='?client_id=86iV2gubesh9UEujSUYfj&client_secret=m9aGtUHzVZ1KfvZ4hF3OnzYVdVtbi22BowAfell1&filter=1hr&limit=200'
    return base

def process_request(request_url,post_code):
    try:
        res = request.urlopen(request_url)
        res_string = str(res.read(),'utf-8')
    except Exception as e:
        logging.error('request error while processing %s, error: %s' % (request_url,e))
        return
    json_obj = json.loads(res_string)
    isError = json_obj['error'] != None
    results = []
    if isError:
        logging.error('error processing %s error code %s error desc %s' % (request_url,
                                                                           json_obj['error']['code'],
                                                                           json_obj['error']['description']))
        return
    else:
        periods = json_obj['response'][0]['periods']
        for period in periods:
            valid_time = period['validTime']
            forecast_time = make_date_from_string(valid_time)
            temp = period['tempC']
            feel_temp = period['feelslikeC']
            humidity = period['humidity']
            dewpoint = period['dewpointC']
            weather_text = period['weather']
            results.append((post_code,forecast_time,temp,feel_temp,humidity,dewpoint,weather_text))
    return results

def make_date_from_string(date_string):
    date_string = date_string.replace('-04:00','')
    return datetime.datetime.strptime(date_string,date_form)


if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log/aeris.log'),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    location_keys = basic_functions.read_accu_keys()

    sql_query = '''
            insert into aeris_forecast (post_code,observation_time,temperature,feel_temperature,
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
        counter+=1
        print('processing aeris online {}, {}/{}'.format(post_code, counter, len(location_keys)))
        request_url = make_request_url(post_code)
        results=process_request(request_url,post_code)
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
        # break
        # request_status = process_request(request_url,db,code,insert_time,error_file)
        # if request_status == 0:
        #     error += 1
    db.close()
    logging.info('crawl finished')