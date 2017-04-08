import basic_functions
from urllib import request
from urllib import parse
import os
import json
import datetime
import pymysql
import logging

logging.basicConfig(filename=os.path.join(os.path.dirname(__file__),'log/accu_forecast.log'),
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

class MakeURL(object):
    base = 'http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/'
    api_key = 'llcQpcesWLPhLpkpZQTELTwNQy3J2WzA'

    def get_url(self,post_code,location_key):
        params = {'apikey': MakeURL.api_key,'details':'true'}
        url_parts = list(parse.urlparse(MakeURL.base))
        path=(os.path.join(url_parts[2],location_key))
        query = dict(parse.parse_qsl(url_parts[4]))
        query.update(params)
        url_parts[2] = path
        url_parts[4] = parse.urlencode(query)
        return parse.urlunparse(url_parts)

def process_request(request_url,post_code,location_key):
    try:
        res = request.urlopen(request_url)
    except Exception as e:
        logging.error('process request %s, %post code %s, network error: %s \n' %(request_url,post_code,e))
        print(e)
    res_string = str(res.read(), 'utf-8')
    json_obj = json.loads(res_string)
    print(len(json_obj))
    results = []
    try:
        for day_forecast in json_obj:
            obs_time_str = day_forecast.get('DateTime',None)
            #observation time's python object
            obs_python_obj = datetime.datetime.strptime(obs_time_str[0:-6],'%Y-%m-%dT%H:%M:%S')
            temperature=basic_functions.temperature_f2c(day_forecast.get('Temperature').get('Value'))
            feel_temperature=basic_functions.temperature_f2c(day_forecast.get('RealFeelTemperature').get('Value'))
            humidity=day_forecast.get('RelativeHumidity')
            dewpoint=basic_functions.temperature_f2c(day_forecast.get('DewPoint').get('Value'))
            weather_text= day_forecast.get('IconPhrase')
            results.append ((post_code,obs_python_obj,temperature,feel_temperature,
                             humidity,dewpoint,weather_text))
    except Exception as e:
        logging.error('process request %s, %post code %s, json error: %s \n' %(request_url,post_code,e))
    return results

if __name__ == '__main__':
    location_keys = basic_functions.read_accu_keys()
    u = MakeURL()
    sql_query = '''
    insert into accu_forecast (post_code,observation_time,temperature,feel_temperature,
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
    for key_tuple in location_keys:
        post_code = key_tuple[0]
        location_key = key_tuple[0]
        res_str = u.get_url(post_code,location_key)
        print(res_str)
        results = process_request(res_str,post_code,location_key)
        try:
            for result in results:
                db.cursor().execute(
                    sql_query % (result[0],result[1],result[2],result[3],result[4],result[5],result[6])
                )
        except Exception as e:
            logging.error('sql error %s',e)
            db.commit()
    db.close()
    logging.info('crawl finished')

