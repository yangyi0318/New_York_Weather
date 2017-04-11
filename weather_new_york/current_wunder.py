from urllib import request
from bs4 import BeautifulSoup
import datetime
import logging
import basic_functions
import pymysql
import os

def make_url(post_code):
    return 'https://www.wunderground.com/cgi-bin/findweather/getForecast?query='+post_code

def process_request(request_url,post_code):
    try:
        res = request.urlopen(request_url)
        res_string = str(res.read(),'utf-8')
    except Exception as e:
        logging.error('request error while processing %s, error: %s' % (request_url,e))
        return
    soup = BeautifulSoup(res_string, 'html.parser', from_encoding='utf-8')
    div_current = soup.select_one('div#current')
    # div_left = div_current.select_one('div.small-12.medium-6.large-4.columns.cc1')
    # print(div_left)
    div_curTemp = div_current.select_one('div#curTemp')
    current_temp = float(div_curTemp.select_one('span.wx-value').get_text())
    div_curFeel = div_current.select_one('div#curFeel')
    current_feel_temp = float(div_curFeel.select_one('span.wx-value').get_text())
    div_curCond = div_current.select_one('div#curCond')
    current_condition = div_curCond.select_one('span.wx-value').get_text()

    div_right = div_current.select_one('div.show-for-large-up.large-3.columns.cc2')
    trs = div_right.select_one('table').select('tr')
    current_dewpoint = -99.9
    current_humidity = -99.9
    for tr in trs:
        tds = tr.select('td')
        if tds[0].get_text() == 'Dew Point':
            current_dewpoint = float(tds[1].select_one('span.wx-value').get_text())
        if tds[0].get_text() == 'Humidity':
            current_humidity = float(tds[1].select_one('span.wx-value').get_text())

    local_time = soup.select_one('div#location').select_one('div.local-time').select_one('span')
    time_python = datetime.datetime.strptime(local_time.get_text(), '%I:%M %p EDT on %B %d, %Y')
    return ((post_code,time_python,current_temp,current_feel_temp,current_humidity,current_dewpoint,current_condition))


if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log/current_wunder.log'),
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)
    logging.info('begin to crawl')

    location_keys = basic_functions.read_accu_keys()

    sql_query = '''
                insert into wunder_current (post_code,observation_time,temperature,feel_temperature,
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
        request_url = make_url(post_code)
        result = process_request(request_url,post_code)
        print(result)
        try:
            db.cursor().execute(
                sql_query % (result[0], result[1], result[2], result[3], result[4], result[5], result[6]))
            db.commit()
        except Exception as e:
            logging.error('sql error %s', e)
            db.rollback()

    db.close()
    logging.info('finish crawl')

