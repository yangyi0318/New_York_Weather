import read_new_york_places
import pymysql
from urllib import request
import xml.dom.minidom
import time
import datetime

places = read_new_york_places.read_places()

# for row_entry in places.values():
#     res = request.urlopen(row_entry.get_gov_url())
#     res_string = str(res.read(),'utf-8')
#     print(res_string)
#     print()

def populate_places_to_database(places):
    db = pymysql.connect('localhost', 'root', 'Aa19890318', 'weather')
    cursor = db.cursor()
    sql_string = '''
    insert into new_york (code,name,lat,lon,zip,url) VALUES ('%s','%s','%s','%s','%s','%s')
    '''
    for row_entry in places.values():
        sql_string_temp = sql_string
        sql_string_temp=sql_string_temp % (row_entry.get_code(),row_entry.get_name(),row_entry.get_lat(),row_entry.get_lon(),row_entry.get_zip(),row_entry.get_gov_url())
        # print(sql_string_temp)
        cursor.execute(sql_string_temp)
        db.commit()
    db.close()

##log all weathers
def log_weather():
    db = pymysql.connect('localhost', 'root', 'Aa19890318', 'weather')
    places = read_new_york_places.read_places()
    for row_entry in places.values():
        res = request.urlopen(row_entry.get_gov_url())
        res_string = str(res.read(),'utf-8')
        dom_tree = xml.dom.minidom.parseString(res_string)
        # if row_entry.get_code() == 'K6B9':
        log_single_weather(db,row_entry.get_code(),dom_tree)


    db.close()

## log single weather
def log_single_weather(db,code,dom_tree):
    print('processing ',code)
    sql_string = '''
    insert into new_york_gov (code,observation_time,weather,temperature_c,humidity,wind_string,wind_dir,
    wind_degrees,wind_mph,wind_kt,pressure_mb,pressure_in,dewpoint_c,visibility_mi) VALUES
    ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
    '''
    obv_time = dom_tree.getElementsByTagName('observation_time_rfc822')[0].firstChild.nodeValue
    # obv_time = trim_datetime_string(obv_time)
    obv_time = datetime_string_to_object(obv_time)
    exist = check_entry_exist(db,code,obv_time)
    if exist:
        print('exist')
        return
    # print(obv_time)
    weather = str(get_node_data(dom_tree,'weather'))
    # print(weather)
    temperature_c = str(get_node_data(dom_tree,'temp_c'))
    # print(temperature_c)
    humidity = str(get_node_data(dom_tree,'relative_humidity'))
    # print(humidity)
    wind_string = str(get_node_data(dom_tree,'wind_string'))
    # print(wind_string)
    wind_dir = str(get_node_data(dom_tree,'wind_dir'))
    # print(wind_dir)
    wind_degrees = str(get_node_data(dom_tree,'wind_degrees'))
    # print(wind_degrees)
    wind_mph = str(get_node_data(dom_tree,'wind_mph'))
    # print(wind_mph)
    wind_kt = str(get_node_data(dom_tree,'wind_kt'))
    # print(wind_kt)
    # print(dom_tree.getElementsByTagName('pressure_mb'))
    # pressure_mb = float(dom_tree.getElementsByTagName('pressure_mb')[0].firstChild.nodeValue)
    pressure_mb = str(get_node_data(dom_tree,'pressure_mb'))
    # print(pressure_mb)
    pressure_in = str(get_node_data(dom_tree,'pressure_in'))
    # print(pressure_in)
    dewpoint_c = str(get_node_data(dom_tree,'dewpoint_c'))
    # print(dewpoint_c)
    visibility_mi = str(get_node_data(dom_tree,'visibility_mi'))
    # print(visibility_mi)
    sql_string = sql_string % (code,obv_time,weather,temperature_c,humidity,wind_string,wind_dir,wind_degrees,wind_mph,wind_kt,pressure_mb,pressure_in,dewpoint_c,visibility_mi)
    try:
        db.cursor().execute(sql_string)
        db.commit()
    except:
        print('database error')
        db.rollback()
    # print(sql_string)

def get_node_data(dom_tree,tag_name):
    tags = dom_tree.getElementsByTagName(tag_name)
    if len(tags) == 0:
        return ''
    else:
        return dom_tree.getElementsByTagName(tag_name)[0].firstChild.nodeValue

def datetime_string_to_object(datetime_string):
    form = '''%d %b %Y %H:%M:%S'''
    return datetime.datetime.strptime(trim_datetime_string(datetime_string),form)

def trim_datetime_string(datetime_string):
    new_datetime_string = datetime_string.split(', ')[1]
    return ' '.join(new_datetime_string.split(' ')[0:-1])

def check_entry_exist(db,code,observation_time):
    sql_search_string = '''
        select * from new_york_gov where code = '{0}' and observation_time = '{1}'
    '''
    sql_search_temp = sql_search_string
    sql_search_temp = sql_search_temp.format(code, observation_time)
    # print(sql_search_temp)
    cursor = db.cursor()
    cursor.execute(sql_search_temp)
    # print(cursor.rowcount)
    if cursor.rowcount > 0:
        return True
    else:
        return False
if '__main__' == __name__:
    log_weather()
    # populate_places_to_database(places)



    # form = '''%a, %d %m %Y %H:%M:%S %Z'''
    # d = datetime.datetime.strptime('Mon, 16 Jan 2017 20:51:00 -0500', form)
    # form = '''%a, %d %b %Y %H:%M:%S %z'''
    # d = datetime.datetime.strptime('Mon, 16 Jan 2017 20:51:00  -0500', form)
    # # print(datetime.datetime.now().strftime(form))
    # print(d)


    # db = pymysql.connect('localhost', 'root', '', 'weather')
    # cursor = db.cursor()
    # # d = datetime_string_to_object('Mon, 16 Jan 2017 20:51:00 -0500')
    # # print(d)
    # a = check_entry_exist(db,'KALB','16 Jan 2017 23:51:00')
    # print(a)
