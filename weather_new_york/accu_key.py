import datetime
from urllib import request
import json
import pymysql
import os
import read_new_york_places
import database_config


places = read_new_york_places.read_places()

def make_request_url(lat,lon):
    base = 'http://dataservice.accuweather.com/locations/v1/cities/geoposition/search.json?q='
    base+=str(lat)
    base+=','
    base+=str(lon)
    base+='&apikey=llcQpcesWLPhLpkpZQTELTwNQy3J2WzA'
    return base

right_request_url = 'http://dataservice.accuweather.com/locations/v1/cities/geoposition/search.json?q=42.74722,-73.79912&apikey=llcQpcesWLPhLpkpZQTELTwNQy3J2WzA'
right_request_url = make_request_url(42.74722,-73.79912)
def process_request(request_url,db,code):
    res = request.urlopen(request_url)
    res_string = str(res.read(),'utf-8')
    json_obj = json.loads(res_string)
    print(json_obj)

db = pymysql.connect(database_config.get_host(), database_config.get_username(), database_config.get_password(),
                         database_config.get_database())
process_request(right_request_url,db,'aaa')
#
# db.close()
