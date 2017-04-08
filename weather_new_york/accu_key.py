import datetime
from urllib import request
import json
import pymysql
import os
import read_new_york_places
import basic_functions
from urllib import parse


# url = "http://stackoverflow.com/search?q=question"
# params = {'lang':'en','tag':'python'}
#
# url_parts = list(parse.urlparse(url))
# print(url_parts)
# query = dict(parse.parse_qsl(url_parts[4]))
# query.update(params)
#
# url_parts[4] = parse.urlencode(query)
#
# print(parse.urlunparse(url_parts))

class MakeURL(object):
    base = 'http://dataservice.accuweather.com/locations/v1/postalcodes/search.json'
    api_key = 'llcQpcesWLPhLpkpZQTELTwNQy3J2WzA'

    def get_url(self,post_code):
        params = {'q': post_code, 'apikey': MakeURL.api_key}
        url_parts = list(parse.urlparse(MakeURL.base))
        query = dict(parse.parse_qsl(url_parts[4]))
        query.update(params)
        url_parts[4] = parse.urlencode(query)
        return parse.urlunparse(url_parts)



# right_request_url = 'http://dataservice.accuweather.com/locations/v1/cities/geoposition/search.json?q=42.74722,-73.79912&apikey=llcQpcesWLPhLpkpZQTELTwNQy3J2WzA'
# right_request_url = make_request_url(42.74722,-73.79912)
def process_request(request_url,post_code):
    res = request.urlopen(request_url)
    res_string = str(res.read(),'utf-8')
    json_obj = json.loads(res_string)
    results = []
    results.append(post_code)
    results.append(json_obj[0].get('Key'))
    results.append(json_obj[0].get('GeoPosition').get('Latitude'))
    results.append(json_obj[0].get('GeoPosition').get('Longitude'))
    # print(json_obj)
    # print(json_obj[0].get('Key'))
    # print(json_obj[0].get('GeoPosition').get('Latitude'))
    return results

if __name__ == '__main__':
    db = pymysql.connect(basic_functions.Database_Config.get_host(),
                         basic_functions.Database_Config.get_username(),
                         basic_functions.Database_Config.get_password(),
                         basic_functions.Database_Config.get_database())
    post_codes = basic_functions.read_post_codes()
    u = MakeURL()
    # u.get_url('12345')
    # print(u.get_url('123456'))
    for post_code in post_codes:
        request_url = u.get_url(post_code)
        results = process_request(request_url,post_code)
        try:
            db.cursor().execute('''
            insert into accu_location_key (post_code,location_key,latitude,longitude)
            VALUES ('%s','%s',%f,%f)
            ''' %(results[0],results[1],results[2],results[3]))
            db.commit()
        except Exception as e:
            db.rollback()
            print("error: %s" %e)

        print(results)
    db.close()

# db = pymysql.connect(database_config.get_host(), database_config.get_username(), database_config.get_password(),
#                          database_config.get_database())
# process_request(right_request_url,db,'aaa')
#
# db.close()
