# from urllib import request
# import xml.dom.minidom
# res = request.urlopen('http://w1.weather.gov/xml/current_obs/KALB.xml')
#
# res_data = res.read()
#
# res_string = str(res_data,'utf-8')
# dom_tree = xml.dom.minidom.parseString(res_string)
# print('lat')
# print(dom_tree.getElementsByTagName('latitude')[0].firstChild.nodeValue)
# print('lon')
# print(dom_tree.getElementsByTagName('longitude')[0].firstChild.nodeValue)
# print('obv time')
# print(dom_tree.getElementsByTagName('observation_time_rfc822')[0].firstChild.nodeValue)
# print('temp')
# print(dom_tree.getElementsByTagName('temp_c')[0].firstChild.nodeValue)
# print('humidity')
# print(dom_tree.getElementsByTagName('relative_humidity')[0].firstChild.nodeValue)
# print('wind_string')
# print(dom_tree.getElementsByTagName('wind_string')[0].firstChild.nodeValue)
# print('wind_dir')
# print(dom_tree.getElementsByTagName('wind_dir')[0].firstChild.nodeValue)
# print('wind_degrees')
# print(dom_tree.getElementsByTagName('wind_degrees')[0].firstChild.nodeValue)
# print('wind_mph')
# print(dom_tree.getElementsByTagName('wind_mph')[0].firstChild.nodeValue)
# print('pressure_mb')
# print(dom_tree.getElementsByTagName('pressure_mb')[0].firstChild.nodeValue)
# print('dewpoint_c')
# print(dom_tree.getElementsByTagName('dewpoint_c')[0].firstChild.nodeValue)
# print('windchill_c')
# print(dom_tree.getElementsByTagName('windchill_c')[0].firstChild.nodeValue)
# print('visibility_mi')
# print(dom_tree.getElementsByTagName('visibility_mi')[0].firstChild.nodeValue)

import os
print(os.path.join(os.getcwd(),'App.cfg'))