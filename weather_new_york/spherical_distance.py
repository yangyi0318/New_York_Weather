from math import radians, cos, sin, asin, sqrt
import read_new_york_places
import basic_functions

def spherical_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6373 * c
    km = '%d' % km
    return float(km)

if __name__ == '__main__':

    location_keys = basic_functions

    places = read_new_york_places.read_places()
    with open('distance_matrix.txt','w') as f:
        for idx,place in enumerate(places):
            f.write(place)
            if idx != len(places)-1:
                f.write(',')
        f.write('\n')
        for place_main in places.values():
            print('main {} \n'.format(place_main.get_code()))
            lat1 = float(place_main.get_lat())
            lon1 = float(place_main.get_lon())
            for idx,place_sub in enumerate(places.values()):
                print('sub {} \n'.format(place_sub.get_code()))
                lat2 = float(place_sub.get_lat())
                lon2 = float(place_sub.get_lon())
                distance = spherical_distance(lat1,lon1,lat2,lon2)
                f.write(str(distance))
                print(str(distance))
                if idx != len(places) - 1:
                    f.write(',')
            f.write('\n')