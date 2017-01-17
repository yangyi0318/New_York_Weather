class Weather_Row:
    def __init__(self):
        self.code = ''
        self.name=''
        self.lat =0
        self.lon = 0
        self.zip = ''
        self.gov_url = ''

    def set_code(self,code):
        self.code = code

    def get_code(self):
        return self.code

    def set_name(self,name):
        self.name = name

    def get_name(self):
        return self.name

    def set_lat(self,lat):
        self.lat = lat

    def get_lat(self):
        return self.lat

    def set_lon(self,lon):
        self.lon = lon

    def get_lon(self):
        return self.lon

    def set_gov_url(self,gov_url):
        self.gov_url = gov_url

    def get_gov_url(self):
        return self.gov_url

    def set_zip(self,zip):
        self.zip = zip

    def get_zip(self):
        return self.zip

    def __str__(self):
        return '({},{},{},{},{},{})'.format(self.code,self.name,self.lat,self.lon,self.zip,self.gov_url)
        # return '('+self.code+','+self.name+','+self.lat+','+self.lon+','+self.zip+','+self.gov_url+')'

