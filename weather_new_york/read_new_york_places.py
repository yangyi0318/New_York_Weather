import xlrd
# from Weather_Row import Weather_Row

def read_places():
    workbook = xlrd.open_workbook('places.xlsx')
    sheet_names = workbook.sheet_names()
    sheet = workbook.sheet_by_name(sheet_names[0])

    places_dict = dict()
    for row_idx in range(sheet.nrows):
        if row_idx == 0:
            continue
        row_data = Weather_Row()
        for col_idx in range(sheet.ncols):
            cell = sheet.cell(row_idx,col_idx)
            if col_idx == 0:
                row_data.set_code(cell.value)
            elif col_idx == 1:
                row_data.set_name(cell.value)
            elif col_idx == 2:
                row_data.set_lat(str(cell.value))
            elif col_idx == 3:
                row_data.set_lon(str(cell.value))
            elif col_idx == 4:
                row_data.set_zip(str(int(cell.value)))
            elif col_idx == 5:
                row_data.set_gov_url(cell.value)
        places_dict[row_data.get_code()] = row_data
            # row_data.__add__(cell.value)
            # print(cell.value,end='\t')
        # print(row_data,end='\n')
        # print(row_data,end='\n')
    return places_dict

class Weather_Row:
    def __init__(self):
        self.code = ''
        self.name=''
        self.lat =0
        self.lon = 0
        self.zip = 0
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

