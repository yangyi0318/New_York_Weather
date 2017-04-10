import pymysql
import basic_functions

def create_accu_forecast(db):
    sql_query = '''
    create table if not exists new_york_weather.accu_forecast (
	sys_id int not null auto_increment
		primary key,
	post_code varchar(20) null,
	observation_time datetime null,
	temperature double null,
	feel_temperature double null,
	humidity double null,
	dewpoint double null,
	weather_text varchar(100) null,
	constraint post_code
		unique (post_code, observation_time)
    );
    '''
    db.cursor().execute(sql_query)

def create_aeris_forecast(db):
    sql_query = '''
    create table if not exists new_york_weather.aeris_forecast (
	sys_id int not null auto_increment
		primary key,
	post_code varchar(20) null,
	observation_time datetime null,
	temperature double null,
	feel_temperature double null,
	humidity double null,
	dewpoint double null,
	weather_text varchar(100) null,
	constraint post_code
		unique (post_code, observation_time)
    );
    '''
    db.cursor().execute(sql_query)

def create_apixu_forecast(db):
    sql_query = '''
    create table if not exists new_york_weather.apixu_forecast (
	sys_id int not null auto_increment
		primary key,
	post_code varchar(20) null,
	observation_time datetime null,
	temperature double null,
	feel_temperature double null,
	humidity double null,
	dewpoint double null,
	weather_text varchar(100) null,
	constraint post_code
		unique (post_code, observation_time)
    );
    '''
    db.cursor().execute(sql_query)

def create_darksky_forecast(db):
    sql_query = '''
    create table if not exists new_york_weather.darksky_forecast (
	sys_id int not null auto_increment
		primary key,
	post_code varchar(20) null,
	observation_time datetime null,
	temperature double null,
	feel_temperature double null,
	humidity double null,
	dewpoint double null,
	weather_text varchar(100) null,
	constraint post_code
		unique (post_code, observation_time)
    );
    '''
    db.cursor().execute(sql_query)

def create_worldweatheronline_forecast(db):
    sql_query = '''
    create table if not exists new_york_weather.worldweatheronline_forecast (
	sys_id int not null auto_increment
		primary key,
	post_code varchar(20) null,
	observation_time datetime null,
	temperature double null,
	feel_temperature double null,
	humidity double null,
	dewpoint double null,
	weather_text varchar(100) null,
	constraint post_code
		unique (post_code, observation_time)
    );
    '''
    db.cursor().execute(sql_query)

def create_wunder_forecast(db):
    sql_query = '''
    create table if not exists new_york_weather.wunder_forecast (
	sys_id int not null auto_increment
		primary key,
	post_code varchar(20) null,
	observation_time datetime null,
	temperature double null,
	feel_temperature double null,
	humidity double null,
	dewpoint double null,
	weather_text varchar(100) null,
	constraint post_code
		unique (post_code, observation_time)
    );
    '''
    db.cursor().execute(sql_query)
if __name__ == '__main__':
    db = pymysql.connect(basic_functions.Database_Config.get_host(),
                         basic_functions.Database_Config.get_username(),
                         basic_functions.Database_Config.get_password(),
                         basic_functions.Database_Config.get_database())
    create_accu_forecast(db)
    create_aeris_forecast(db)
    create_apixu_forecast(db)
    create_darksky_forecast(db)
    create_worldweatheronline_forecast(db)
    create_wunder_forecast(db)
    db.commit()

    db.close()