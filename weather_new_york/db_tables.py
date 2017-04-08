import pymysql
import basic_functions

def create_accu_forecast(db):
    sql_query = '''
    create table new_york_weather.accu_forecast if not exists(
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
    create_accu_forecast()
    db.commit()

    db.close()