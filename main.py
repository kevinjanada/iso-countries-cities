import json
import psycopg2
import pandas as pd

'''
CREATE TABLE countries (
    code char(2) PRIMARY KEY,
    name varchar(255)
);

CREATE TABLE cities (
    code char(3),
    region_code char(3),
    country_code char(2),
    name varchar(255),
    FOREIGN KEY (country_code)
    REFERENCES countries (code)
);

CREATE TABLE regions (
    code char(3) PRIMARY KEY,
    country_code char(2),
    name varchar(255),
    FOREIGN KEY (country_code)
    REFERENCES countries (code)
);
'''

DB_HOST = "localhost"
DB_NAME = "iso-countries"
DB_USER = "kevin"
DB_PASSWORD = ""

def connect_to_db():
    conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
            )
    return conn


def load_countries() -> tuple:
    f = open('./iso-3166-2.json')
    data = json.load(f)
    country_list = []
    for code in data:
        name = data[code]["name"]
        country_list.append((code, name))
    return country_list


def insert_countries(db_conn, country_list):
    # country_list = [('code', 'name'), ('code', 'name')]
    try:
        sql = "INSERT INTO countries(code, name) VALUES(%s, %s)"
        cur = db_conn.cursor()
        cur.executemany(sql, country_list)
        db_conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def load_regions() -> tuple:
    f = open('./iso-3166-2.json')
    data = json.load(f)
    region_list = []
    for country_code in data:
        divisions = data[country_code]["divisions"]
        for div in divisions:
            region_code = div.partition("-")[2]
            region_name = divisions[div]
            region_list.append((region_code, country_code, region_name))
    return region_list



CITY_PART1 = "./cities/2020-2 UNLOCODE CodeListPart1.csv"
CITY_PART2 = "./cities/2020-2 UNLOCODE CodeListPart2.csv"
CITY_PART3 = "./cities/2020-2 UNLOCODE CodeListPart3.csv"

def load_cities(DIR) -> tuple:
    data = pd.read_csv(DIR)
    country_code_city_code_city_name = data.iloc[:, [1,2,3]]
    country_code_city_code_city_name = country_code_city_code_city_name.dropna()
    cities = []
    for index, row in country_code_city_code_city_name.iterrows():
        country_code = row[0]
        city_code = row[1]
        city_name = row[2]
        '''
        if country_code == None or city_code == None or city_name == None:
            print(country_code, city_code, city_name)
        '''
        cities.append((city_code, country_code, city_name))
    return cities


def insert_cities(db_conn, city_list):
    for city in city_list:
        sql = "INSERT INTO cities(code, country_code, name) VALUES(%s, %s, %s)"
        cur = db_conn.cursor()
        try:
            cur.execute(sql, city)
            db_conn.commit()
        except psycopg2.Error as error:
            print(error.pgerror)
            print(error.pgcode)
            print('error in city: ', city)
            db_conn.rollback()
            continue
        cur.close()





if __name__ == '__main__':
    # Connect to DB
    conn = connect_to_db()

    # Load countries
    #countries = load_countries()
    #insert countries
    #insert_countries(conn, countries)

    # Load regions
    regions = load_regions()
    print(regions)

    '''
    cities_part1 = load_cities(CITY_PART1)
    cities_part2 = load_cities(CITY_PART2)
    cities_part3 = load_cities(CITY_PART3)
    insert_cities(conn, cities_part1)
    insert_cities(conn, cities_part2)
    insert_cities(conn, cities_part3)
    '''

    conn.close()
