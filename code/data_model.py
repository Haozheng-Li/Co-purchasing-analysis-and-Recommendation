import pymysql
import re
import os
import time
import requests
import shutil
import gzip
from tqdm import tqdm

if os.path.exists('define.py'):
    from define import *

DATA_PATH = '../data/amazon-meta.txt'
DATA_URL = 'http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz'
g_Model = None


def detect_data_file():
    """
    Could be very slow, suggest downloading from URL by browser, and then put the datafile into project root dir
    """
    if os.path.exists(DATA_PATH):
        return
    start_time = time.time()
    response = requests.get(DATA_URL, stream=True)
    chunk_size = 1024
    content_size = int(response.headers.get('content-length', 0))
    if response.status_code == 200:
        print('Start downloading, [File size]:{size:.2f} MB'.format(size=content_size / chunk_size / 1024))
        progress_bar = tqdm(total=content_size, unit='kB',
                            unit_scale=True, colour='green',)
        with open('data.txt.gz', 'wb') as file:
            for data in response.iter_content(chunk_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        if content_size != 0 and progress_bar.n != content_size:
            print("Download failed")
        else:
            end_time = time.time()
            print('Download completed! time: {.2f}'.format(end_time - start_time))
            with gzip.open('data.txt.gz', 'rb') as zip_data:
                with open('../data/amazon-meta.txt', 'wb') as saida:
                    shutil.copyfileobj(zip_data, saida)


def parse_data():
    f = open(DATA_PATH, 'r', encoding="utf8")
    file_lines = len(f.readlines())
    f.seek(0, 0)
    product_data = {
        'id': 0,
        'available': 0,
        'asin': '',
        'title': '',
        'product_group': '',
        'salesrank': 0,
        'similar_num': 0,
        'similar_items': '',
        'categories_items': '',
        'categories_num': 0,
        'reviews_total': 0,
        'reviews_downloaded': 0,
        'reviews_avg': 0,
    }
    categories_data = {}
    review_total = []
    categories_total = []
    product_total = []
    begin_tag = end_tag = False
    product_id = 0

    print("Begin to parse data")
    for line in tqdm(f.readlines(), total=file_lines):
        line = line.strip()
        colon_pos = line.find(':')

        """
        Separate by each different key
        """
        if line.startswith('Id'):
            begin_tag = True
            product_id = line[colon_pos+2:].strip()
            product_data['id'] = int(product_id)
            product_data['available'] = 1
        elif line == 'discontinued product':
            product_data['available'] = 0
        elif line.startswith('categories'):
            product_data['categories_num'] = int(line.split()[1])
        elif line.startswith('|'):
            # for product data
            categories_id_info = re.findall(r'[1-9]+\.?[0-9]*', line)
            categories_items = '|'.join(categories_id_info)
            product_data['categories_items'] = categories_items if not product_data['categories_items'] else\
                product_data['categories_items'] + '^' + categories_items
            # for categories data
            category_info = line.split('|')
            for each in category_info:
                if not each:
                    continue
                dividing_line_pos = each.find('[')
                category_name = each[:dividing_line_pos].strip()
                category_id_list = re.findall(r'[1-9]+\.?[0-9]*', each)
                if category_id_list:
                    categories_data[category_id_list[0]] = category_name
        elif line.startswith('similar'):
            product_data['similar_num'] = int(line.split()[1])
            product_data['similar_items'] = "|".join(str(i) for i in line.split()[2:])
        elif line.startswith('reviews'):
            info = line.replace('reviews: ', '').split()
            if len(info) == 7:
                product_data['reviews_total'] = int(info[1])
                product_data['reviews_downloaded'] = int(info[3])
                product_data['reviews_avg'] = float(info[6])
        elif line.find('cutomer') != -1:
            review_info = line.split()
            review_total.append(
                            {'product_id': product_id,
                             'review_date':  review_info[0],
                             'customer_id': review_info[2],
                             'rating': int(review_info[4]),
                             'votes': int(review_info[6]),
                             'helpful': int(review_info[8])})
        elif colon_pos != -1:
            key = line[:colon_pos]
            if key in ['ASIN', 'title', 'group', 'salesrank']:
                key = 'asin' if key == 'ASIN' else key
                key = 'product_group' if key == 'group' else key
                product_data[key] = line[colon_pos+2:].strip()
        elif not line and begin_tag:
            end_tag = True
        if begin_tag and end_tag:
            begin_tag = end_tag = False
            product_total.append(product_data)
            product_data = {
                'id': 0,
                'available': 0,
                'asin': '',
                'title': '',
                'product_group': '',
                'salesrank': 0,
                'similar_num': 0,
                'similar_items': '',
                'categories_items': '',
                'categories_num': 0,
                'reviews_total': 0,
                'reviews_downloaded': 0,
                'reviews_avg': 0,
            }
            product_id = 0
    for key, value in categories_data.items():
        categories_total.append({'category_id': key, 'category_name': value})
    return product_total, categories_total, review_total


def get_model():
    global g_Model
    if not g_Model:
        g_Model = Model()
    return g_Model


class Model:
    def __init__(self):
        self.m_db = None
        self.m_cursor = None
        self.init_connection()
        self.init_tables()

    def init_connection(self):
        """
        env: mysql 8.0, should input the mysql host, username, and password
        :return:
        """
        self.m_db = pymysql.connect(host=MYSQL_HOST,
                                    user=MYSQL_USERNAME,
                                    password=MYSQL_PASSWORD,
                                    database=MYSQL_DATABASE)
        self.m_cursor = self.m_db.cursor()

    def close_connection(self):
        self.m_cursor.close()
        self.m_db.close()

    def execute_sql(self, sql):
        """
        Just for single, easy sql
        When you need to insert plenty of data, use insert_by_list
        :param sql: str
        :return: None
        """
        try:
            sql = sql.strip()
            self.m_cursor.execute(sql)
            if sql.startswith('select') or sql.startswith('SELECT'):
                results = self.m_cursor.fetchall()
                return results
            else:
                self.m_db.commit()
        except Exception as ee:
            self.m_db.rollback()
            print("Error sql: ", sql)
            print("Mysql operation error, content: {}".format(ee))

    def insert_by_dict(self, table_name, datadict):
        """
        key name should be equal to table attribute name
        :param table_name: str
        :param datadict: dict
        :return:
        """
        columns = ", ".join('`{}`'.format(k) for k in datadict.keys())
        columns_value = ', '.join('%({})s'.format(k) for k in datadict.keys())

        sql = """INSERT INTO %s (%s) VALUES(%s)""" % (table_name, columns, columns_value)
        self.m_cursor.execute(sql, datadict)
        self.m_db.commit()

    def insert_by_list(self, table_name, datalist):
        """
        :param table_name:
        :param datalist:[{data1}, {data2}]
        :return:
        """
        onedata = datalist[0]
        columns = ", ".join('`{}`'.format(k) for k in onedata.keys())
        columns_value = ', '.join('%({})s'.format(k) for k in onedata.keys())
        sql = """INSERT INTO %s (%s) VALUES(%s)""" % (table_name, columns, columns_value)
        self.m_cursor.executemany(sql, datalist)
        self.m_db.commit()

    def get_tables_info(self):
        sql = """
        select table_name  from information_schema.tables
        where table_schema='csds435project';"""
        return self.execute_sql(sql)

    def init_tables(self):
        if len(self.get_tables_info()) == 4:
            return
        """
        Once the table has been created, it cannot be repeatedly executed to modify the structure of the table.
        And if you want to modify the table structure, you need to inform other team members
        :return: None
        """
        # categories_items Consider creating a new table
        # the purpose is to not affect the index when the amount of data is large
        # But now I couldn't find a good patterns, maybe later
        sql_create_product = """
            create table if not exists products (
                id bigint not null PRIMARY KEY,
                available tinyint(1),
                asin varchar(255),
                title varchar(2000),
                product_group varchar(255),
                salesrank bigint,
                similar_num int,
                similar_items varchar(255),
                categories_num int,
                categories_items varchar(5000),
                reviews_total int,
                reviews_downloaded int,
                reviews_avg double
                );
        """
        self.execute_sql(sql_create_product)

        sql_create_categories = """
            create table if not exists categories (
                category_id varchar(255),
                category_name varchar(255)
            );
        """
        self.execute_sql(sql_create_categories)

        """
        Referring to Ali's database specification, I did not choose foreign key constraints here.
        Because foreign key constraints are too troublesome to delete data and change data. 
        Foreign key constraints will be resolved at the application layer.
        """
        sql_create_reviews = """
             create table if not exists reviews (
                product_id bigint,
                customer_id varchar(255),
                rating int,
                votes int,
                helpful int,
                review_date DATE
            );
        """
        self.execute_sql(sql_create_reviews)

    def store_all_data(self):
        self.execute_sql('truncate table products;')
        self.execute_sql('truncate table reviews;')
        self.execute_sql('truncate table categories;')

        product_total, categories_total, review_total = parse_data()

        self.insert_by_list('products', product_total)
        self.insert_by_list('reviews', review_total)
        self.insert_by_list('categories', categories_total)

    def get_product_by_attribute(self, attribute_name, attribute_value):
        sql = """
        select * from products where %s = '%s';
        """ % (attribute_name, attribute_value)
        return self.execute_sql(sql)

    def get_all_product(self):
        """
        :return: all products info
        """
        sql = """
        select * from products;
        """
        return self.execute_sql(sql)


if __name__ == '__main__':
    detect_data_file()
    model = get_model()
    print(model.get_product_by_attribute('id', 'B00YZQZJQO'))
    print(model.get_all_product())
    model.close_connection()

