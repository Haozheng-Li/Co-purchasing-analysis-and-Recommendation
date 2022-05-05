import re
import os
import time
import requests
import shutil
import gzip
from tqdm import tqdm


DATA_PATH = '../../data/amazon-meta.txt'
DATA_URL = 'http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz'


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
                with open('../../data/amazon-meta.txt', 'wb') as saida:
                    shutil.copyfileobj(zip_data, saida)


def parse_data(limit=None):
    f = open(DATA_PATH, 'r', encoding="utf8")
    product_data = {
        'id': 0,
        'available': 0,
        'asin': '',
        'title': '',
        'product_group': '',
        'salesrank': 0,
        'similar_items': '',
        'categories_items': '',
        'reviews_total': 0,
        'reviews_downloaded': 0,
        'reviews_avg': 0,
    }
    categories_data = {}
    review_total = []
    categories_total = []
    product_total = []
    all_asin = []
    begin_tag = end_tag = False
    product_id = 0

    print("Begin to parse data")
    for index, line in enumerate(tqdm(f.readlines()[:limit])):
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
                value = line[colon_pos+2:].strip()
                key = 'product_group' if key == 'group' else key
                if key == 'ASIN':
                    key = 'asin'
                    all_asin.append(value)
                product_data[key] = value
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

    # Remove similar products that do not exist in the product data
    for each_product in tqdm(product_total):
        new_similar_items = ''
        all_similar_items = each_product['similar_items'].split('|')
        for each_item in all_similar_items:
            if each_item not in all_asin:
                new_similar_items += each_item + '|'
        each_product['similar_items'] = new_similar_items
    return product_total, categories_total, review_total


if __name__ == '__main__':
    print(parse_data(1000))

