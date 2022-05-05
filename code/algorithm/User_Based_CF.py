# -*- Coding UTF-8 -*-
# @Time: 2022/5/2 20:31
# @Author: Yiyang Bian
# @File: User_Based.py

import math
from tqdm import tqdm
import re
import os
import time
import requests
import shutil
import gzip
from tqdm import tqdm
from operator import itemgetter

DATA_PATH = '../../data/amazon-meta.txt'
DATA_URL = 'http://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz'


def parse_data(limit=None):
    f = open(DATA_PATH, 'r', encoding="utf8")
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
            product_id = line[colon_pos + 2:].strip()
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
            product_data['categories_items'] = categories_items if not product_data['categories_items'] else \
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
                 'review_date': review_info[0],
                 'customer_id': review_info[2],
                 'rating': int(review_info[4]),
                 'votes': int(review_info[6]),
                 'helpful': int(review_info[8])})
        elif colon_pos != -1:
            key = line[:colon_pos]
            if key in ['ASIN', 'title', 'group', 'salesrank']:
                value = line[colon_pos + 2:].strip()
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

    # Remove similar products that do not exist in the product data
    for each_product in tqdm(product_total):
        new_similar_items = ''
        all_similar_items = each_product['similar_items'].split('|')
        for each_item in all_similar_items:
            if each_item not in all_asin:
                new_similar_items += each_item + '|'
        each_product['similar_items'] = new_similar_items
        each_product['similar_num'] = len(new_similar_items.split('|'))
    return product_total, categories_total, review_total


_, _, reviews = parse_data(100000)

rating = {}
for each_review in reviews:
    if each_review['customer_id'] in rating:
        rating[each_review['customer_id']].append(each_review['product_id'])
    else:
        rating[each_review['customer_id']] = [each_review['product_id']]


# 计算用户兴趣相似度
def Usersim(dicc):
    # 把用户-商品字典转成商品-用户字典（如图中箭头指示那样）
    item_user = dict()
    for u, items in dicc.items():
        for i in items:  # 文中的例子是不带评分的，所以用的是元组而不是嵌套字典。
            if i not in item_user.keys():
                item_user[i] = set()  # i键所对应的值是一个集合（不重复）。
            item_user[i].add(u)  # 向集合中添加用户。

    C = dict()  # 感觉用数组更好一些，真实数据集是数字编号，但这里是字符，这边还用字典。
    N = dict()
    for item, users in item_user.items():
        for u in users:
            if u not in N.keys():
                N[u] = 0  # 书中没有这一步，但是字典没有初始值不可以直接相加吧
            N[u] += 1  # 每个商品下用户出现一次就加一次，就是计算每个用户一共购买的商品个数。
            # 但是这个值也可以从最开始的用户表中获得。
            # 比如： for u in dic.keys():
            #             N[u]=len(dic[u])
            for v in users:
                if u == v:
                    continue
                if (u, v) not in C.keys():  # 同上，没有初始值不能+=
                    C[u, v] = 0
                C[u, v] += 1  # 这里我不清楚书中是不是用的嵌套字典，感觉有点迷糊。所以我这样用的字典。
    # 到这里倒排阵就建立好了，下面是计算相似度。
    W = dict()
    for co_user, cuv in C.items():
        if cuv / math.sqrt(N[co_user[0]] * N[co_user[1]]) != 1:
            W[co_user] = cuv / math.sqrt(N[co_user[0]] * N[co_user[1]])
    return W


def Recommend(user, dicc, W2, K):
    rvi = 1  # 这里都是1,实际中可能每个用户就不一样了。就像每个人都喜欢beautiful girl,但有的喜欢可爱的多一些，有的喜欢御姐多一些。
    rank = dict()
    related_user = []
    interacted_items = dicc[user]
    for co_user, item in W2.items():
        if co_user[0] == user:
            related_user.append((co_user[1], item))  # 先建立一个和待推荐用户兴趣相关的所有的用户列表。
    for v, wuv in sorted(related_user, key=itemgetter(1), reverse=True)[0:K]:
        # 找到K个相关用户以及对应兴趣相似度，按兴趣相似度从大到小排列。itemgetter要导包。
        for i in dicc[v]:
            if i in interacted_items:
                continue  # 书中少了continue这一步吧？
            if i not in rank.keys():  # 如果不写要报错，是不是有更好的方法？
                rank[i] = 0
            rank[i] += wuv * rvi
    return rank


if __name__ == '__main__':
    # print(rating)
    W = Usersim(rating)
    # print(W)
    Last_Rank = Recommend('A393PYR83LT7R8', rating, W, 2)
    print(Last_Rank)
