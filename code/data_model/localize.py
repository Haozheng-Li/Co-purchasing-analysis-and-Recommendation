import code.data_model.preprocessing as preprocessing
import pandas as pd
import csv
import os


def localize_list_data(data, file_name):
    """
    Data format: [{'key': 'value'}, {'key': 'value'}]
    """

    if not data or type(data[0]) is not dict:
        return
    print('Localizing data...')

    with open("../../data/{}.csv".format(file_name), "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data[0].keys())
        for index, each_dict in enumerate(data):
            writer.writerow(list(each_dict.values()))


def read_local_data(file_name):
    """
    Read data from local file
    """
    data = pd.read_csv(os.getcwd()+"/../../data/{}.csv".format(file_name), encoding='utf-8')
    return data


if __name__ == '__main__':
    products, categories, reviews = preprocessing.parse_data(100000)
    localize_list_data(products, 'products')
    localize_list_data(categories, 'categories')
    localize_list_data(reviews, 'reviews')
    print(read_local_data('products'))


