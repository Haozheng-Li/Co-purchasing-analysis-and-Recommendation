import localize


def get_data(data_type):
    """
    data_type:['categories','products', 'reviews']
    """
    return localize.read_local_data(data_type)


if __name__ == '__main__':
    print(get_data('products'))
