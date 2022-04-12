import os
import data_controller
from code.utils import log


@log
def get_local_data():
    if not os.path.exists(data_controller.DATA_PATH):
        data_controller.detect_data_file()
    product_total, categories_total, review_total = data_controller.parse_data()
    return product_total, categories_total, review_total


def get_all_reviews_info(limit=1000):
    model = data_controller.get_model()
    return model.get_all_reviews(limit)


def get_all_products_info(limit=1000):
    model = data_controller.get_model()
    return model.get_all_product(limit)


def get_product_by_attribute(attribute, value):
    """
    @param attribute: str; could be {id | available | asin | title | product_group | salesrank | similar_num
                                        | similar_items | categories_num | categories_items | reviews_total
                                        | reviews_downloaded | reviews_avg}
    @param value: str
    """
    model = data_controller.get_model()
    return model.get_product_by_attribute(attribute, value)


if __name__ == '__main__':
    # get_local_data()
    print(get_product_by_attribute('asin', '0871318237'))
    # print(get_all_products_info())
    # print(get_all_reviews_info())



