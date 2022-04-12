import os
import model
from code.utils import log


@log
def get_local_data():
    if not os.path.exists(model.DATA_PATH):
        model.detect_data_file()
    product_total, categories_total, review_total = model.parse_data()
    return product_total, categories_total, review_total


if __name__ == '__main__':
    get_local_data()



