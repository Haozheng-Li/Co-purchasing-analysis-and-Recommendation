import code.data_model.localize as localize


def get_data_by_type(data_type='products', is_local=True):
	"""
	@param data_type: str [products, categories, reviews]
	@param is_local: bool
	"""
	if is_local:
		return localize.read_local_data(data_type)