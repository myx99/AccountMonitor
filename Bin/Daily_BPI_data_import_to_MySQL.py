



m = GlobalConfig()
product_list_tmp = m.getConfig('product', 'list')
product_list = product_list_tmp.split(", ")
for p in product_list:
    singleImport(p)