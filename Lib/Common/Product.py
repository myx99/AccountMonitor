import Lib.Common.GlobalConfig as cf


class Product(object):
    def __init__(self):
        self.productID = ''
        self.StartDate = ''
        self.EndDate = ''

    def getProduct(self, product_number):
        config = cf.GlobalConfig()
        self.ProductID = config.getConfig(product_number, 'ID')
        self.StartDate = config.getConfig(product_number, 'Start_date')
        self.EndDate = config.getConfig(product_number, 'End_date')
        return self.ProductID, self.StartDate, self.EndDate


if __name__ == '__main__':
    p = Product()
    q = p.getProduct('product1')
    print(q[0], q[1], q[2])



