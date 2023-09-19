import datetime


class Sales_Distribution_Report:
    order_date = None
    seller_id = 0
    ciro = 0.00
    basket_count = 0
    item_count = 0
    distribution_range = 0
    dict_version = {}

    def __init__(self, row):
        str_order_date = (row[0].strftime("%d-%m-%Y")).split("-")
        self.order_date = datetime.date(int(str_order_date[2]), int(str_order_date[1]), int(str_order_date[0]))
        self.seller_id = row[1]
        self.ciro = row[2]
        self.basket_count = row[3]
        self.item_count = row[4]
        self.distribution_range = row[5]

    def get_dict_version(self):
        return self.dict_version
