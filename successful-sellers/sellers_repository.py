from date_time_util import datetime_to_epoch_converter


class SellersRepository:

    # Take all information of one seller
    def __init__(self, postgres_connection):
        self.postgres_connection = postgres_connection

    def read_seller_info(self, seller_id):
        query = """SELECT * FROM seller_success WHERE seller_id = {}""".format(seller_id)
        return self.__read_helper(query)

    # Take successful sellers' information by date
    def read_successful_sellers(self, date_as_str):
        if date_as_str is None:
            raise Exception("Date must be provided!")

        epoch_time = datetime_to_epoch_converter(date_as_str)
        query = """
                SELECT 
                    * 
                FROM seller_success 
                WHERE 
                    is_successful = True AND 
                    processed_date = {}""".format(epoch_time)

        return self.__read_helper(query)

    # Take all information of successful sellers
    def read_all_successful_sellers(self):
        query = """SELECT * FROM seller_success WHERE is_successful = True;"""
        return self.__read_helper(query)

    # Helper function to take the data of query result
    def __read_helper(self, query):
        cursor = self.postgres_connection.cursor()
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]

        data = []
        for record in cursor.fetchall():
            # record = id, now-created_date, total_sold_product, total_visit,
            #           average_product_review, average_seller_score, is_successful, processed_date
            seller_success_info = dict(zip(column_names, record))
            data.append(seller_success_info)

        self.postgres_connection.commit()
        cursor.close()

        return data
