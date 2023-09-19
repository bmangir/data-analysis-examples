from datetime import date
from decimal import Decimal
from Sales_Distribution_Report import Sales_Distribution_Report


# Convert the features of sale into dict for json format
def convert_dict(sale):
    dict_version = {}
    dict_version.update({"distribution_range": sale.distribution_range})
    dict_version.update({"seller_id": sale.seller_id})
    dict_version.update({"total_basket_count": sale.basket_count})
    dict_version.update({"total_ciro": sale.ciro})
    dict_version.update({"total_item_count": sale.item_count})
    dict_version.update({"order_date": sale.order_date})
    sale.dict_version = dict_version
    return sale


# Read data from sales_report table in postgresql and store it in a dict
# Return tuple, first one is list of data as an array store dicts, second one is list of sales
# (Sales' information might be used later)
def read_data(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)  # Execute the sql command and take the data

    json_list = []
    sales_list = []
    for row in cursor.fetchall():
        sale = Sales_Distribution_Report(row)  # Create sales object and fill the variable
        sale = convert_dict(sale)
        sales_list.append(sale)  # Add them into this list
        json_list.append(sale.get_dict_version())

    connection.commit()
    cursor.close()
    return json_list, sales_list


# Return seller's report by requests
def get_seller_reports_by_date(connection, seller_id, year, month, day):
    if seller_id is None:
        return []

    query = "SELECT * FROM sales_distribution_report WHERE seller_id = " + str(seller_id)  # It is for if year is None

    if year is not None:
        if month is None:
            query = query + " AND EXTRACT(YEAR FROM order_date) = " + str(year) + ";"
        else:
            if day is None:
                query = query + " AND EXTRACT(YEAR FROM order_date) = " + str(year) + \
                                " AND EXTRACT(MONTH FROM order_date) = " + str(month) + ";"
            else:
                query = query + " AND order_date = '" + str(year) + "-" + str(month) + "-" + str(day) + "';"

    return read_reports_helper(connection, query)


# Return reports for all seller by interval of year
def get_reports_by_interval_of_year(connection, start, end):
    if start is None and end is None:
        return []

    query = "SELECT * FROM sales_distribution_report WHERE "
    if start is None and end is not None:
        query = query + "EXTRACT(YEAR FROM order_date) <= " + str(end) + ";"

    elif start is not None and end is None:
        query = query + "EXTRACT(YEAR FROM order_date) >= " + str(start) + ";"

    elif start is not None and end is not None:
        query = query + "EXTRACT(YEAR FROM order_date) >= " + str(start) + \
                       " AND EXTRACT(YEAR FROM order_date) <= " + str(end) + ";"

    return read_reports_helper(connection, query)


# The helper function to take the data from the table by query
def read_reports_helper(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]

    reports_list = []
    for row in cursor.fetchall():
        basket_data = dict(zip(column_names, row))
        reports_list.append(basket_data)

    connection.commit()
    cursor.close()

    return reports_list
