def get_filtered_data(connection, top_n):
    query = """
            SELECT 
                seller_id,
                product_id,
                daily_average_sale_quantity,
                daily_average_sale_price,
                critical_threshold,
                name,
                brand,
                category,
                product_size
            FROM result_table"""

    # If the top is 0 or None which means return all data by ascending order
    if top_n is 0 or top_n is None:
        query = query + """
            ORDER BY critical_threshold;"""

    # If the top is different from 0 or None, return the top 'n' output where critical threshold is under 30.
    else:
        query = query + """
            WHERE critical_threshold < 30.000
            ORDER BY critical_threshold 
            LIMIT {};""".format(top_n)

    return read_data_helper(connection, query)


# Helper function to store the cells in a list by dict.
def read_data_helper(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]

    data_list = []
    for row in cursor.fetchall():
        sale_info = dict(zip(column_names, row))
        data_list.append(sale_info)

    connection.commit()
    cursor.close()

    return data_list
