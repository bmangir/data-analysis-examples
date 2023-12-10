# Merge the tables, products and daily_sales_info.
# Find the critical threshold and return the data by ascending order.
# If the top is 0 or None which means return all data.
# If the top is different from 0 or None, return the top 'n' output where critical threshold is under 30.
def take_join_tables_data(connection, top):
    query = "SELECT\n"\
                "\tdsi.seller_id,\n"\
                "\tdsi.product_id,\n"\
                "\tdsi.daily_average_sale_quantity,\n"\
                "\tdsi.daily_average_sale_price,\n"\
                "\tROUND((p.stock / dsi.daily_average_sale_quantity), 3) as critical_threshold,\n"\
                "\tp.name,\n" \
                "\tp.brand,\n" \
                "\tp.category,\n" \
                "\tp.product_size\n"\
                "FROM daily_sales_info dsi\n"\
                "JOIN products p ON dsi.product_id = p.id\n"

    if top is 0 or top is None:
        query = query + " ORDER BY critical_threshold;"
    else:
        query = query + "WHERE ROUND((p.stock / dsi.daily_average_sale_quantity), 3) < 30.000\n" \
                        "ORDER BY critical_threshold\n" + \
                        "LIMIT " + str(top) + ";"

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
