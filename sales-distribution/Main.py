from flask import Flask
from flask import request
from Postgresql_Connector import PostgresqlConnector
import Sales_Report_Processor

app = Flask(__name__)

connectorToDatabase = PostgresqlConnector()
connection = connectorToDatabase.create_connection()  # Create a connection


@app.route("/sales")
def all_data():
    # Example url: http://127.0.0.1:8080/sales
    query = "SELECT * FROM sales_distribution_report;"
    tuple_data = Sales_Report_Processor.read_data(connection, query)  # Read data and store it as tuple
    json_sales_list = tuple_data[0]  # Take all data from table

    return json_sales_list


@app.route("/sales/reports", methods=["GET"])
def get_reports_by_date():
    # Example url: http://127.0.0.1:8080/sales/reports?seller=100&year=2021&month=03&day=6
    seller_id = request.args.get('seller', type=int)
    order_year = request.args.get('year', type=int)
    order_month = request.args.get('month', type=int)
    order_day = request.args.get('day', type=int)

    return Sales_Report_Processor.get_seller_reports_by_date(connection, seller_id, order_year, order_month, order_day)


@app.route("/sales/reports/year", methods=["GET"])
def get_reports_by_year_interval():
    # Example url: http://127.0.0.1:8080/sales/reports/year?start=2019&end=2023
    start_year = request.args.get("start", type=int)
    end_year = request.args.get("end", type=int)

    return Sales_Report_Processor.get_reports_by_interval_of_year(connection, start_year, end_year)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
