import json
from date_time_util import convert_to_serializable
from flask import Flask
from postgresql_connector import Connector
from sellers_repository import SellersRepository

app = Flask(__name__)

connector = Connector()
connection = connector.create_connection()

repository = SellersRepository(connection)


# Example URL: http://127.0.0.1:8080/sellers/id=6
@app.route("/sellers/id=<seller_id>")
def get_info_of_a_seller(seller_id):
    data = repository.read_seller_info(seller_id)
    return json.dumps(data, default=convert_to_serializable, indent=4)


# Example URL: http://127.0.0.1:8080/sellers/successful/date=2023-12-4
@app.route("/sellers/successful/date=<string:date>")
def get_successful_sellers(date):
    data = repository.read_successful_sellers(date)
    return json.dumps(data, default=convert_to_serializable, indent=4)


# Example URL: http://127.0.0.1:8080/sellers/successful/all
@app.route("/sellers/successful/all")
def get_all_successful_sellers():
    data = repository.read_all_successful_sellers()
    return json.dumps(data, default=convert_to_serializable, indent=4)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
