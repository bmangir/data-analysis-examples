from flask import Flask
from flask import request
from SQLconnector import SQL_connector
import DataProcessor

app = Flask(__name__)

connector = SQL_connector()
connection = connector.create_connection()


# Return the data by ascending order of critical threshold.
@app.route("/data")
def index():
    data = DataProcessor.take_join_tables_data(connection, top=None)

    return data


# Return the top data by critical threshold under 30.
@app.route("/data/", methods=["GET"])
def top_pagination():
    top = request.args.get("top", type=int, default=1)
    data = DataProcessor.take_join_tables_data(connection, top)

    return data


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

connection.close()
