from flask import Flask
from flask import request
from SQLconnector import SQLConnector
import DataProcessor

app = Flask(__name__)

connector = SQLConnector()
connection = connector.create_connection()


# Return the all data by ascending order of critical threshold.
@app.route("/data")
def index():
    # Example URL: http://127.0.0.1:8080/data
    data = DataProcessor.get_filtered_data(connection, top_n=None)

    return data


# Return the top 'n' data by critical threshold under 30.
@app.route("/data/", methods=["GET"])
def top_pagination():
    # Example URL: http://127.0.0.1:8080/data/?top=3
    top = request.args.get("top", type=int, default=1)
    data = DataProcessor.get_filtered_data(connection, top)

    return data


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

connection.close()
