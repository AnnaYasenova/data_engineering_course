"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing

from bll.sales_api import save_sales_to_local_disk


AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "data: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json

    if not input_data:
        return {
            "message": "Invalid JSON body",
        }, 400

    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')

    if not date:
        return {
            "message": "date parameter missed",
        }, 400

    if not raw_dir:
        return {
            "message": "raw_dir parameter is required",
        }, 400

    try:
        save_sales_to_local_disk(date=date, raw_dir=raw_dir)
        return {
            "message": "Data retrieved successfully from API",
        }, 201
    except Exception as e:
        return {
            "message": f"Error retrieving data: {str(e)}",
        }, 500


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
