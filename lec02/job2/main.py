"""
Job 2: Convert JSON files from raw directory to Avro format in stg directory
"""
from flask import Flask, request
from flask import typing as flask_typing
from bll.sales_converter import convert_json_to_avro

app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and trigger business logic layer
    Proposed POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09",
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json

    if not input_data:
        return {
            "message": "Invalid JSON body",
        }, 400

    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not raw_dir:
        return {
            "message": "raw_dir parameter is required",
        }, 400

    if not stg_dir:
        return {
            "message": "stg_dir parameter is required",
        }, 400

    try:
        convert_json_to_avro(raw_dir=raw_dir, stg_dir=stg_dir)
        return {
            "message": "Data converted successfully to Avro format",
        }, 201
    except Exception as e:
        return {
            "message": f"Error converting data: {str(e)}",
        }, 500


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)