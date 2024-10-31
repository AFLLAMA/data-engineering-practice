"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

from flask import Flask, request
from flask import typing as flask_typing

from lesson_02.job2.bll.sales_api import save_sales_to_stg


app = Flask(__name__)


@app.route("/", methods=["POST"])
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

    stg_dir = input_data.get("stg_dir")
    raw_dir = input_data.get("raw_dir")

    if not stg_dir:
        return {
            "message": "stg_dir parameter missed",
        }, 400

    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400

    save_sales_to_stg(stg_dir=stg_dir, raw_dir=raw_dir)

    return {
        "message": "Data converted to avro successfully",
    }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
