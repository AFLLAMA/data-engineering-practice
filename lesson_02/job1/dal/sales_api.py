from typing import List, Dict, Any

import os
import requests

API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"


def get_sales(date: str, page: bool) -> tuple[int, List[Dict[str, Any]]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    response = requests.get(
        url=API_URL,
        params={"date": date, "page": page},
        headers={"Authorization": os.environ.get("AUTH_TOKEN")},
    )
    is_data = True if response.ok else False
    return is_data, response.json()
