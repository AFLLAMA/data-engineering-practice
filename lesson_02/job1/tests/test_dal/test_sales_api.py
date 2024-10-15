"""
Tests sales_api.py module.
"""

import os
from unittest import TestCase, mock

from lesson_02.job1.dal.sales_api import API_URL
from lesson_02.job1.dal.sales_api import get_sales


class GetSalesTestCase(TestCase):
    @mock.patch("requests.get")
    def test_get_sales(self, mock_get):
        json_content = [
            {
                "client": "Michael Wilkerson",
                "purchase_date": "2022-08-09",
                "product": "Vacuum cleaner",
                "price": 346,
            },
            {
                "client": "Russell Hill",
                "purchase_date": "2022-08-09",
                "product": "Microwave oven",
                "price": 446,
            },
        ]

        mock_response = mock.Mock()
        mock_response.ok = True
        mock_response.json.return_value = json_content

        mock_get.return_value = mock_response

        is_data, sales = get_sales(date="2022-08-09", page=1)

        mock_get.assert_called_once_with(
            url=API_URL,
            params={"date": "2022-08-09", "page": 1},
            headers={"Authorization": os.environ.get("AUTH_TOKEN")},
        )
        self.assertTrue(is_data)
        self.assertEqual(sales, json_content)
