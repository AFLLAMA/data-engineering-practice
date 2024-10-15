"""
Tests for main.py
"""

import json
from unittest import TestCase, mock
from lesson_02.job1 import main


class MainFunctionTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch("lesson_02.job1.main.save_sales_to_local_disk")
    def test_return_400_date_param_missed(self, get_sales_mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                # no 'date' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch("lesson_02.job1.main.save_sales_to_local_disk")
    def test_return_400_raw_dir_param_missed(self, empty_disk_mock: mock.MagicMock):
        empty_disk_mock.return_value = None
        resp = self.client.post(
            "/",
            json={
                "date": "2022-08-30",
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch("lesson_02.job1.main.save_sales_to_local_disk")
    def test_save_sales_to_local_disk(
        self, save_sales_to_local_disk_mock: mock.MagicMock
    ):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_date = "1970-01-01"
        fake_raw_dir = "/foo/bar/"
        self.client.post(
            "/",
            json={
                "date": fake_date,
                "raw_dir": fake_raw_dir,
            },
        )

        save_sales_to_local_disk_mock.assert_called_with(
            date=fake_date,
            raw_dir=fake_raw_dir,
        )

    @mock.patch("lesson_02.job1.main.save_sales_to_local_disk")
    def test_return_201_when_all_is_ok(self, save_sales_mock: mock.MagicMock):
        input_data = {
            "date": "2022-08-09",
            "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09",
        }

        # Make save_sales_to_local_disk do nothing
        save_sales_mock.return_value = None

        response = self.client.post(
            "/", data=json.dumps(input_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, 201)
