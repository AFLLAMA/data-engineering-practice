"""
Tests for main.py
"""

import json
from unittest import TestCase, mock
from lesson_02.job2 import main


class MainFunctionTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch("lesson_02.job2.main.save_sales_to_stg")
    def test_return_400_stg_dir_param_missed(
        self, save_sales_to_stg_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when no 'stg_dir' param
        """
        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                # no 'stg_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch("lesson_02.job2.main.save_sales_to_stg")
    def test_return_400_raw_dir_param_missed(
        self, save_sales_to_stg_mock: mock.MagicMock
    ):
        resp = self.client.post(
            "/",
            json={
                "stg_dir": "/foo/bar/",
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch("lesson_02.job2.main.save_sales_to_stg")
    def test_save_sales_to_stg(self, save_sales_to_stg_mock: mock.MagicMock):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_stg_dir = "/foo/bar/"
        fake_raw_dir = "/foo/bar/"
        self.client.post(
            "/",
            json={
                "stg_dir": fake_stg_dir,
                "raw_dir": fake_raw_dir,
            },
        )

        save_sales_to_stg_mock.assert_called_with(
            stg_dir=fake_stg_dir,
            raw_dir=fake_raw_dir,
        )

    @mock.patch("lesson_02.job2.main.save_sales_to_stg")
    def test_return_201_when_all_is_ok(self, save_sales_mock: mock.MagicMock):
        input_data = {
            "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09",
            "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09",
        }

        # Make save_sales_to_stg do nothing
        save_sales_mock.return_value = None

        response = self.client.post(
            "/", data=json.dumps(input_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, 201)
