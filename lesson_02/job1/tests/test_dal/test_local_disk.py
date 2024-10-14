"""
Tests dal.local_disk.py module
# TODO: write tests
"""

import json
import os
import shutil
import tempfile
from unittest import TestCase

from lesson_02.job1.dal.local_disk import save_to_disk, empty_disk


class SaveToDiskTestCase(TestCase):
    """
    Test dal.local_disk.save_to_disk function.
    """

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_empty_disk(self):
        os.mkdir(os.path.join(self.test_dir, "subdir"))
        with open(os.path.join(self.test_dir, "subdir", "test.txt"), "w") as f:
            f.write("test")

        self.assertTrue(os.path.exists(os.path.join(self.test_dir, "subdir")))
        self.assertTrue(
            os.path.exists(os.path.join(self.test_dir, "subdir", "test.txt"))
        )
        empty_disk(self.test_dir)
        self.assertTrue(os.path.exists(self.test_dir))
        self.assertFalse(
            os.path.exists(os.path.join(self.test_dir, "subdir", "test.txt"))
        )

    def test_save_to_disk(self):
        file_path = os.path.join(self.test_dir, "data", "sales.json")

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
        save_to_disk(json_content, file_path)

        self.assertTrue(os.path.exists(file_path))
        with open(file_path, "r") as f:
            saved_content = json.load(f)
        self.assertEqual(saved_content, json_content)
