"""
Tests dal.local_disk.py module
"""

import json
import os
import shutil
import tempfile
from unittest import TestCase

import fastavro
from lesson_02.job2.dal.local_disk import save_avro_to_disk, empty_disk, get_from_disk


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

    def test_save_avro_to_disk(self):
        file_path = os.path.join(self.test_dir, "data", "sales.avro")
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
        avro_schema = {
            "type": "record",
            "name": "Sales",
            "fields": [
                {"name": "client", "type": "string"},
                {"name": "purchase_date", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "price", "type": "int"},
            ],
        }
        save_avro_to_disk(json_content, file_path, avro_schema)
        self.assertTrue(os.path.exists(file_path))
        with open(file_path, "rb") as avro_file:
            reader = fastavro.reader(avro_file)
            saved_content = [record for record in reader]

        self.assertEqual(saved_content, json_content)

    def test_get_from_disk(self):
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
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(json_content, json_file)
        loaded_content = get_from_disk(file_path)

        self.assertEqual(loaded_content, json_content)
