import json
import os
import shutil
from typing import List, Dict, Any

import fastavro


def empty_disk(path: str) -> None:
    # check if folder exists
    if os.path.exists(path):
        # remove if exists
        shutil.rmtree(path)
        # recreate
        os.mkdir(path)


def save_avro_to_disk(json_content: List[Dict[str, Any]], path: str, avro_schema) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as fp:
        fastavro.writer(fp, avro_schema, json_content)


def get_from_disk(file_path: str) -> List[Dict[str, Any]]:
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data
