import json
import os
import shutil
from typing import List, Dict, Any


def empty_disk(path: str) -> None:
    # check if folder exists
    if os.path.exists(path):
        # remove if exists
        shutil.rmtree(path)
        # recreate
        os.mkdir(path)


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as fp:
        json.dump(json_content, fp)
