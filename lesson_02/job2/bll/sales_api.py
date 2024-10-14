import os
from lesson_02.job2.dal import local_disk


def save_sales_to_stg(stg_dir: str, raw_dir: str) -> None:
    print("\tI'm in save_sales_to_local_disk function!")
    local_disk.empty_disk(path=stg_dir)
    print("Cleared disk")
    for path, _, fnames in os.walk(raw_dir):
        for fname in fnames:
            print(path, " ", fname)
            file_path = os.path.join(path, fname)
            # 1. get data from disk
            sales = local_disk.get_from_disk(file_path=file_path)
            # 2. schemas
            avro_path = os.path.join(stg_dir, fname.replace(".json", ".avro"))
            schema = {
                "type": "record",
                "name": avro_path,
                "fields": [
                    {"name": "client", "type": "string"},
                    {
                        "name": "purchase_date",
                        "type": {"type": "int", "logicalType": "date"},
                    },
                    {"name": "product", "type": "string"},
                    {"name": "price", "type": "int"},
                ],
            }
            # 3. save data to disk
            local_disk.save_avro_to_disk(
                json_content=sales, path=avro_path, avro_schema=schema
            )
            print("Save sales to stg.")
    print("\tLeaving save_sales_to_local_disk!")
