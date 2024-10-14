import os
from lesson_02.job1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    print("\tI'm in save_sales_to_local_disk function!")
    is_data = True
    page = 1
    local_disk.empty_disk(path=raw_dir)
    print("Cleared disk")
    while is_data:
        # 1. get data from the API
        is_data, sales = sales_api.get_sales(date=date, page=page)
        # 2. save data to disk
        if is_data:
            local_disk.save_to_disk(
                json_content=sales, path=os.path.join(raw_dir, f"{date}_{page}.json")
            )
            print(f"Save sales to local disk for {date=}; {page=}")
        page += 1
    print("\tLeaving save_sales_to_local_disk!")
