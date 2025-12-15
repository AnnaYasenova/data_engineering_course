from lec02.job1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    sales_data = sales_api.get_sales(date)
    local_disk.save_to_disk(sales_data, raw_dir, date)

