from pathlib import Path
import subprocess

from prefect import task, flow
import pandas as pd

from postprocess.categorize import mask_user_name, classify_title


@task
def check_file_exists(file_path: str | Path) -> bool:
    if isinstance(file_path, str):
        file_path = Path(file_path)
    return file_path.exists()


@task
def crawl(spider_name: str="coconala") -> None:
    result = subprocess.run(["scrapy", "crawl", spider_name])
    if result.returncode != 0:
        raise RuntimeError(f"Spider failed with exit code {result.returncode}")


@flow
def workflow(
    crawl_result: str="output.json",
    classified_result: str="with-category.json"
):
    if not check_file_exists(crawl_result):
        crawl()

    if not check_file_exists(classified_result):
        data = pd.read_json(crawl_result, orient="records", lines=True)
        data = mask_user_name(data)
        data = classify_title(data)
        data.to_json(classified_result, orient="records", lines=True, force_ascii=False)

if __name__ == "__main__":
    workflow()
