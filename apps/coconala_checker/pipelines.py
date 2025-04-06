# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pandas as pd
from scrapy.spiders import Spider


class AppsPipeline:
    """
    スクレイピングで収集したアイテムを処理し、最終的にJSONファイルとして保存するパイプライン
    """

    def __init__(self) -> None:
        """
        パイプラインの初期化

        Attributes:
            items_buffer (list): アイテムを格納するバッファ
        """
        self.items_buffer: list = []  # Buffer to store items

    def process_item(self, item: dict, spider: Spider) -> dict:
        """
        アイテムをバッファに追加する

        Args:
            item (dict): アイテム
            spider (Spider): スパイダーオブジェクト

        Returns:
            dict: 処理されたアイテム
        """
        self.items_buffer.append(item)  # Add item to the buffer
        return item

    def close_spider(self, spider: Spider) -> None:
        """
        スパイダーが終了する際に呼ばれ、アイテムをDataFrameに変換してJSONとして保存する

        Args:
            spider (Spider): スパイダーオブジェクト
        """
        # Convert the items in the buffer to a pandas DataFrame
        df = pd.DataFrame(self.items_buffer)

        # Save the DataFrame to a JSON file
        df.to_json("output.json", orient="records", force_ascii=False, lines=True, mode="a")  # Appends to the file
