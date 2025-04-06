import logging
import re
from typing import Any, Generator
from urllib.parse import parse_qs, urlencode, urlparse

import parsel
import parsel.selector
import scrapy
from scrapy.http import Response

SAMPLING_PAGINATION = 10

logger = logging.getLogger(__file__)


class CoconalaSpider(scrapy.Spider):
    """
    Coconalaのサービス情報をスクレイピングするスパイダー
    """

    name = "coconala"
    allowed_domains = ["coconala.com"]
    start_urls = [
        "https://coconala.com/categories/231?ref=category_popular_subcategories",
        "https://coconala.com/categories/230?ref=category_popular_subcategories",
        "https://coconala.com/categories/239?ref=category_popular_subcategories",
        "https://coconala.com/categories/243?ref=category_popular_subcategories",
    ]

    def parse(self, response: Response) -> Generator[Any, None, None]:
        """
        サービスリストのページからデータを抽出し、次のページがあれば遷移してスクレイピングを続ける

        Args:
            response: レスポンスオブジェクト

        Yields:
            dict: サービスのタイトル、価格、ユーザーレベル画像の情報
        """
        logger.info(f"Processing URL: {response.url}")

        if response.css(".c-searchNoHits"):
            logger.info("No hits found on this page, skipping next page.")
            return

        for service in response.css(".c-serviceListItemRow"):
            yield {
                "title": self.extract_title(service),
                "price": self.extract_price(service),
                "user_level_image": self.extract_user_level_image(service),
                "user_name": self.extract_user_name(service),
                "sales_count": self.extract_sales_count(service),
            }

        next_page_url = self.construct_next_page_url(response.url)
        yield response.follow(next_page_url, self.parse)

    def construct_next_page_url(self, url: str) -> str:
        """
        次のページのURLを構成する

        Args:
            url: 現在のページのURL

        Returns:
            str: 次のページのURL
        """

        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        current_page = int(query_params.get("page", [1])[0])
        next_page_number = current_page + SAMPLING_PAGINATION
        query_params["page"] = [str(next_page_number)]
        next_page_url = parsed_url._replace(query=urlencode(query_params, doseq=True)).geturl()

        return next_page_url

    def extract_title(self, service: parsel.selector.Selector) -> str|None:
        """
        サービスのタイトルを抽出する

        Args:
            service: サービスのHTML要素

        Returns:
            str: サービスのタイトル
        """
        try:
            text_or_none = service.css(".c-serviceListItemColContentHeader_overview::text").get()
            if text_or_none is None:
                return None
            return text_or_none.strip()
        except Exception as e:
            logger.error(f"Error extracting title: {e}")
            return "Unknown Title"  # Default value

    def extract_price(self, service: parsel.selector.Selector) -> float:
        """
        サービスの価格を抽出する

        Args:
            service: サービスのHTML要素

        Returns:
            float: サービスの価格
        """
        try:
            price = service.css(".c-serviceListItemColContentFooterPrice_price strong::text").get()
            if price is None:
                return 0.0
            return float(price.replace(",", "").strip()) if price else 0.0
        except Exception as e:
            logger.error(f"Error extracting price: {e}")
            return 0.0  # Default value

    def extract_user_level_image(self, service: parsel.selector.Selector) -> int|None:
        """
        サービスのユーザーレベル画像のURLから、画像の番号を抽出する

        Args:
            service: サービスのHTML要素

        Returns:
            int: ユーザーレベル画像の番号（例：icon_2.svg から 2 を抽出）
        """
        try:
            img_url = service.css(".c-serviceListItemColContentFooterInfoUser_level img::attr(src)").get()
            if img_url:
                match = re.search(r"icon_(\d+)\.svg", img_url)
                if match:
                    return int(match.group(1))
            return None
        except Exception as e:
            logger.error(f"Error extracting user level image: {e}")
            return -1  # Default value

    def extract_user_name(self, service: parsel.selector.Selector) -> str|None:
        """
        サービスのユーザー名を抽出する

        Args:
            service: サービスのHTML要素

        Returns:
            str: ユーザー名
        """
        try:
            text_or_none = service.css(".c-serviceListItemColContentFooterInfoUser_name span::text").get()
            if text_or_none is None:
                return None
            return text_or_none.strip()
        except Exception as e:
            logger.error(f"Error extracting user name: {e}")
            return "Unknown User"  # Default value

    def extract_sales_count(self, service: parsel.selector.Selector) -> int:
        """
        サービスの販売実績を抽出する

        Args:
            service: サービスのHTML要素

        Returns:
            int: 販売実績（数値）
        """
        try:
            sales_text = service.css(".c-serviceListItemColContentFooterPriceRating_count::text").get()
            if sales_text:
                match = re.search(r"\((\d+)\)", sales_text)
                if match:
                    return int(match.group(1))
            return 0  # デフォルト値
        except Exception as e:
            logger.error(f"Error extracting sales count: {e}")
            return 0  # デフォルト値
