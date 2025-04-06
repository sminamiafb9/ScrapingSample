import hashlib
import re
from textwrap import dedent

import langchain
import pandas as pd
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool
from langchain_openai import OpenAI
from prefect import task
from tqdm import tqdm

langchain.debug = True


@task
def mask_user_name(df: pd.DataFrame, input_col: str = "user_name", n: int = 10) -> pd.DataFrame:
    def _fn(name: str) -> str:
        return hashlib.sha256(name.encode("utf-8")).hexdigest()[:n]

    df[input_col] = df[input_col].apply(_fn)
    return df


@task
def classify_title(df: pd.DataFrame, input_col: str = "title", output_col: str = "category") -> pd.DataFrame:
    classifier = Classifier()
    tqdm.pandas(desc="Processing")
    df[output_col] = df[input_col].progress_apply(classifier.classify)
    return df


class Classifier:
    def __init__(self):
        llm = OpenAI(
            model="gemma-3-4b-it",
            base_url="http://host.docker.internal:1234/v1",
            api_key="sk-xxxxx",  # type: ignore
            stop=["[END]"],  # type: ignore
        )

        extract_prompt = PromptTemplate.from_template(
            dedent("""
                # TASK
                文章からカテゴリを抽出してください。カテゴリは言語名や成果物、使用技術、タスク名とします。

                # INPUT
                {input}

                # OUTPUT FORMAT
                以下のカンマ区切り形式で1行で出力してください。
                最初に[BEG]を出力してください。
                最後に[END]を出力してください。
                [BEG]カテゴリ1, カテゴリ2, ...[END]

                # LIMITATION
                カテゴリは日本名を用いてください。
                カテゴリは一般的な粒度としてください。
                ニッチなカテゴリや詳細なカテゴリは一般的な粒度にまとめてください。
                指定のフォーマットを厳密に守ってください。指定外の情報は出力しません。

                # EXAMPLES
                INPUT: Excelのツールを作成します
                OUTPUT: [BEG]excel, ツール開発[END]

                INPUT: GASのご相談に乗ります!
                OUTPUT: [BEG]Google App Script, 相談, サポート[END]

                # OUTPUT
            """)
        )

        @tool
        def _extract_csv(text: str) -> str:
            """コードブロックや空行・改行を削除する。

            Args:
                text (str): 入力文字列（コードブロック付きのcsvが含まれる可能性あり）

            Returns:
                str: カンマ区切りのCSV文字列
            """
            try:
                text = text.split("[BEG]")[1]
                text = text.split("[END]")[0]
                cleaned = re.sub(r"^```(?:csv)?\s*|\s*```$", "", text.strip(), flags=re.IGNORECASE)
                return cleaned.lower()
            except Exception as _:
                return ""

        self.chain = extract_prompt | llm | _extract_csv

    def classify(self, s: str) -> str:
        return self.chain.invoke(s)
