from pathlib import Path
import os

from git import Repo
from invoke import task


def get_git_root(path: str|None=None) -> Path:
    repo = Repo(path or os.getcwd(), search_parent_directories=True)
    return Path(repo.git.rev_parse("--show-toplevel"))


@task
def format(ctx):
    """コードをフォーマットする"""
    ctx.run("ruff format apps")  # ruffでコードを自動フォーマットする


@task
def lint(ctx):
    """コードをリントする"""
    ctx.run("ruff check apps --fix")  # ruffでコードのリントを実行


@task
def type_check(ctx):
    """型チェックを実行する"""
    ctx.run("mypy apps")  # mypyで型チェックを実行


@task(pre=[format, lint, type_check])
def all_checks(ctx):
    """フォーマット、リント、型チェックを一括で実行"""
    print("All checks completed.")


@task()
def main(ctx):
    """メインのタスクを実行"""
    git_root = get_git_root()
    app_path = git_root / "apps"
    ctx.run(f"cd {app_path} && python workflow.py")
