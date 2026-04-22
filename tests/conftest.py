"""tests/conftest.py — shared pytest fixtures."""
import pytest
import polars as pl
from pathlib import Path


@pytest.fixture
def orders_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id":    [1, 2, 3, 9999],
        "customer_id": [10, 20, 9999, 30],
        "updated_at":  ["2024-01-15", "2024-02-20", "2024-03-10", "2024-04-01"],
    })


@pytest.fixture
def customers_df() -> pl.DataFrame:
    return pl.DataFrame({
        "customer_id": [10, 20, 30],
        "full_name":   ["Alice Smith", "Bob Jones", "Carol White"],
        "email":       ["  ALICE@EXAMPLE.COM  ", "bob@test.org", "carol@example.com"],
    })


@pytest.fixture
def products_df() -> pl.DataFrame:
    return pl.DataFrame({
        "product_id":   [100, 200, 300],
        "product_name": ["Widget A", "Widget B", "Widget C"],
        "price":        [9.99, 24.99, 4.99],
    })


@pytest.fixture
def order_lines_df() -> pl.DataFrame:
    return pl.DataFrame({
        "line_id":    [1, 2, 3, 4],
        "order_id":   [1, 1, 2, 2],
        "product_id": [100, 200, 100, 300],
        "qty":        [2, 1, 3, 5],
    })


@pytest.fixture
def silver_dir(tmp_path, orders_df, customers_df, products_df, order_lines_df) -> Path:
    d = tmp_path / "silver"
    d.mkdir()
    orders_df.write_parquet(d / "orders.parquet")
    customers_df.write_parquet(d / "customers.parquet")
    products_df.write_parquet(d / "products.parquet")
    order_lines_df.write_parquet(d / "order_lines.parquet")
    return d


@pytest.fixture
def gold_dir(tmp_path) -> Path:
    d = tmp_path / "gold"
    d.mkdir()
    return d


@pytest.fixture
def export_dir(tmp_path) -> Path:
    d = tmp_path / "export"
    d.mkdir()
    return d


@pytest.fixture
def base_cfg(tmp_path) -> dict:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold   = tmp_path / "gold"
    export = tmp_path / "export"
    for d in (bronze, silver, gold, export):
        d.mkdir()
    return {
        "pipeline": {"name": "test_project"},
        "paths": {
            "bronze": str(bronze),
            "silver": str(silver),
            "gold":   str(gold),
            "export": str(export),
        },
        "bronze_to_silver": {"tables": [], "derived_tables": []},
        "silver_to_gold": {"projects": []},
        "bi_export": {"enabled": True, "projects": []},
    }
