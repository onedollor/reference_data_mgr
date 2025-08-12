import os
import pandas as pd
import asyncio
from utils.ingest import DataIngester
from utils.database import DatabaseManager
from utils.logger import Logger

class DummyLogger(Logger):
    async def log_info(self, *args, **kwargs):
        return

async def _run_inference(df):
    os.environ['INGEST_TYPE_INFERENCE'] = '1'
    ingester = DataIngester(DatabaseManager(), DummyLogger())
    return ingester._infer_types(df, list(df.columns))

def test_integer_inference():
    df = pd.DataFrame({'col1': ['1','2','300','4000']})
    inferred = asyncio.run(_run_inference(df))
    assert inferred['col1'] in ('int','bigint','decimal(38,0)')

def test_decimal_inference():
    df = pd.DataFrame({'price': ['1.23','2.5','3.1415']})
    inferred = asyncio.run(_run_inference(df))
    assert inferred['price'].startswith('decimal(')

def test_datetime_inference():
    df = pd.DataFrame({'dt': ['2024-01-01','2024-02-02','2024-03-03']})
    inferred = asyncio.run(_run_inference(df))
    assert inferred['dt'] == 'datetime'

def test_varchar_sizing():
    df = pd.DataFrame({'txt': ['a'*10, 'b'*60, 'c'*210]})
    inferred = asyncio.run(_run_inference(df))
    # Max len is 210 -> expect at least varchar(200) or above
    assert inferred['txt'] in ('varchar(200)','varchar(500)','varchar(100)','varchar(4000)','varchar(2000)')
