import os
import json
import pandas as pd
import tempfile
from utils.csv_detector import CSVFormatDetector
from utils.ingest import DataIngester
from utils.database import DatabaseManager
from utils.logger import Logger

class DummyLogger(Logger):
    async def log_info(self, *args, **kwargs):
        pass
    async def log_error(self, *args, **kwargs):
        pass

class DummyDB(DatabaseManager):
    def __init__(self):
        # Do not call real DB init
        self.data_schema = 'ref'
        self.backup_schema = 'bkp'
        self.validation_sp_schema = 'ref'
    def get_connection(self):
        class DummyConn:
            def cursor(self_inner):
                class C:
                    def execute(self_cur, *a, **k):
                        return None
                    def fetchone(self_cur):
                        return [0]
                    @property
                    def rowcount(self_cur):
                        return 0
                return C()
            def close(self_inner):
                pass
            def commit(self_inner):
                pass
            def rollback(self_inner):
                pass
        return DummyConn()
    def ensure_schemas_exist(self, c):
        return
    def table_exists(self, c, t, schema=None):
        return False
    def create_table(self, *a, **k):
        return
    def create_backup_table(self, *a, **k):
        return
    def create_validation_procedure(self, *a, **k):
        return
    def execute_validation_procedure(self, *a, **k):
        return {"validation_result":0, "validation_issue_list": []}


def test_trailer_detection_and_pattern():
    csv_content = """id,value\n1,10\n2,20\nTOTAL: 2\n"""
    with tempfile.NamedTemporaryFile('w', delete=False, suffix='.csv') as f:
        f.write(csv_content)
        path = f.name
    detector = CSVFormatDetector()
    result = detector.detect_format(path)
    os.unlink(path)
    assert result['has_trailer'] is True
    assert result.get('trailer_pattern') is not None


def test_numeric_type_downgrade():
    # Simulate ingestion logic for type inference then downgrade
    db = DummyDB()
    logger = DummyLogger(db, db) if isinstance(db, DatabaseManager) else None
    ingester = DataIngester(db, logger)
    os.environ['INGEST_TYPE_INFERENCE'] = '1'
    df = pd.DataFrame({
        'num_col': ['1','2','TRAILER'],
        'other': ['a','b','c']
    })
    sample_df = df.head(2)  # Will infer int for num_col
    inferred = ingester._infer_types(sample_df, ['num_col','other'])
    assert inferred['num_col'] in ('int','bigint','decimal(38,0)')
    # Simulate validation adjustment logic
    # Copy of loop condition
    series_full = df['num_col'].astype(str)
    pattern = r'^[+-]?\d+$'
    mask_non_empty = (series_full.str.strip() != '') & (~series_full.str.strip().isin(['None','none','NULL','null','NaN','nan']))
    invalid_mask = mask_non_empty & (~series_full.str.match(pattern))
    assert invalid_mask.any()  # 'TRAILER'

