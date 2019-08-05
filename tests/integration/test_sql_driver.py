import os

import pytest

from toll_booth.obj.rds import SqlDriver


class TestSqlDriver:
    @pytest.mark.sql_driver_i
    def test_sql_driver(self):
        sql_host = 'algernon-1.cluster-cnd32dx4xing.us-east-1.rds.amazonaws.com'
        sql_db = 'algernon'
        internal_id = '223f05efd6a77bce5005d9070a25ea58'
        with SqlDriver.generate(sql_host, 3306, sql_db) as driver:
            results = driver.retrieve_documentation(internal_id)
            assert results
