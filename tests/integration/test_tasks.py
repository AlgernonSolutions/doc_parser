import os

import pytest
from toll_booth import handler
from toll_booth.obj.rds import ApiDriver
from toll_booth.obj.troubles import EmptyParserResponseException


@pytest.mark.tasks_i
class TestParsers:
    def test_task(self, task_event, mock_context):
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-prod'
        os.environ['RDS_HOST'] = 'algernon-1.cluster-cnd32dx4xing.us-east-1.rds.amazonaws.com'
        os.environ['RDS_DB_NAME'] = 'algernon'
        os.environ['INDEX_TABLE_NAME'] = 'Indexes'
        os.environ['RDS_SECRET_ARN'] = 'arn:aws:secretsmanager:us-east-1:322652498512:secret:rds-xudEmz'
        os.environ['RDS_CLUSTER_ARN'] = 'arn:aws:rds:us-east-1:322652498512:cluster:algernon'
        results = handler(task_event, mock_context)
        assert results
