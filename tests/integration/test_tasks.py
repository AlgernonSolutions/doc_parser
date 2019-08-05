import os

import pytest
from toll_booth import handler
from toll_booth.obj.troubles import EmptyParserResponseException


@pytest.mark.tasks_i
class TestParsers:
    def test_task(self, task_event, mock_context):
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-dev'
        os.environ['RDS_HOST'] = 'algernon-1.cluster-cnd32dx4xing.us-east-1.rds.amazonaws.com'
        os.environ['RDS_DB_NAME'] = 'algernon'
        results = handler(task_event, mock_context)
        assert results
