import os

import pytest
from toll_booth import handler
from toll_booth.obj.troubles import EmptyParserResponseException


@pytest.mark.tasks_i
class TestParsers:
    def test_task(self, task_event, mock_context):
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-dev'
        results = handler(task_event, mock_context)
        assert results
