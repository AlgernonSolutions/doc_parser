import os

import pytest
import rapidjson
from algernon import rebuild_event

from toll_booth.tasks.rds_documentation_text import rds_documentation_text


class TestPutDocumentation:
    @pytest.mark.put_documentation_i
    def test_put_documentation(self, test_documentation):
        os.environ['RDS_HOST'] = 'algernon-1.cluster-cnd32dx4xing.us-east-1.rds.amazonaws.com'
        os.environ['RDS_DB_NAME'] = 'algernon'
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-dev'
        os.environ['GRAPH_GQL_ENDPOINT'] = 'jlgmowxwofe33pdekndakyzx4i.appsync-api.us-east-1.amazonaws.com'
        documentation_text = rapidjson.loads(test_documentation['documentation_text'])
        documentation_text = rebuild_event(documentation_text)
        encounter = test_documentation['encounter']
        results = rds_documentation_text(encounter, documentation_text)
        assert results is None
