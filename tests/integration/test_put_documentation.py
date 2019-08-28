import os

import pytest
import rapidjson
from algernon import rebuild_event

from toll_booth.tasks import es_documentation


class TestPutDocumentation:
    @pytest.mark.put_documentation_i
    def test_put_documentation(self, test_documentation):
        os.environ['ELASTIC_HOST_NAME'] = 'vpc-algernon-test-ankmhqkcdnx2izwfkwys67wmiq.us-east-1.es.amazonaws.com'
        os.environ['RDS_DB_NAME'] = 'algernon'
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-dev'
        os.environ['GRAPH_GQL_ENDPOINT'] = 'jlgmowxwofe33pdekndakyzx4i.appsync-api.us-east-1.amazonaws.com'
        os.environ['INDEX_TABLE_NAME'] = 'Indexes'
        documentation_text = rapidjson.loads(test_documentation['parse_kwargs']['documentation_text'])
        documentation_text = rebuild_event(documentation_text)
        encounter = test_documentation['parse_kwargs']['encounter']
        results = es_documentation(encounter, rapidjson.dumps(documentation_text))
        assert results is None
