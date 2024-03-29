import os

import pytest
from toll_booth import handler
from toll_booth.obj.troubles import EmptyParserResponseException


@pytest.mark.parsers_i
class TestParsers:
    def test_parser(self, mock_context):
        os.environ['ALGERNON_BUCKET_NAME'] = 'algernonsolutions-leech-dev'
        encounter_internal_ids = [
            '4b234ef92e04789c211e0e43eea3cd45', '01e63570d0b834b35f757f9ed5a0ae7f',
            '67e449e989722799dbebde18124991e3', "cebdc99f8c8a80289c6f60d97e9f2de5"]
        for encounter_internal_id in encounter_internal_ids:
            event = {'parse_type': 'distill', 'parse_kwargs': {'encounter_internal_id': encounter_internal_id}}
            try:
                results = handler(event, mock_context)
            except EmptyParserResponseException:
                continue
            assert results
