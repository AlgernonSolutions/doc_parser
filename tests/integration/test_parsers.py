import pytest
from toll_booth import handler
from toll_booth.obj.troubles import EmptyParserResponseException


@pytest.mark.parsers_i
class TestParsers:
    def test_parser(self, mock_context):
        encounter_internal_ids = ['67e449e989722799dbebde18124991e3', "cebdc99f8c8a80289c6f60d97e9f2de5"]
        for encounter_internal_id in encounter_internal_ids:
            event = {'encounter_internal_id': encounter_internal_id}
            try:
                results = handler(event, mock_context)
            except EmptyParserResponseException:
                continue
            assert results
