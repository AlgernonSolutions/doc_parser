import pytest
from toll_booth.tasks import parsers


@pytest.mark.parsers
class TestParsers:
    def test_parser(self, documentation):
        parser_name = 'dcdbh'
        parser = getattr(parsers, parser_name)
        results = parser(documentation)
        assert results
