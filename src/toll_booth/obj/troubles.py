class EmptyParserResponseException(Exception):
    def __init__(self, parser_id, encounter_internal_id):
        msg = f'could not parse anything for encounter: {encounter_internal_id}, with parser: {parser_id}'
        self._msg = msg
