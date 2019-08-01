import os
from datetime import datetime

from algernon import ajson
from algernon.aws import Bullhorn

from toll_booth.obj.troubles import EmptyParserResponseException
from toll_booth.tasks.aws_tasks import s3_tasks
from toll_booth.obj.gql.gql_client import GqlClient
from toll_booth.tasks import parsers
from toll_booth.tasks.retrieve_documentation import retrieve_documentation

documentation_map = {
    'PSI': 'dcdbh',
    'ICFS': 'dcdbh'
}


def _publish_documentation_node(encounter_id, parser_id, id_source, bullhorn):
    task_name = 'leech'
    leech_arn = bullhorn.find_task_arn(task_name)
    generation_utc_datetime = datetime.utcnow()
    documentation_node = {
        'encounter_id': encounter_id,
        'id_source': id_source,
        'parser_id': parser_id,
        'utc_generated_datetime': generation_utc_datetime.isoformat()
    }
    message = {
        'task_name': task_name,
        'task_kwargs': {
            'object_type': 'Documentation',
            'extracted_data': {'source': documentation_node}
        }
    }
    strung_event = ajson.dumps(message)
    bullhorn.publish('new_event', leech_arn, strung_event)


def _publish_documentation_field_node(encounter_id, id_source, parser_id, field_name, field_documentation, bullhorn):
    task_name = 'leech'
    leech_arn = bullhorn.find_task_arn(task_name)
    documentation_field_node = {
        'encounter_id': encounter_id,
        'id_source': id_source,
        'parser_id': parser_id,
        'field_name': field_name,
        'field_documentation': field_documentation
    }
    message = {
        'task_name': task_name,
        'task_kwargs': {
            'object_type': 'DocumentationField',
            'extracted_data': {'source': documentation_field_node}
        }
    }
    strung_event = ajson.dumps(message)
    bullhorn.publish('new_event', leech_arn, strung_event)


def _publish_results(encounter_id, parser_id, id_source, parser_results):
    bullhorn = Bullhorn.retrieve(profile=os.getenv('AWS_PROFILE'))
    publish_kwargs = {
        'encounter_id': encounter_id,
        'id_source': id_source,
        'parser_id': parser_id,
        'bullhorn': bullhorn
    }
    _publish_documentation_node(**publish_kwargs)
    for field_name, field_documentation in parser_results.items():
        publish_kwargs.update({
            'field_name': field_name,
            'field_documentation': field_documentation
        })
        _publish_documentation_field_node(**publish_kwargs)


def _organize_documentation_node(encounter_id, id_source, parser_id):
    generation_utc_datetime = datetime.utcnow()
    documentation_node = {
        'encounter_id': encounter_id,
        'id_source': id_source,
        'parser_id': parser_id,
        'utc_generated_datetime': generation_utc_datetime.isoformat()
    }
    documentation_payload = {
        'task_name': 'leech',
        'task_kwargs': {
            'object_type': 'Documentation',
            'extracted_data': {'source': documentation_node}
        }
    }
    return documentation_payload


def _organize_documentation_field_node(encounter_id, id_source, parser_id, field_name, field_documentation):
    documentation_field_node = {
        'encounter_id': encounter_id,
        'id_source': id_source,
        'parser_id': parser_id,
        'field_name': field_name,
        'field_documentation': field_documentation
    }
    payload = {
        'task_name': 'leech',
        'task_kwargs': {
            'object_type': 'DocumentationEntry',
            'extracted_data': {'source': documentation_field_node}
        }
    }
    return payload


def _organize_results(encounter_id, id_source, parser_id, parser_results):
    results = {
        'documentation': _organize_documentation_node(encounter_id, id_source, parser_id)
    }
    documentation_fields = []
    for field_name, field_documentation in parser_results.items():
        field_args = (encounter_id, id_source, parser_id, field_name, field_documentation)
        documentation_fields.append(_organize_documentation_field_node(*field_args))
    results['documentation_fields'] = documentation_fields
    more = len(documentation_fields) > 0
    results['iterator'] = {'count': len(documentation_fields), 'index': 0, 'more': more}
    return results


def parse_documentation(encounter_internal_id):
    documentation_data = retrieve_documentation(encounter_internal_id)
    id_source = documentation_data['id_source']
    documentation = documentation_data['documentation']
    encounter_id = documentation_data['encounter_id']
    parser_name = id_source
    if id_source in documentation_map:
        parser_name = documentation_map[id_source]
    parser = getattr(parsers, parser_name)
    parser_results, parser_id = parser(documentation)
    if not parser_results:
        raise EmptyParserResponseException(parser_id, encounter_internal_id)
    organized_results = _organize_results(encounter_id, id_source, parser_id, parser_results)
    return organized_results
