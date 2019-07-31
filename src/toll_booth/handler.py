import logging
import os

import boto3
from algernon.aws import lambda_logged

from toll_booth.tasks.parse_documentation import parse_documentation


def _load_config(variable_names):
    client = boto3.client('ssm')
    response = client.get_parameters(Names=[x for x in variable_names])
    results = [(x['Name'], x['Value']) for x in response['Parameters']]
    for entry in results:
        os.environ[entry[0]] = entry[1]


@lambda_logged
def handler(event, context):
    logging.info(f'received a call to run a documentation_parser: {event}/{context}')
    variable_names = ['GRAPH_GQL_ENDPOINT']
    _load_config(variable_names)
    encounter_internal_id = event['encounter_internal_id']
    results = parse_documentation(encounter_internal_id)
    logging.info(f'completed a call to the documentation_parser: {event}/{results}')
    return results
