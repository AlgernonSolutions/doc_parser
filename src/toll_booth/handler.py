import logging
import os

import boto3
from algernon.aws import lambda_logged

from toll_booth import tasks


def _load_config(variable_names):
    client = boto3.client('ssm')
    response = client.get_parameters(Names=[x for x in variable_names])
    results = [(x['Name'], x['Value']) for x in response['Parameters']]
    for entry in results:
        os.environ[entry[0]] = entry[1]


@lambda_logged
def handler(event, context):
    logging.info(f'received a call to run a parser: {event}/{context}')
    variable_names = ['GRAPH_GQL_ENDPOINT']
    _load_config(variable_names)
    parse_type = event['parse_type']
    parse_kwargs = event['parse_kwargs']
    operation = getattr(tasks, f'{parse_type}_documentation')
    results = operation(**parse_kwargs)
    logging.info(f'completed a call to the parser: {event}/{results}')
    return results
