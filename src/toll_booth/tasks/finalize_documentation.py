import os

import boto3
import rapidjson
from algernon import rebuild_event


def finalize_documentation(identifier, id_value, parser_results):
    parser_results = rebuild_event(rapidjson.loads(parser_results))
    session = boto3.session.Session()
    table = session.resource('dynamodb').Table(os.environ['PROGRESS_TABLE_NAME'])
    table.update_item(
        Key={'identifier': identifier, 'sid_value': str(id_value)},
        UpdateExpression='SET post_process = :p',
        ExpressionAttributeValues={
            ':p': parser_results
        }
    )
