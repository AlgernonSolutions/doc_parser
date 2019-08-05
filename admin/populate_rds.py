import json
import os
from datetime import datetime
from decimal import Decimal
from queue import Queue
from threading import Thread

import boto3
import rapidjson
from algernon.serializers import ExplosionJson

from toll_booth.obj.rds import SqlDriver
from toll_booth.tasks import rds_documentation_text


class FireHoseEncoder(json.JSONEncoder):
    @classmethod
    def default(cls, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super(FireHoseEncoder, cls()).default(obj)


_local_fields = (
    'provider_id', 'patient_id', 'id_source', 'encounter_type', 'encounter_datetime_in', 'encounter_datetime_out')


def _query_encounter(identifier_stem, sid_value):
    resource = boto3.resource('dynamodb')
    table = resource.Table('Indexes')
    response = table.get_item(Key={'identifier_stem': identifier_stem, 'sid_value': sid_value})
    return response['Item']


def _scan_dynamo(table_name, segment, total_segments, queue):
    session = boto3.session.Session()
    client = session.client('dynamodb')
    event_client = session.client('events')
    paginator = client.get_paginator('scan')
    response_iterator = paginator.paginate(
        TableName=table_name,
        FilterExpression='begins_with(identifier_stem, :i)',
        ExpressionAttributeValues={
            ':i': {'S': "#vertex#Encounter#"}
        },
        Segment=segment,
        TotalSegments=total_segments
    )
    for page in response_iterator:
        for page_item in page['Items']:
            item = ExplosionJson.loads(rapidjson.dumps(page_item))
            local_properties = []
            stored_properties = []
            for key, value in item.items():
                if key in _local_fields:
                    local_properties.append(value)
                if key == 'documentation':
                    stored_properties.append(value)
            encounter = {
                    'internal_id': item['internal_id'],
                    'id_value': item['id_value']['property_value'],
                    'identifier_stem': {
                        'property_value': item['identifier_stem'],
                        'property_name': 'identifier_stem',
                        'data_type': 'S'
                    },
                    'vertex_properties': {
                        'local_properties': local_properties,
                        'stored_properties': stored_properties
                    }
            }
            entry = {
                'Source': 'algernon',
                'DetailType': 'vertex_added',
                'Detail': rapidjson.dumps(encounter, default=FireHoseEncoder.default),
                'Resources': []
            }
            event_client.put_events(Entries=[entry])
            print(item)


def _put_documentation_text(sql_host, queue):
    driver = SqlDriver.generate(sql_host, 3306, 'algernon')
    while True:
        task = queue.get()
        if task is None:
            return
        rds_documentation_text(**task, sql_driver=driver)
        queue.task_done()
        print(queue.qsize())


def _start_scanners(table_name, queue):
    workers = []
    for _ in range(5):
        worker_kwargs = {
            'table_name': table_name,
            'segment': _,
            'total_segments': 5,
            'queue': queue
        }
        worker = Thread(target=_scan_dynamo, kwargs=worker_kwargs)
        worker.start()
        workers.append(worker)
    return workers


def _start_putters(sql_host, queue):
    workers = []
    for _ in range(4):
        worker_kwargs = {'sql_host': sql_host, 'queue': queue}
        worker = Thread(target=_put_documentation_text, kwargs=worker_kwargs)
        worker.start()
        workers.append(worker)
    return workers


if __name__ == '__main__':
    os.environ['GRAPH_GQL_ENDPOINT'] = 'jlgmowxwofe33pdekndakyzx4i.appsync-api.us-east-1.amazonaws.com'
    test_table_name = 'Indexes'
    test_sql_host = 'algernon-1.cluster-cnd32dx4xing.us-east-1.rds.amazonaws.com'
    item_queue = Queue()
    scanners = _start_scanners(test_table_name, item_queue)
    # putters = _start_putters(test_sql_host, item_queue)
    for scanner in scanners:
        scanner.join()
