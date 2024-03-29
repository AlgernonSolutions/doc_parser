import json
import os
from datetime import datetime
from decimal import Decimal

import boto3
import rapidjson
from algernon import rebuild_event


class FireHoseEncoder(json.JSONEncoder):
    @classmethod
    def default(cls, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super(FireHoseEncoder, cls()).default(obj)


def _send_to_fire_hose(stream_name, records):
    client = boto3.client('firehose')
    batch = [{'Data': rapidjson.dumps(x, default=FireHoseEncoder.default).encode()} for x in records]
    response = client.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=batch
    )
    if response['FailedPutCount']:
        failed_records = [x for x in response['RequestResponses'] if 'ErrorCode' in x]
        raise RuntimeError(f'could not fire_hose these records: {failed_records}')


def fire_hose_documentation(**kwargs):
    extracted_data = kwargs['extracted_data']
    stream_name = os.environ['FIRE_HOSE_NAME']
    if isinstance(extracted_data, str):
        extracted_data = rapidjson.loads(extracted_data)
    extracted_data = rebuild_event(extracted_data)
    _send_to_fire_hose(stream_name, [extracted_data])
