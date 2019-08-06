import logging
import os
from decimal import Decimal

import boto3
import rapidjson
from algernon import rebuild_event
from boto3.dynamodb.conditions import Key

from toll_booth.obj.rds import SqlDriver, DocumentationTextEntry


def _build_sql_driver():
    rds_host = os.environ['RDS_HOST']
    rds_db_name = os.environ['RDS_DB_NAME']
    rds_port = os.environ.get('RDS_PORT', '3306')
    driver = SqlDriver.generate(rds_host, int(rds_port), rds_db_name)
    return driver


def _retrieve_internal_id(identifier_stem, id_value, table_resource):
    sid_value = str(int(id_value))
    response = table_resource.query(
        KeyConditionExpression=Key('identifier_stem').eq(identifier_stem) & Key('sid_value').eq(sid_value),
        ProjectionExpression='internal_id'
    )
    for entry in response['Items']:
        return entry['internal_id']


def _resolve_internal_ids(identifier_stem, provider_id_value, patient_id_value):
    table_name = os.environ['INDEX_TABLE_NAME']
    session = boto3.session.Session()
    table = session.resource('dynamodb').Table(table_name)
    provider_stem = identifier_stem.replace('Encounter', 'Provider')
    patient_stem = identifier_stem.replace('Encounter', 'Patient')
    provider_internal_id = _retrieve_internal_id(provider_stem, provider_id_value, table)
    patient_internal_id = _retrieve_internal_id(patient_stem, patient_id_value, table)
    return provider_internal_id, patient_internal_id


def _find_encounter_property(property_name, encounter_properties):
    for encounter_property in encounter_properties:
        if property_name == encounter_property['property_name']:
            return encounter_property['property_value']['property_value']
    raise KeyError(property_name)


def rds_documentation(encounter, documentation_text, sql_driver=None):
    if not sql_driver:
        sql_driver = _build_sql_driver()
    documentation_text = rebuild_event(rapidjson.loads(documentation_text))
    logging.debug(f'after rebuilding, documentation_text is {documentation_text}')
    encounter_properties = encounter['vertex_properties']['local_properties']
    patient_id_value = _find_encounter_property('patient_id', encounter_properties)
    provider_id_value = _find_encounter_property('provider_id', encounter_properties)
    identifier_stem = encounter['identifier_stem']['property_value']
    logging.debug(f'going to resolve the provider and patient internal_id values')
    provider_internal_id, patient_internal_id = _resolve_internal_ids(identifier_stem, provider_id_value, patient_id_value)
    logging.debug(f'resolved values are provider: {provider_internal_id}, patient: {patient_internal_id}')
    entry_kwargs = {
        'encounter_internal_id': encounter['internal_id'],
        'encounter_type': _find_encounter_property('encounter_type', encounter_properties),
        'id_source': _find_encounter_property('id_source', encounter_properties),
        'documentation_text': documentation_text['extracted_data']['source']['documentation_text'],
        'provider_internal_id': provider_internal_id,
        'patient_internal_id': patient_internal_id,
        'patient_id_value': patient_id_value,
        'provider_id_value': provider_id_value,
        'encounter_id_value': int(encounter['id_value']['property_value'])
    }
    text_entry = DocumentationTextEntry(**entry_kwargs)
    with sql_driver as driver:
        logging.debug(f'going to push the created documentation entry: {entry_kwargs} to the database')
        driver.put_documentation(text_entry)
    logging.debug(f'successfully pushed the documentation to the database')
