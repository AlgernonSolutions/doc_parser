import json
import os

from toll_booth.obj.gql.gql_client import GqlClient
from toll_booth.obj.rds import SqlDriver, DocumentationTextEntry


def _build_sql_driver():
    rds_host = os.environ['RDS_HOST']
    rds_db_name = os.environ['RDS_DB_NAME']
    rds_port = os.environ.get('RDS_PORT', '3306')
    driver = SqlDriver.generate(rds_host, int(rds_port), rds_db_name)
    return driver


def _resolve_internal_ids(identifier_stem, provider_id_value, patient_id_value):
    gql_endpoint = os.environ['GRAPH_GQL_ENDPOINT']
    client = GqlClient.from_gql_endpoint(gql_endpoint)
    identifier_stem = identifier_stem.replace('"', '\\\"')
    provider_stem = identifier_stem.replace('Encounter', 'Provider')
    patient_stem = identifier_stem.replace('Encounter', 'Patient')
    provider_internal_id = client.get_internal_id(provider_stem, provider_id_value)
    patient_internal_id = client.get_internal_id(patient_stem, patient_id_value)
    return provider_internal_id, patient_internal_id


def _find_encounter_property(property_name, encounter_properties):
    for encounter_property in encounter_properties:
        if property_name == encounter_property['property_name']:
            return encounter_property['property_value']
    raise KeyError(property_name)


def rds_documentation_text(encounter, documentation_text, sql_driver=None):
    if not sql_driver:
        sql_driver = _build_sql_driver()
    encounter_properties = encounter['vertex_properties']['local_properties']
    patient_id_value = _find_encounter_property('patient_id', encounter_properties)
    provider_id_value = _find_encounter_property('provider_id', encounter_properties)
    identifier_stem = encounter['identifier_stem']['property_value']
    provider_internal_id, patient_internal_id = _resolve_internal_ids(identifier_stem, provider_id_value, patient_id_value)
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
        driver.put_documentation(text_entry)
