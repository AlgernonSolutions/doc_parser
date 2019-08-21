import uuid
from datetime import datetime

import bs4
import rapidjson
from algernon import ajson
from algernon.aws import StoredData

from toll_booth.tasks.aws_tasks import s3_tasks


def _distill_strings(documentation):
    soup = bs4.BeautifulSoup(documentation, 'lxml')
    return ' '.join([x for x in soup.strings])


def _find_vertex_property(property_name, property_type, vertex_properties, value_name=None):
    if not value_name:
        value_name = 'property_value'
    search_properties = vertex_properties[property_type]
    for search_property in search_properties:
        search_property_name = search_property['property_name']
        if property_name == search_property_name:
            return search_property[value_name]
    raise RuntimeError(f'could not find {property_type}.{property_name} on vertex: {vertex_properties}')


def _retrieve_documentation(vertex_properties):
    storage_uri = _find_vertex_property('documentation', 'stored_properties', vertex_properties, 'storage_uri')
    return s3_tasks.retrieve_s3_property(storage_uri)


def distill_documentation(encounter_vertex):
    if isinstance(encounter_vertex, str):
        encounter_vertex = rapidjson.loads(encounter_vertex)
    vertex_properties = encounter_vertex['vertex_properties']
    documentation = _retrieve_documentation(vertex_properties)
    id_source = _find_vertex_property('id_source', 'local_properties', vertex_properties)
    encounter_id = int(encounter_vertex['id_value']['property_value'])
    distilled_text = _distill_strings(documentation)
    extracted_data = {
        'object_type': 'DocumentationText',
        'identifier': f'#{id_source}#Encounter#',
        'id_value': encounter_id,
        'extracted_data': {
            'source': {
                'id_source': id_source,
                'encounter_id': encounter_id,
                'utc_generated_datetime': datetime.utcnow().isoformat(),
                'documentation_text': distilled_text
            }
        }

    }
    stored_data = StoredData.from_object(uuid.uuid4(), extracted_data, full_unpack=True)
    return ajson.dumps(stored_data)
