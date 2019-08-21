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


def _retrieve_documentation(encounter_vertex):
    vertex_properties = encounter_vertex['vertex_properties']['stored_properties']
    for stored_property in vertex_properties:
        property_name = stored_property['property_name']
        if property_name == 'documentation':
            storage_uri = stored_property['storage_uri']
            return s3_tasks.retrieve_s3_property(storage_uri)
    raise RuntimeError(f'could not find documentation property on vertex: {encounter_vertex}')


def distill_documentation(encounter_vertex):
    if isinstance(encounter_vertex, str):
        encounter_vertex = rapidjson.loads(encounter_vertex)
    documentation_data = _retrieve_documentation(encounter_vertex)
    id_source = documentation_data['id_source']
    documentation = documentation_data['documentation']
    encounter_id = documentation_data['encounter_id']
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
