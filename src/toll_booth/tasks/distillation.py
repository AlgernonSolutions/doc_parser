import uuid
from datetime import datetime

import bs4
from algernon.aws import StoredData
from algernon import ajson

from toll_booth.tasks.retrieve_documentation import retrieve_documentation


def _distill_strings(documentation):
    soup = bs4.BeautifulSoup(documentation, 'lxml')
    return ' '.join([x for x in soup.strings])


def distill_documentation(encounter_internal_id):
    documentation_data = retrieve_documentation(encounter_internal_id)
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
