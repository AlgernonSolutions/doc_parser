import bs4

from toll_booth.tasks.retrieve_documentation import retrieve_documentation


def _distill_strings(documentation):
    soup = bs4.BeautifulSoup(documentation, 'lxml')
    return ' '.join([x for x in soup.strings])


def distill_documentation(encounter_internal_id):
    documentation_data = retrieve_documentation(encounter_internal_id)
    id_source = documentation_data['id_source']
    documentation = documentation_data['documentation']
    distilled_text = _distill_strings(documentation)
    return {'id_source': id_source, 'distilled_text': distilled_text}
