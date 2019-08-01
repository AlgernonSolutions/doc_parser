import re

import bs4
import rapidjson

from toll_booth.obj.credibles.data_obj import TableEncoder, CredibleTable


def _test_element_as_header(element, link_pattern):
    links = element.find_all('a')
    targets = [x.attrs['href'] for x in links]
    target_string = ' '.join(targets)
    matches = link_pattern.search(target_string)
    if matches:
        return True
    return False


def _test_element_as_signature_block(element, signature_pattern):
    links = element.find_all('img')
    targets = [x.attrs['src'] for x in links]
    target_string = ' '.join(targets)
    matches = signature_pattern.search(target_string)
    if matches:
        return True
    return False


def _test_element_as_spacer(element):
    if [x for x in element.stripped_strings]:
        return False
    return True


class CredibleFormExtraction:
    def __init__(self, form_soup):
        self._form_soup = form_soup

    @classmethod
    def from_raw_html(cls, raw_html):
        return cls(bs4.BeautifulSoup(raw_html, 'lxml'))

    @property
    def form_soup(self):
        return self._form_soup

    @property
    def note_body(self):
        table = self._form_soup.find('body').find('table', recursive=False)
        return table

    @property
    def note_body_elements(self):
        table_rows = self.note_body.find_all('tr', recursive=False)
        return table_rows

    @property
    def identified_pieces(self):
        identified = {'unspecified': [], 'spacers': []}
        link_pattern = re.compile(r'(/client/client_view\.asp).*(/employee/emp_view\.asp).*(/client/list_auth.asp)')
        signature_pattern = re.compile(r'/admin/images\.ashx\?t=s.+sig_type=visit')
        for element in self.note_body_elements:
            if _test_element_as_header(element, link_pattern):
                if identified.get('header'):
                    raise RuntimeError(f'found two headers for the same note')
                identified['header'] = element
                continue
            if _test_element_as_signature_block(element, signature_pattern):
                if identified.get('signatures'):
                    raise RuntimeError(f'found two headers for the same note')
                identified['signatures'] = element
                continue
            if _test_element_as_spacer(element):
                identified['spacers'].append(element)
                continue
            identified['unspecified'].append(element)
        return identified


class CredibleFormHeader:
    def __init__(self, header_data):
        self._header_data = header_data

    @classmethod
    def from_extraction_element(cls, extraction_element):
        return cls({})


class CredibleNoteBody:
    def __init__(self, body_entries):
        self._body_entries = body_entries

    @classmethod
    def from_extracted_piece(cls, extracted_piece):
        top_table = CredibleTable.from_soup(extracted_piece.find('table'))
        entries = rapidjson.loads(rapidjson.dumps(top_table, default=TableEncoder.default))

        def _clean_entry(entry):
            if isinstance(entry, list):
                cleaned = []
                for sub_entry in entry:
                    clean = _clean_entry(sub_entry)
                    if clean:
                        cleaned.append(clean)
                if len(cleaned) == 1:
                    return cleaned[0]
                return cleaned
            return entry
        return cls(_clean_entry(entries))

    @property
    def body_entries(self):
        return self._body_entries

    def rehydrate(self):
        hydrated = {}
        for pointer, entry in enumerate(self._body_entries):
            if isinstance(entry, str):
                try:
                    fields = self._body_entries[pointer+1]
                except IndexError:
                    continue
                for field_pointer, field in enumerate(fields):
                    if isinstance(field, list):
                        if '!#!' not in field:
                            key_value = f'{entry}#{field[0]}'
                            hydrated[key_value] = field[1]
                            continue
                        try:
                            next_field = fields[field_pointer+1]
                        except IndexError:
                            continue
                        if isinstance(next_field, str):
                            key_value = f'{entry}#{field[0]}'
                            hydrated[key_value] = next_field
                            continue
                        if isinstance(next_field, list):
                            if '!#!' in next_field:
                                key_value = f'{entry}#{field[0]}'
                                hydrated[key_value] = ''
                                continue
        return hydrated


class CredibleSignatureBlock:
    def __init__(self, elements):
        self._elements = elements

    @classmethod
    def from_extracted_piece(cls, extracted_piece):
        return cls(extracted_piece)


class CredibleForm:
    def __init__(self, header, signature_block, note_body, unspecified=None):
        self._header = header
        self._signature_block = signature_block
        self._note_body = note_body
        self._unspecified = unspecified

    @classmethod
    def from_extracted_pieces(cls, extracted_pieces):
        header = extracted_pieces.get('header')
        note_body = extracted_pieces.get('note_body')
        signatures = extracted_pieces.get('signatures')
        unspecified = extracted_pieces['unspecified']
        if signatures:
            signatures = CredibleSignatureBlock.from_extracted_piece(signatures)
        if note_body is None and len(unspecified) == 1:
            note_body = unspecified[0]
            unspecified = []
        if note_body:
            note_body = CredibleNoteBody.from_extracted_piece(note_body)
        return cls(header, signatures, note_body, unspecified)

    @property
    def header(self):
        return self._header

    @property
    def note_body(self):
        return self._note_body

    @property
    def unspecified(self):
        return self._unspecified
