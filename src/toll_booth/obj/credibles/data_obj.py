import json

import bs4


class TableEncoder(json.JSONEncoder):
    @classmethod
    def default(cls, obj):
        if isinstance(obj, CredibleTable):
            return obj.table_rows
        if isinstance(obj, CredibleTableRow):
            return obj.table_data
        if isinstance(obj, CredibleTableDatum):
            return obj.datum_value
        return super(TableEncoder, cls()).default(obj)


class CredibleTable:
    def __init__(self, table_rows):
        self._table_rows = table_rows

    @classmethod
    def from_soup(cls, table_soup):
        table_rows = table_soup.find_all('tr', recursive=False)
        return cls([CredibleTableRow.from_soup(x) for x in table_rows])

    @property
    def table_rows(self):
        return [x for x in self._table_rows if x]


class CredibleTableRow:
    def __init__(self, table_data):
        self._table_data = table_data

    @classmethod
    def from_soup(cls, row_soup):
        table_datum = row_soup.find_all('td', recursive=False)
        return cls([CredibleTableDatum.from_datum_soup(x) for x in table_datum])

    @property
    def table_data(self):
        return [x for x in self._table_data if x]


class CredibleTableDatum:
    def __init__(self, datum_value):
        self._datum_value = datum_value

    @classmethod
    def from_datum_soup(cls, datum_soup):
        values = []
        datum_contents = datum_soup.contents
        for datum_content in datum_contents:
            if isinstance(datum_content, bs4.NavigableString):
                value = str(datum_content)
                value = value.replace('   ', "!#!")
                value = value.strip()
                values.append(value)
                continue
            contents_name = datum_content.name
            if contents_name == 'table':
                datum_value = CredibleTable.from_soup(datum_content)
                values.append(datum_value)
                continue
            if contents_name == 'b':
                values.append(datum_content.text)
                continue
            if contents_name in ['font', 'img',  None]:
                continue
            if contents_name == 'br':
                values.append('\n')
                continue
            if contents_name == 'p':
                values.append(datum_content.text.strip())
                continue
            if contents_name == 'a':
                values.append(datum_content.text.strip())
                continue
            if contents_name == 'span':
                values.append(datum_content.text.strip())
                continue
            raise RuntimeError(f'have not dealt with: {datum_content}')
        return cls(values)

    @property
    def datum_value(self):
        return self._datum_value
