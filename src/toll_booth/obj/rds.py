from algernon.aws import Opossum
from pymysql import connect


class DocumentationTextEntry:
    def __init__(self,
                 encounter_internal_id,
                 encounter_type,
                 id_source,
                 documentation_text,
                 provider_internal_id,
                 patient_internal_id,
                 provider_id_value,
                 patient_id_value,
                 encounter_id_value):
        self._encounter_internal_id = encounter_internal_id
        self._encounter_type = encounter_type
        self._id_source = id_source
        self._documentation_text = documentation_text
        self._provider_internal_id = provider_internal_id
        self._patient_internal_id = patient_internal_id
        self._provider_id_value = provider_id_value
        self._patient_id_value = patient_id_value
        self._encounter_id_value = encounter_id_value

    @property
    def documentation_text(self):
        return self._documentation_text

    @property
    def encounter_internal_id(self):
        return self._encounter_internal_id

    @property
    def encounter_type(self):
        return self._encounter_type

    @property
    def id_source(self):
        return self._id_source

    @property
    def provider_internal_id(self):
        return self._provider_internal_id

    @property
    def patient_internal_id(self):
        return self._patient_internal_id

    @property
    def provider_id_value(self):
        return self._provider_id_value

    @property
    def patient_id_value(self):
        return self._patient_id_value

    @property
    def for_rds_insertion(self):
        return {
            'encounter_internal_id': self._encounter_internal_id,
            'encounter_type': self._encounter_type,
            'id_source': self._id_source,
            'documentation_text': self._documentation_text,
            'provider_internal_id': self._provider_internal_id,
            'patient_internal_id': self._patient_internal_id,
            'provider_id_value': self._provider_id_value,
            'patient_id_value': self._patient_id_value,
            'encounter_id_value': self._encounter_id_value
        }

    def __getitem__(self, item):
        result = getattr(self, f'_{item}', None)
        if result is None:
            raise KeyError
        return result


class SqlDriver:
    def __init__(self, sql_host, sql_port, db_name, username, password):
        self._sql_host = sql_host
        self._sql_port = sql_port
        self._db_name = db_name
        self._username = username
        self._password = password
        self._cursor = None
        self._connection = None

    @classmethod
    def generate(cls, sql_host, sql_port, db_name):
        credentials = Opossum.get_secrets('rds')
        return cls(sql_host, sql_port, db_name, credentials['username'], credentials['password'])

    def __enter__(self):
        self._connection = connect(
            host=self._sql_host,
            port=self._sql_port,
            database=self._db_name,
            user=self._username,
            password=self._password
        )
        self._cursor = self._connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self._cursor.close()
            self._connection.close()
            self._cursor = None
            self._connection = None
            return True
        raise Exception(exc_val)

    def put_documentation(self, documentation_text_entry: DocumentationTextEntry):
        if self._cursor is None:
            raise RuntimeError(f'must access the SqlDriver from within a context manager')
        command = 'INSERT INTO Documentation ' \
                  '(encounter_internal_id, encounter_type, id_source, documentation_text, provider_internal_id, ' \
                  'patient_internal_id, provider_id_value, patient_id_value, encounter_id_value) VALUES ' \
                  '(%(encounter_internal_id)s, %(encounter_type)s, %(id_source)s, %(documentation_text)s, ' \
                  '%(provider_internal_id)s, %(patient_internal_id)s, %(provider_id_value)s, %(patient_id_value)s, ' \
                  '%(encounter_id_value)s)'
        results = self._cursor.execute(command, documentation_text_entry.for_rds_insertion)
        self._connection.commit()
        return results

    def retrieve_documentation(self, encounter_internal_id: str):
        query = 'SELECT * FROM Documentation WHERE encounter_internal_id = %(encounter_internal_id)s'
        params = {'encounter_internal_id': encounter_internal_id.encode()}
        self._cursor.execute(query, params)
        results = self._cursor.fetchall()
        for entry in results:
            return DocumentationTextEntry(*entry)
