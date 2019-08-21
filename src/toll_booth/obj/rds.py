import os
from datetime import datetime
from decimal import Decimal

import boto3
from algernon.aws import Opossum
from botocore.exceptions import ClientError
from pymysql import connect, IntegrityError


def _set_parameter_type(parameter):
    if isinstance(parameter, str):
        return {'stringValue': parameter}
    if isinstance(parameter, datetime):
        return {'stringValue': parameter.isoformat()}
    if isinstance(parameter, bool):
        return {'booleanValue': parameter}
    if parameter is None:
        return {'isNull': True}
    if isinstance(parameter, Decimal):
        return {'doubleValue': float(parameter)}
    if isinstance(parameter, int):
        return {'longValue': parameter}
    if isinstance(parameter, bytes):
        return {'blobValue': parameter}
    raise RuntimeError(f'do not know how to set parameter_type for {parameter}')


class IndexViolationException(Exception):
    def __init__(self, index_name, key_value):
        self._index_name = index_name
        self._key_value = key_value

    @classmethod
    def from_integrity_error(cls, error):
        error_msg = error.args[1]
        error_msg = error_msg.replace('Duplicate entry ', '')
        error_msg = error_msg.replace(' for key ', '!')
        error_msg = error_msg.replace("'", '')
        pieces = error_msg.split('!')
        return cls(pieces[1], pieces[0])


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


class ApiDriver:
    def __init__(self, db_name, db_cluster_arn, secret_arn):
        self._db_name = db_name
        self._db_cluster_arn = db_cluster_arn
        self._secret_arn = secret_arn

    @property
    def db_name(self):
        return self._db_name

    @property
    def db_cluster_arn(self):
        return self._db_cluster_arn

    @property
    def secret_arn(self):
        return self._secret_arn

    @classmethod
    def generate(cls):
        return cls(os.environ['RDS_DB_NAME'], os.environ['RDS_CLUSTER_ARN'], os.environ['RDS_SECRET_ARN'])

    def put_documentation(self, documentation_text_entry: DocumentationTextEntry):
        command = 'INSERT INTO Documentation ' \
                  '(encounter_internal_id, encounter_type, id_source, documentation_text, provider_internal_id, ' \
                  'patient_internal_id, provider_id_value, patient_id_value, encounter_id_value) VALUES ' \
                  '(:encounter_internal_id, :encounter_type, :id_source, :documentation_text, :provider_internal_id, ' \
                  ':patient_internal_id, :provider_id_value, :patient_id_value, :encounter_id_value)'
        try:
            params = documentation_text_entry.for_rds_insertion
            results = self._send(command, params)
        except ClientError as e:
            if e.args[0] != 1062:
                raise e
            raise IndexViolationException.from_integrity_error(e)
        return results

    def retrieve_documentation(self, encounter_internal_id: str):
        query = 'SELECT * FROM Documentation WHERE encounter_internal_id = :encounter_internal_id'
        params = {'encounter_internal_id': encounter_internal_id.encode()}
        results = self._send(query, params)
        return results

    def _send(self, sql, parameters=None, transaction_id=None):
        client = boto3.client('rds-data')
        statement_kwargs = {
            "database": self._db_name,
            "resourceArn": self._db_cluster_arn,
            "secretArn": self._secret_arn,
            "sql": sql
        }
        if transaction_id:
            statement_kwargs['transactionId'] = transaction_id
        if parameters:
            statement_kwargs['parameters'] = [{
                'name': x,
                'value': _set_parameter_type(y)
            } for x, y in parameters.items()]
        response = client.execute_statement(**statement_kwargs)
        results = response.get('records', [])
        records = []
        for row in results:
            for cell in row:
                records.extend([x for x in cell.values()])
        return records


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
        raise exc_val

    def put_documentation(self, documentation_text_entry: DocumentationTextEntry):
        if self._cursor is None:
            raise RuntimeError(f'must access the SqlDriver from within a context manager')
        command = 'INSERT INTO Documentation ' \
                  '(encounter_internal_id, encounter_type, id_source, documentation_text, provider_internal_id, ' \
                  'patient_internal_id, provider_id_value, patient_id_value, encounter_id_value) VALUES ' \
                  '(%(encounter_internal_id)s, %(encounter_type)s, %(id_source)s, %(documentation_text)s, ' \
                  '%(provider_internal_id)s, %(patient_internal_id)s, %(provider_id_value)s, %(patient_id_value)s, ' \
                  '%(encounter_id_value)s)'
        try:
            params = documentation_text_entry.for_rds_insertion
            results = self._cursor.execute(command, params)
            self._connection.commit()
        except IntegrityError as e:
            if e.args[0] != 1062:
                raise e
            raise IndexViolationException.from_integrity_error(e)
        return results

    def retrieve_documentation(self, encounter_internal_id: str):
        query = 'SELECT * FROM Documentation WHERE encounter_internal_id = %(encounter_internal_id)s'
        params = {'encounter_internal_id': encounter_internal_id.encode()}
        self._cursor.execute(query, params)
        results = self._cursor.fetchall()
        for entry in results:
            return DocumentationTextEntry(*entry)
