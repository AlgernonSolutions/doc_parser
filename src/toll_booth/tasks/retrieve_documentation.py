import os

from toll_booth.obj.gql.gql_client import GqlClient
from toll_booth.tasks.aws_tasks import s3_tasks


def retrieve_documentation(encounter_internal_id):
    gql_endpoint = os.environ['GRAPH_GQL_ENDPOINT']
    client = GqlClient.from_gql_endpoint(gql_endpoint)
    documentation_property = client.get_documentation_property(encounter_internal_id)
    documentation_uri = documentation_property['documentation']['storage_uri']
    documentation = s3_tasks.retrieve_s3_property(documentation_uri)
    id_source = documentation_property['id_source']['property_value']
    encounter_id = documentation_property['encounter_id']['property_value']
    return {'id_source': id_source, 'documentation': documentation, 'encounter_id': encounter_id}