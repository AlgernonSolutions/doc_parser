from algernon.aws.gql import GqlDriver
from algernon import ajson

from jsonpath_rw import parse

from toll_booth.obj.gql import gql_queries


def _standardize_gql_endpoint(gql_endpoint: str):
    variable_fields = ('https://', 'http://', '/graphql')
    for field_name in variable_fields:
        gql_endpoint = gql_endpoint.replace(field_name, '')
    return gql_endpoint


class GqlClient:
    def __init__(self, gql_connection: GqlDriver):
        self._gql_connection = gql_connection

    @classmethod
    def from_gql_endpoint(cls, gql_endpoint: str):
        gql_endpoint = _standardize_gql_endpoint(gql_endpoint)
        gql_connection = GqlDriver(gql_endpoint)
        return cls(gql_connection)

    def _send(self, query, variables=None):
        if not variables:
            variables = {}
        response = self._gql_connection.send(query, variables)
        return ajson.loads(response)

    def get_documentation(self, identifier_stem, encounter_type):
        results = {}
        query_results, token = self.paginate_documentation(identifier_stem, encounter_type)
        results.update(query_results)
        while token:
            query_results, token = self.paginate_documentation(identifier_stem, encounter_type, token)
            results.update(query_results)
        return results

    def paginate_documentation(self, identifier_stem, encounter_type, token=None):
        results = {}
        variables = {
            'encounter_identifier_stem': identifier_stem,
            'object_type': 'Encounter',
            'object_properties': [
                {
                    'property_name': 'encounter_type',
                    'data_type': 'S',
                    'property_value': encounter_type
                }
            ]
        }
        if token:
            variables['token'] = token
        response = self._send(gql_queries.GET_COMMUNITY_SUPPORT_DOCUMENTATION, variables)
        fn_response = response['data']['list_vertexes']
        for vertex in fn_response['vertexes']:
            vertex_properties = {}
            encounter_internal_id = vertex['internal_id']
            for vertex_property in vertex['vertex_properties']:
                property_name = vertex_property['property_name']
                if property_name in ['documentation', 'id_source', 'encounter_type']:
                    vertex_properties[property_name] = vertex_property['property_value']
            results[encounter_internal_id] = vertex_properties
        return results, fn_response.get('token')

    def get_client_documentation_properties(self, internal_id):
        results = {}
        query_results, token = self._paginate_client_documentation(internal_id)
        results.update(query_results)
        while token:
            query_results, token = self._paginate_client_documentation(internal_id, token)
            results.update(query_results)
        return results

    def _paginate_client_documentation(self, internal_id, token=None):
        results = {}
        variables = {'internal_id': internal_id}
        if token:
            variables['token'] = token
        response = self._send(gql_queries.GET_CLIENT_DOCUMENTATION, variables)
        connected_edges = response['data']['vertex']['connected_edges']
        page_info = connected_edges['page_info']
        edges = connected_edges.get('edges', {})
        for entry in edges.get('inbound', []):
            vertex_properties = {}
            vertex = entry['from_vertex']
            encounter_internal_id = vertex['internal_id']
            for vertex_property in vertex['vertex_properties']:
                property_name = vertex_property['property_name']
                if property_name in ['documentation', 'id_source', 'encounter_type']:
                    vertex_properties[property_name] = vertex_property['property_value']
            results[encounter_internal_id] = vertex_properties
        if page_info.get('more'):
            return results, page_info.get('token')
        return results, None

    def get_documentation_property(self, internal_id: str):
        results = {}
        response = self._send(gql_queries.GET_DOCUMENTATION, {'internal_id': internal_id})
        vertex_properties = response['data']['vertex']['vertex_properties']
        for entry in vertex_properties:
            property_name = entry['property_name']
            results[property_name] = entry['property_value']
        return results

    def paginate_provider_encounters(self, id_source, provider_id, encounter_type, token=None, page_size=250):
        encounter_expr = parse('data.get_vertex.connected_edges.[*].edges.edges.[*].encounter')
        encounter_type_expr = parse('vertex_properties.[*].property_value.encounter_type')
        page_info_expr = parse('data.get_vertex.connected_edges.[*].edges.page_info')
        variables = {
            'identifier_stem': f'#vertex#Provider#{{\\"id_source\\": \\"{id_source}\\"}}#',
            'provider_id': provider_id,
            'page_size': page_size
        }
        if token:
            variables['token'] = token
        response = self._send(gql_queries.GET_PROVIDER_ENCOUNTERS, variables)
        encounters = [x.value for x in encounter_expr.find(response)]
        page_info = [x.value for x in page_info_expr.find(response)][0]
        token = None
        filtered_encounters = [x['internal_id'] for x in encounters if encounter_type in [
            x.value for x in encounter_type_expr.find(x)]]
        if page_info['more']:
            token = page_info['next_token']
        return filtered_encounters, token

    def get_encounter_documentation(self, encounter_internal_id):
        expr = parse(
            'data.vertex.connected_edges.[*].edges.edges.[*].documentation'
            '.connected_edges.[*].edges.edges.[*].documentation_entry')
        variables = {
            'encounter_internal_id': encounter_internal_id
        }
        response = self._send(gql_queries.GET_ENCOUNTER_DOCUMENTATION, variables)
        return [x.value for x in expr.find(response)]

    def get_provider_encounter_count(self, id_source, provider_id):
        variables = {
            'identifier_stem': f'#vertex#Provider#{{\\"id_source\\": \\"{id_source}\\"}}#',
            'provider_id': provider_id
        }
        response = self._send(gql_queries.GET_PROVIDER_ENCOUNTER_COUNT, variables)
        expr = parse('data.get_vertex.connected_edges.[*]')
        edge_summaries = [x.value for x in expr.find(response)]
        for summary in edge_summaries:
            if summary['edge_label'] == '_provided_':
                return summary['total_count']

    def get_encounter(self, encounter_internal_id):
        query = '''
            query get_id_value($internal_id: ID!){
                vertex(internal_id: $internal_id){
                    vertex_properties{
                        property_name
                        property_value{
                            ... on LocalPropertyValue{
                                property_value
                            }
                        }
                    }
                }
            }
        '''
        variables = {'internal_id': encounter_internal_id}
        response = self._send(query, variables)
        property_expr = parse('data.vertex.vertex_properties.[*]')
        vertex_properties = [x.value for x in property_expr.find(response)]
        filtered_properties = {
            'encounter_id': [
                x['property_value']['property_value'] for x in vertex_properties if x['property_name'] == 'encounter_id'
            ][0],
            'patient_id': [
                x['property_value']['property_value'] for x in vertex_properties if x['property_name'] == 'patient_id'
            ][0],
            'provider_id': [
                x['property_value']['property_value'] for x in vertex_properties if x['property_name'] == 'provider_id'
            ][0],
        }
        return filtered_properties

    def get_internal_id(self, identifier_stem, id_value):
        variables = {
            'identifier_stem': identifier_stem,
            'sid_value': str(id_value)
        }
        response = self._send(gql_queries.GET_INTERNAL_ID, variables)
        internal_id = response['data']['get_vertex']['internal_id']
        return internal_id

    def get_encounter_data(self, identifier_stem, sid_value):
        variables = {'identifier_stem': identifier_stem, 'sid_value': sid_value}
        results = self._send(gql_queries.GET_ENCOUNTER, variables=variables)
        return results
