GET_DOCUMENTATION = '''
    query getDocumentation($internal_id: ID!){
        vertex(internal_id: $internal_id){
            vertex_properties(property_names: ["documentation", "id_source", "encounter_type", "encounter_id"]){
                property_name
                property_value{
                    ... on StoredPropertyValue{
                        storage_class
                        storage_uri
                        stored_data_type: data_type
                    }
                    ... on LocalPropertyValue{
                        property_value
                        local_data_type: data_type
                    }
                }
            }
        }
    }
'''

GET_CLIENT_DOCUMENTATION = '''
    query getDocumentation($internal_id: ID!, $token: ID){
        vertex(internal_id: $internal_id){
            connected_edges(edge_labels:["_received_"], token: $token){
                page_info{
                    more
                    token
                }
                edges{
                    inbound{
                        edge_label
                        from_vertex{
                            internal_id
                            vertex_properties(property_names: ["documentation", "id_source", "encounter_type"]){
                                property_name
                                property_value{
                                    ... on StoredPropertyValue{
                                        storage_class
                                        storage_uri
                                        stored_data_type: data_type
                                    }
                                    ... on LocalPropertyValue{
                                        property_value
                                        local_data_type: data_type
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
'''

GET_ENCOUNTER_DOCUMENTATION = '''
    query documentation($encounter_internal_id: ID!, $token: String){
      vertex(internal_id: $encounter_internal_id){
        vertex_type
        connected_edges(edge_labels: ["_documentation_"]){
          edges(token: $token){
            edges{
              documentation: vertex{
                internal_id
                connected_edges(edge_labels: ["_documentation_entry_"]){
                  edges{
                    page_info{
                      documentation_more: more
                      documentation_token: next_token
                    }
                    edges{
                      documentation_entry: vertex{
                        internal_id                        
                        vertex_properties{
                          property_name
                          property_value{
                            ... on LocalPropertyValue{
                              property_value
                            }
                            ... on StoredPropertyValue{
                              storage_uri
                            }
                            ... on SensitivePropertyValue{
                              sensitive
                            }
                          }
                        }                     
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
'''

GET_PROVIDER_ENCOUNTERS = '''
    query encounters($identifier_stem: String!, $provider_id: String!, $token: String, $page_size: Int){
        get_vertex(identifier_stem: $identifier_stem, sid_value: $provider_id){
            connected_edges(edge_labels: ["_provided_"]){
                edges(token: $token, page_size: $page_size){
                    page_info{
                        more
                        next_token
                    }
                    edges{
                        encounter: vertex{
                            internal_id
                            vertex_properties(property_names: ["encounter_type"]){
                                property_value{
                                    ... on LocalPropertyValue{
                                        encounter_type: property_value
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
'''

GET_COMMUNITY_SUPPORT_DOCUMENTATION = """
query get_comm_supt($encounter_identifier_stem: String!, $object_type: String!, $object_properties: [InputLocalProperty]!, $token: ID){
  list_vertexes(identifier_stem: $encounter_identifier_stem, object_type: $object_type, object_properties: $object_properties, token: $token){
    token
    vertexes{
      internal_id
      vertex_properties(property_names: ["documentation", "id_source", "encounter_type", "encounter_id"]){
            property_name
            property_value{
                ... on StoredPropertyValue{
                    storage_class
                    storage_uri
                    stored_data_type: data_type
                }
                ... on LocalPropertyValue{
                    property_value
                    local_data_type: data_type
                }
            }
        }
    }
  }
}
"""

GET_PROVIDER_ENCOUNTER_COUNT = """
     query encounters($identifier_stem: String!, $provider_id: String!){
        get_vertex(identifier_stem: $identifier_stem, sid_value: $provider_id){
            connected_edges(edge_labels: ["_provided_"]){
                edge_label
                total_count
            }
        }
    }
"""
