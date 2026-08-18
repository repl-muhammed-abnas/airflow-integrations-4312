def get_allocation_details_graphql_query(dag_run):
    return {
        "operationName": "ResourceAllocationDetailsQuery",
        "variables": {
            "resourceAllocationId": [dag_run.conf['resource_uri']]
        },
        "query": """query ResourceAllocationDetailsQuery($resourceAllocationId: [String!]) {
            resourceAllocations(
                filter: {
                resourceAllocationIds: $resourceAllocationId
                }
            ) {
                resourceAllocations {
                id
                projectUri
                startDate
                endDate
                totalHours
                __typename
                }
                __typename
            }
        }"""
    }
