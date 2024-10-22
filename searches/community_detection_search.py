from gqlalchemy import Memgraph
from typing import List, Dict

def community_detection_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Run community detection algorithm
    community_query = """
    CALL louvain.get()
    YIELD node, community
    SET node.louvain_community = community
    """
    memgraph.execute(community_query)

    # Search for communities related to the query
    search_query = """
    MATCH (e:__Entity__)
    WHERE e.name CONTAINS $query OR e.description CONTAINS $query
    WITH e.louvain_community AS community, count(*) AS relevance
    ORDER BY relevance DESC
    LIMIT $limit
    MATCH (n:__Entity__ {louvain_community: community})
    RETURN community, collect(n.name) AS entities, relevance
    """

    result = memgraph.execute_and_fetch(search_query, {'query': query.lower(), 'limit': limit})

    return [{'community': row['community'], 'entities': row['entities'], 'relevance': row['relevance']} for row in result]
