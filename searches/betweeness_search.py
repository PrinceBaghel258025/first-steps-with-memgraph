from gqlalchemy import Memgraph
from typing import List, Dict

def betweenness_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Run betweenness centrality algorithm
    betweenness_query = """
    CALL betweenness_centrality.get()
    YIELD node, betweenness_centrality
    SET node.betweenness = betweenness_centrality
    """
    memgraph.execute(betweenness_query)

    # Search for important connector entities related to the query
    search_query = """
    MATCH (e:__Entity__)
    WHERE e.name CONTAINS $query OR e.description CONTAINS $query
    RETURN e.id AS entity_id, e.name AS name, e.description AS description, e.betweenness AS importance
    ORDER BY e.betweenness DESC
    LIMIT $limit
    """

    result = memgraph.execute_and_fetch(search_query, {'query': query.lower(), 'limit': limit})

    return [{'entity_id': row['entity_id'], 'name': row['name'], 'description': row['description'], 'importance': row['importance']} for row in result]