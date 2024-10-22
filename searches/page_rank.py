from gqlalchemy import Memgraph
from typing import List, Dict

def pagerank_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # First, run PageRank on the graph
    pagerank_query = """
    CALL pagerank.get() YIELD node, rank
    SET node.pagerank = rank
    """
    memgraph.execute(pagerank_query)
    
    # Now, search for entities related to the query, ordered by PageRank
    search_query = """
    MATCH (e:__Entity__)
    WHERE e.name CONTAINS $query OR e.description CONTAINS $query
    RETURN e.id AS entity_id, e.name AS name, e.description AS description, e.pagerank AS rank
    ORDER BY e.pagerank DESC
    LIMIT $limit
    """
    
    result = memgraph.execute_and_fetch(search_query, {'query': query.lower(), 'limit': limit})
    
    return [{'entity_id': row['entity_id'], 'name': row['name'], 'description': row['description'], 'rank': row['rank']} for row in result]