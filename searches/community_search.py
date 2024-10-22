from gqlalchemy import Memgraph
from typing import List, Dict

def community_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Construct the Cypher query
    cypher_query = """
    MATCH (c:__Community__)
    WHERE c.title CONTAINS $query OR c.summary CONTAINS $query
    WITH c, (
        CASE
            WHEN c.title CONTAINS $query AND c.summary CONTAINS $query THEN 2
            ELSE 1
        END
    ) AS relevance
    ORDER BY relevance DESC, c.rank DESC
    LIMIT $limit
    RETURN c.community AS community_id, c.title AS title, c.summary AS summary, c.rank AS rank
    """
    
    # Execute the query
    result = memgraph.execute_and_fetch(cypher_query, {'query': query.lower(), 'limit': limit})
    
    # Process and return the results
    return [{'community_id': row['community_id'], 'title': row['title'], 'summary': row['summary'], 'rank': row['rank']} for row in result]

# Usage example:
# memgraph = Memgraph()
# results = community_search("renewable energy", memgraph)
# for result in results:
#     print(f"Community ID: {result['community_id']}")
#     print(f"Title: {result['title']}")
#     print(f"Summary: {result['summary'][:100]}...")
#     print(f"Rank: {result['rank']}")
#     print("---")
