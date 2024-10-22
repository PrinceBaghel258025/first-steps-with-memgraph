from gqlalchemy import Memgraph
from typing import List, Dict
import re

def keyword_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Preprocess the query
    keywords = re.findall(r'\w+', query.lower())
    
    # Construct the Cypher query
    cypher_query = """
    MATCH (c:__Chunk__)
    WHERE ANY(keyword IN $keywords WHERE c.text CONTAINS keyword)
    WITH c, size([keyword IN $keywords WHERE c.text CONTAINS keyword]) AS matches
    ORDER BY matches DESC
    LIMIT $limit
    RETURN c.id AS chunk_id, c.text AS text, matches
    """
    
    # Execute the query
    result = memgraph.execute_and_fetch(cypher_query, {'keywords': keywords, 'limit': limit})
    
    # Process and return the results
    return [{'chunk_id': row['chunk_id'], 'text': row['text'], 'relevance_score': row['matches']} for row in result]

# Usage example:
# memgraph = Memgraph()
# results = keyword_search("artificial intelligence and ethics", memgraph)
# for result in results:
#     print(f"Chunk ID: {result['chunk_id']}")
#     print(f"Text: {result['text'][:100]}...")
#     print(f"Relevance Score: {result['relevance_score']}")
#     print("---")
