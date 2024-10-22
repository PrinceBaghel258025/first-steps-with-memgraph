from gqlalchemy import Memgraph
from typing import List, Dict
import spacy

nlp = spacy.load("en_core_web_sm")

def entity_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Extract entities from the query
    doc = nlp(query)
    entities = [ent.text for ent in doc.ents]
    
    # Construct the Cypher query
    cypher_query = """
    MATCH (e:__Entity__)
    WHERE e.name IN $entities
    WITH e
    MATCH (e)<-[:HAS_ENTITY]-(c:__Chunk__)
    RETURN DISTINCT c.id AS chunk_id, c.text AS text, count(DISTINCT e) AS entity_matches
    ORDER BY entity_matches DESC
    LIMIT $limit
    """
    
    # Execute the query
    result = memgraph.execute_and_fetch(cypher_query, {'entities': entities, 'limit': limit})
    
    # Process and return the results
    return [{'chunk_id': row['chunk_id'], 'text': row['text'], 'entity_matches': row['entity_matches']} for row in result]

# Usage example:
# memgraph = Memgraph()
# results = entity_search("How does climate change affect New York and London?", memgraph)
# for result in results:
#     print(f"Chunk ID: {result['chunk_id']}")
#     print(f"Text: {result['text'][:100]}...")
#     print(f"Entity Matches: {result['entity_matches']}")
#     print("---")
