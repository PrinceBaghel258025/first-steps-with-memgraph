from gqlalchemy import Memgraph
from typing import List, Dict
import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

def semantic_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Encode the query
    query_embedding = model.encode(query)
    
    # Construct the Cypher query using Memgraph's vector similarity function
    cypher_query = """
    MATCH (e:__Entity__)
    WHERE e.description_embedding IS NOT NULL
    WITH e, mg.vector.similarity.cosine(e.description_embedding, $query_embedding) AS similarity
    ORDER BY similarity DESC
    LIMIT $limit
    RETURN e.id AS entity_id, e.name AS name, e.description AS description, similarity
    """
    
    # Execute the query
    result = memgraph.execute_and_fetch(cypher_query, {'query_embedding': query_embedding.tolist(), 'limit': limit})
    
    # Process and return the results
    return [{'entity_id': row['entity_id'], 'name': row['name'], 'description': row['description'], 'similarity': row['similarity']} for row in result]