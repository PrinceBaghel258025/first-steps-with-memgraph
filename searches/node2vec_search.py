from gqlalchemy import Memgraph
from typing import List, Dict
import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

def node2vec_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Encode the query
    query_embedding = model.encode(query)

    # Run node2vec algorithm
    node2vec_query = """
    CALL node2vec.set(128, 10, 1, 1, 'RELATED')
    YIELD node, embedding
    SET node.node2vec_embedding = embedding
    """
    memgraph.execute(node2vec_query)

    # Search for similar entities using node2vec embeddings
    search_query = """
    MATCH (e:__Entity__)
    WHERE e.node2vec_embedding IS NOT NULL
    WITH e, mg.vector.similarity.cosine(e.node2vec_embedding, $query_embedding) AS similarity
    ORDER BY similarity DESC
    LIMIT $limit
    RETURN e.id AS entity_id, e.name AS name, e.description AS description, similarity
    """

    result = memgraph.execute_and_fetch(search_query, {'query_embedding': query_embedding.tolist(), 'limit': limit})

    return [{'entity_id': row['entity_id'], 'name': row['name'], 'description': row['description'], 'similarity': row['similarity']} for row in result]