from gqlalchemy import Memgraph
from typing import List, Dict
from keyword_search import keyword_search
from semantic_search import semantic_search
from entity_search import entity_search
from community_search import community_search
from pagerank_search import pagerank_search
from node2vec_search import node2vec_search
from betweenness_search import betweenness_search
from community_detection_search import community_detection_search

def hybrid_search(query: str, memgraph: Memgraph, limit: int = 10) -> List[Dict]:
    # Perform individual searches
    keyword_results = keyword_search(query, memgraph, limit)
    semantic_results = semantic_search(query, memgraph, limit)
    entity_results = entity_search(query, memgraph, limit)
    community_results = community_search(query, memgraph, limit)
    pagerank_results = pagerank_search(query, memgraph, limit)
    node2vec_results = node2vec_search(query, memgraph, limit)
    betweenness_results = betweenness_search(query, memgraph, limit)
    community_detection_results = community_detection_search(query, memgraph, limit)
    
    # Combine and rank results
    combined_results = {}
    
    for result in keyword_results:
        combined_results[result['chunk_id']] = {'text': result['text'], 'score': result['relevance_score']}
    
    for result in semantic_results:
        if result['entity_id'] in combined_results:
            combined_results[result['entity_id']]['score'] += result['similarity'] * 2
        else:
            combined_results[result['entity_id']] = {'text': result['description'], 'score': result['similarity'] * 2}
    
    for result in entity_results:
        if result['chunk_id'] in combined_results:
            combined_results[result['chunk_id']]['score'] += result['entity_matches'] * 1.5
        else:
            combined_results[result['chunk_id']] = {'text': result['text'], 'score': result['entity_matches'] * 1.5}
    
    for result in community_results:
        if result['community_id'] in combined_results:
            combined_results[result['community_id']]['score'] += result['rank']
        else:
            combined_results[result['community_id']] = {'text': result['summary'], 'score': result['rank']}
    
    for result in pagerank_results:
        if result['entity_id'] in combined_results:
            combined_results[result['entity_id']]['score'] += result['rank'] * 3
        else:
            combined_results[result['entity_id']] = {'text': result['description'], 'score': result['rank'] * 3}
    
    for result in node2vec_results:
        if result['entity_id'] in combined_results:
            combined_results[result['entity_id']]['score'] += result['similarity'] * 2.5
        else:
            combined_results[result['entity_id']] = {'text': result['description'], 'score': result['similarity'] * 2.5}
    
    for result in betweenness_results:
        if result['entity_id'] in combined_results:
            combined_results[result['entity_id']]['score'] += result['importance'] * 2
        else:
            combined_results[result['entity_id']] = {'text': result['description'], 'score': result['importance'] * 2}
    
    for result in community_detection_results:
        for entity in result['entities']:
            if entity in combined_results:
                combined_results[entity]['score'] += result['relevance'] * 1.5
            else:
                combined_results[entity] = {'text': f"Part of community {result['community']}", 'score': result['relevance'] * 1.5}
    
    # Sort and limit results
    sorted_results = sorted(combined_results.items(), key=lambda x: x[1]['score'], reverse=True)[:limit]
    
    return [{'id': item[0], 'text': item[1]['text'], 'score': item[1]['score']} for item in sorted_results]

# Usage example:
# memgraph = Memgraph()
# results = hybrid_search("renewable energy impact on climate change", memgraph)
# for result in results:
#     print(f"ID: {result['id']}")
#     print(f"Text: {result['text'][:100]}...")
#     print(f"Score: {result['score']}")
#     print("---")
