import time
import numpy as np
import pandas as pd
from gqlalchemy import Memgraph
from gqlalchemy.exceptions import GQLAlchemyDatabaseError
import backoff

# Configuration
GRAPHRAG_FOLDER = "par2"
MEMGRAPH_HOST = "localhost"
MEMGRAPH_PORT = 7687

# Create Memgraph connection
memgraph = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

@backoff.on_exception(backoff.expo, GQLAlchemyDatabaseError, max_tries=5)
def execute_with_retry(memgraph, query, parameters):
    return memgraph.execute(query, parameters)

def convert_numpy_types(obj):
    """
    Convert numpy types to Python native types for Memgraph compatibility.
    """
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj

def batched_import(statement, df, batch_size=100):
    """
    Import a dataframe into Memgraph using a batched approach.
    Handles numpy array conversion and implements retry mechanism.
    """
    total = len(df)
    start_s = time.time()
    for start in range(0, total, batch_size):
        batch = df.iloc[start : min(start + batch_size, total)]
        # Convert the batch to records and handle numpy types
        records = [convert_numpy_types(record) for record in batch.to_dict('records')]
        try:
            execute_with_retry(
                memgraph,
                "UNWIND $rows AS value " + statement,
                {'rows': records}
            )
        except GQLAlchemyDatabaseError as e:
            print(f"Error importing batch starting at index {start}: {e}")
            print("Retrying with smaller batch size...")
            batched_import(statement, batch, batch_size=batch_size//2)
        
        if (start + batch_size) % 1000 == 0 or (start + batch_size) >= total:
            print(f"Imported {min(start + batch_size, total)} / {total} rows in {time.time() - start_s:.2f} s.")
    
    print(f"Total: {total} rows imported in {time.time() - start_s:.2f} s.")
    return total

# Create constraints
constraints = [
    "CREATE CONSTRAINT ON (c:__Chunk__) ASSERT c.id IS UNIQUE;",
    "CREATE CONSTRAINT ON (d:__Document__) ASSERT d.id IS UNIQUE;",
    "CREATE CONSTRAINT ON (c:__Community__) ASSERT c.community IS UNIQUE;",
    "CREATE CONSTRAINT ON (e:__Entity__) ASSERT e.id IS UNIQUE;",
    "CREATE CONSTRAINT ON (e:__Entity__) ASSERT e.name IS UNIQUE;",
    "CREATE CONSTRAINT ON (e:__Covariate__) ASSERT e.title IS UNIQUE;"
]

for constraint in constraints:
    try:
        memgraph.execute(constraint)
        print(f"Created constraint: {constraint}")
    except Exception as e:
        print(f"Constraint might already exist or failed: {e}")

# Import documents
print("Importing documents...")
doc_df = pd.read_parquet(
    f"{GRAPHRAG_FOLDER}/create_final_documents.parquet", 
    columns=["id", "title"]
)

document_statement = """
MERGE (d:__Document__ {id: value.id})
SET d.title = value.title
"""

batched_import(document_statement, doc_df)

# Import text chunks
print("Importing text chunks...")
text_df = pd.read_parquet(
    f"{GRAPHRAG_FOLDER}/create_final_text_units.parquet",
    columns=["id", "text", "n_tokens", "document_ids"]
)

text_statement = """
MERGE (c:__Chunk__ {id: value.id})
SET c.text = value.text,
    c.n_tokens = value.n_tokens
WITH c, value
UNWIND value.document_ids AS document_id
MATCH (d:__Document__ {id: document_id})
MERGE (c)-[:PART_OF]->(d)
"""

batched_import(text_statement, text_df)

# Import entities
print("Importing entities...")
entity_df = pd.read_parquet(
    f"{GRAPHRAG_FOLDER}/create_final_entities.parquet",
    columns=[
        "name", "type", "description", "human_readable_id",
        "id", "description_embedding", "text_unit_ids"
    ]
)

entity_statement = """
MERGE (e:__Entity__ {id: value.id})
SET e.human_readable_id = value.human_readable_id,
    e.description = value.description,
    e.name = replace(value.name, '"', ''),
    e.description_embedding = value.description_embedding
WITH e, value
CALL {
    WITH e, value
    FOREACH (ignore IN CASE WHEN value.type IS NOT NULL AND value.type <> '' THEN [1] ELSE [] END |
        SET e:__Entity__:`replace(value.type, '"', '')`)
}
WITH e, value
UNWIND value.text_unit_ids AS text_unit_id
MATCH (c:__Chunk__ {id: text_unit_id})
MERGE (c)-[:HAS_ENTITY]->(e)
"""

batched_import(entity_statement, entity_df)

# Import relationships
print("Importing relationships...")
rel_df = pd.read_parquet(
    f"{GRAPHRAG_FOLDER}/create_final_relationships.parquet",
    columns=[
        "source", "target", "id", "rank", "weight",
        "human_readable_id", "description", "text_unit_ids"
    ]
)

rel_statement = """
MATCH (source:__Entity__ {name: replace(value.source, '"', '')})
MATCH (target:__Entity__ {name: replace(value.target, '"', '')})
MERGE (source)-[rel:RELATED {id: value.id}]->(target)
SET rel.rank = value.rank,
    rel.weight = value.weight,
    rel.human_readable_id = value.human_readable_id,
    rel.description = value.description,
    rel.text_unit_ids = value.text_unit_ids
"""

batched_import(rel_statement, rel_df)

# Import communities
print("Importing communities...")
community_df = pd.read_parquet(
    f"{GRAPHRAG_FOLDER}/create_final_communities.parquet",
    columns=["id", "level", "title", "text_unit_ids", "relationship_ids"]
)

community_statement = """
MERGE (c:__Community__ {community: value.id})
SET c.level = value.level,
    c.title = value.title
WITH c, value
UNWIND value.relationship_ids as rel_id
MATCH (start:__Entity__)-[r:RELATED {id: rel_id}]->(end:__Entity__)
MERGE (start)-[:IN_COMMUNITY]->(c)
MERGE (end)-[:IN_COMMUNITY]->(c)
"""

batched_import(community_statement, community_df)

# Import community reports
print("Importing community reports...")
community_report_df = pd.read_parquet(
    f"{GRAPHRAG_FOLDER}/create_final_community_reports.parquet",
    columns=[
        "id", "community", "level", "title", "summary",
        "findings", "rank", "rank_explanation", "full_content"
    ]
)

community_report_statement = """
MERGE (c:__Community__ {community: value.community})
SET c.level = value.level,
    c.title = value.title,
    c.rank = value.rank,
    c.rank_explanation = value.rank_explanation,
    c.full_content = value.full_content,
    c.summary = value.summary
WITH c, value
UNWIND range(0, size(value.findings)-1) AS finding_idx
WITH c, finding_idx, value.findings[finding_idx] as finding
MERGE (f:Finding {id: finding_idx})
SET f += finding
MERGE (c)-[:HAS_FINDING]->(f)
"""

batched_import(community_report_statement, community_report_df)

# Close connection
print("Import complete. Closing connection...")
# memgraph.close()
