import psycopg
from sentence_transformers import SentenceTransformer
import requests

class InformationRetrieval:
    def __init__(self):
        self.session = requests.Session()
        self.api_base_url = "http://localhost:5000"

        # try different models
        self.model = SentenceTransformer('sentence-transformers/LaBSE')

    # API helper methods for better caching and connection handling
    def _get_api(self, endpoint, params=None):
        """Make a GET request to the API with built-in retry and caching."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.get(url, params=params)
    
    #query using L1 distance
    def query_db_L1(self, query):
        """
        The query_db_L1 function retrieves the top 5 most similar sentences from a pgvector database based on L1 (Manhattan) distance. 
        It uses a pre-trained SentenceTransformer model to encode the input query and then searches for the closest embeddings stored in the database.
        """       

        #calculate embedding for the query
        query_embedding = self.model.encode(query).tolist()  

        # TODO: Following the logic below add an API call in main.py
        # execute the query to fetch the top 5 most similar sentences based on L1 distance
        result = cur.execute(
            'SELECT sentence, (embedding <+> %s::vector) AS distance '
            'FROM ' + PageSegment + ' '
            'ORDER BY embedding <+> %s::vector '
            'LIMIT 5',
            (query_embedding, query_embedding)  # pass the embedding twice, once for ordering and once for calculation
        ).fetchall()
        cur.close()
        conn.close()
        return result

    #query using cosine distance
    def query_db_cosine(self, query):
        """
        The query_db_cosine function retrieves the top 5 most similar sentences from a pgvector database based on cosine distance. 
        It uses a pre-trained SentenceTransformer model to encode the input query and then searches for the closest embeddings stored in the database.
        """  

        #calculate embedding for the query
        query_embedding = self.model.encode(query).tolist()  

        # TODO: Following the logic below add an API call in main.py
        # execute the query to fetch the top 5 most similar sentences based on cosine distance
        result = cur.execute(
            'SELECT sentence, 1 - (embedding <=> %s::vector) AS distance '
            'FROM ' + PageSegment + ' '
            'ORDER BY embedding <=> %s::vector '
            'LIMIT 5',
            (query_embedding, query_embedding)  # pass the embedding twice, once for ordering and once for calculation
        ).fetchall()
        cur.close()
        conn.close()
        return result

    #query using negative inner product
    def query_db_inner(self, query):
        """
        The query_db_inner function retrieves the top 5 most similar sentences from a pgvector database based on (negative) inner product. 
        It uses a pre-trained SentenceTransformer model to encode the input query and then searches for the closest embeddings stored in the database.
        """

        #calculate embedding for the query
        query_embedding = self.model.encode(query).tolist()  

        # TODO: Following the logic below add an API call in main.py
        # execute the query to fetch the top 5 most similar sentences based negative inner product
        result = cur.execute(
            'SELECT sentence, -(embedding <#> %s::vector) AS distance '
            'FROM ' + PageSegment + ' '
            'ORDER BY embedding <#> %s::vector '
            'LIMIT 5',
            (query_embedding, query_embedding)  # pass the embedding twice, once for ordering and once for calculation
        ).fetchall()
        cur.close()
        conn.close()
        return result

if __name__ == "__main__":
    retrieval = InformationRetrieval()
    query = input("Prosim, vnesite poizvedbo: ").strip()
    if not query:
        print("Poizvedba je prazna, konec.")
        exit(0)
    # Test
    for text, score in retrieval.query_db_cosine(query):
        print(f"[{score:.4f}] {text}")