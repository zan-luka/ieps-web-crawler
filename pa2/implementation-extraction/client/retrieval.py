import psycopg
from sentence_transformers import SentenceTransformer
import requests

class InformationRetrieval:
    def __init__(self):
        self.session = requests.Session()
        self.api_base_url = "http://localhost:5000"  # Adjust this URL if necessary
        self.model = SentenceTransformer('sentence-transformers/LaBSE')

    def query_db(self, query, metric="cosine"):
        """
        Sends a request to the server's /query endpoint to retrieve the top 5 most similar sentences
        using the specified distance metric (cosine, L1, or inner).
        """
        data = {
            "query": query,
            "metric": metric
        }

        # Send POST request to /query endpoint
        response = self.session.post(f"{self.api_base_url}/query", json=data)

        if response.status_code == 200:
            return response.json()  # Returns the JSON response with the query results
        else:
            print(f"Error querying the database: {response.text}")
            return None

    def query_db_L1(self, query):
        """
        Retrieves the top 5 most similar sentences based on L1 (Manhattan) distance using the server's /query endpoint.
        """
        return self.query_db(query, metric="L1")

    def query_db_cosine(self, query):
        """
        Retrieves the top 5 most similar sentences based on cosine distance using the server's /query endpoint.
        """
        return self.query_db(query, metric="cosine")

    def query_db_inner(self, query):
        """
        Retrieves the top 5 most similar sentences based on negative inner product using the server's /query endpoint.
        """
        return self.query_db(query, metric="inner")

if __name__ == "__main__":
    retrieval = InformationRetrieval()
    query = input("Prosim, vnesite poizvedbo: ").strip()
    if not query:
        print("Poizvedba je prazna, konec.")
        exit(0)
    # Test
    for text, score in retrieval.query_db_cosine(query):
        print(f"[{score:.4f}] {text}")