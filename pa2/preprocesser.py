import re
from bs4 import BeautifulSoup
import nltk
import textwrap
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer


# Download NLTK sentence tokenizer if not already installed
nltk.download("punkt")
nltk.download("punkt_tab")

# try different models
model = SentenceTransformer('sentence-transformers/LaBSE')

conn = psycopg.connect(host="localhost", dbname='wier', autocommit=True, password='SecretPassword', user='user')
cur = conn.cursor() 

def fix_html_structure(html_content):
    html_content = html_content.replace('""', '"')
    soup = BeautifulSoup(html_content, 'html.parser')
    for br in soup.find_all('br'):
        br.decompose()
    return soup

def remove_whitespaces(text):
    return re.sub(r'\s+', ' ', text).strip()

def extract_news_data(soup):
    news_text = soup.find_all('div', class_='news_item')
    for news in news_text:
        title = remove_whitespaces(news.find('h3').get_text())
        time = remove_whitespaces(news.find('time').get_text())
        category = remove_whitespaces(news.find('li', class_='categories').get_text())
        for element in news.find_all(['img']):
            element.decompose()

        for a_tag in news.find_all(['a', 'em']):
            a_tag.unwrap()

        print(news.prettify(), title, category, time)

def extract_comments(soup):
    comments = soup.find_all('div', class_='content')
    # remove unwanted elements
    for comment in comments:
        for element in comment.find_all(['blockquote', 'time', 'p', 'img', 'div']):
            element.decompose()

        for a_tag in comment.find_all(['a', 'em']):
            a_tag.unwrap()

    for comment in comments:
        print(remove_whitespaces(comment.get_text()) + "\n")


def chunk_fixed_length(text, chunk_size=50):
    """Fixed length chunking."""
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]


def chunk_segments(text, max_words=256):
    """Splits text into sentence-based chunks with a max word count limit."""
    sentences = sent_tokenize(text)  # Split into sentences
    chunks, current_chunk = [], []
    current_length = 0
    
    for sentence in sentences:
        words = sentence.split()
        if current_length + len(words) > max_words:
            chunks.append(" ".join(current_chunk))  # Save current chunk
            current_chunk, current_length = [], 0  # Reset chunk
        current_chunk.append(sentence)
        current_length += len(words)
    
    if current_chunk:
        chunks.append(" ".join(current_chunk))  # Add last chunk

    return chunks


def calculate_embeddings(chunks):
    global model
    embeddings = []
    for chunk in chunks:
        embedding = model.encode(chunk).tolist()
        embeddings.append(embedding)
    
    return embeddings


def query_db_cosine(query, table_name):
    """
    The query_db_cosine function retrieves the top 5 most similar sentences from a pgvector database based on cosine distance. 
    It uses a pre-trained SentenceTransformer model to encode the input query and then searches for the closest embeddings stored in the database.

    Parameters
    - query (str): The input text query to be searched.
    - table_name (str): The name of the table containing the stored sentence embeddings. Possible options are showcase.vector_demo and showcase.vector_demo2
    """
    
    #download the model
    global model, conn, cur

    #calculate embedding for the query
    query_embedding = model.encode(query).tolist()  

    # execute the query to fetch the top 5 most similar sentences based on cosine distance
    result = cur.execute(
        'SELECT chunk, 1 - (embedding <=> %s::vector) AS similarity '
        'FROM ' + table_name + ' ORDER BY similarity DESC LIMIT 5',
        (query_embedding,)  # pass the embedding twice, once for ordering and once for calculation
    ).fetchall()
    #cur.close()
    #conn.close()
    return result

    
if __name__ == "__main__":
    with open("../test.html", "r", encoding="utf-8") as file:
        html_content = file.read()

    soup = fix_html_structure(html_content)
    extract_news_data(soup)
    #extract_comments(soup)

