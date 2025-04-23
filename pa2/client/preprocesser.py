import re
from bs4 import BeautifulSoup
import nltk
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer
import requests

class PageProcessor:
    def __init__(self):
        self.session = requests.Session()
        self.api_base_url = "http://localhost:5000"
        # Download NLTK sentence tokenizer if not already installed
        #nltk.download("punkt")
        #nltk.download("punkt_tab")

        # try different models
        self.model = SentenceTransformer('sentence-transformers/LaBSE')


    # API helper methods for better caching and connection handling
    def _get_api(self, endpoint, params=None):
        """Make a GET request to the API with built-in retry and caching."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.get(url, params=params)
    
    def fix_html_structure(self, html_content):
        html_content = html_content.replace('""', '"')
        soup = BeautifulSoup(html_content, 'html.parser')

        tags_to_remove = ['aside', 'footer', 'nav']
        for tag in tags_to_remove:
            for element in soup.find_all(tag):
                element.decompose()

        for header in soup.find_all('header'):
            if not header.find_parent(id='content'):
                header.decompose()

        classes_to_remove = ['thread_nav', 'forums']
        for class_name in classes_to_remove:
            for element in soup.find_all(class_=lambda x: x and class_name in x):
                element.decompose()

        for element in soup.find_all("div", id="menus"):
            element.decompose()

        for br in soup.find_all('br'):
            br.decompose()
        return soup

    def remove_whitespaces(self, text):
        return re.sub(r'\s+', ' ', text).strip()

    def extract_news_data(self, soup):
        news = soup.find('div', class_='news_item')

        if not news:
            return None 
    
        title = self.remove_whitespaces(news.find('h3').get_text())
        time = self.remove_whitespaces(news.find('time').get_text())
        category = self.remove_whitespaces(news.find('li', class_='categories').get_text())      
        for element in news.find_all(['img']):
            element.decompose()

        for a_tag in news.find_all(['a', 'em']):
            a_tag.unwrap()

        article_div = news.find('div', class_='besediloNovice')
        article_text = ""
        if article_div:
            for img_block in article_div.find_all(attrs={"itemprop": "image"}):
                img_block.decompose()
            article_text = self.remove_whitespaces(article_div.get_text())

        print(article_text, title, category, time)
        return { "title": title, "time": time, "category": category, "article": article_text}

    def extract_comments(self, soup):
        comments = soup.find_all('div', class_='content')
        cleaned_comments = []

        # remove unwanted elements
        for comment in comments:
            for element in comment.find_all(['blockquote', 'time', 'p', 'img', 'div']):
                element.decompose()

            for a_tag in comment.find_all(['a', 'em']):
                a_tag.unwrap()

            text = self.remove_whitespaces(comment.get_text())
            if text:
                cleaned_comments.append(text)

        return cleaned_comments

    def chunk_fixed_length(self, text, chunk_size=50):
        """Fixed length chunking."""
        return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]


    def chunk_segments(self, text, max_words=256):
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


    def calculate_embeddings(self, chunks):
        global model
        embeddings = []
        for chunk in chunks:
            embedding = model.encode(chunk).tolist()
            embeddings.append(embedding)
        
        return embeddings


    def query_db_cosine(self, query, table_name):
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

    def get_htmls(self):
        response = self._get_api("/pages/html")
        if response.status_code == 200:
            html_pages = response.json()
            page_data = []
            for page in html_pages:
                page_id = page['id']
                html_content = page['html_content']
                fixed_html = self.fix_html_structure(html_content)
                news_data = self.extract_news_data(fixed_html)
                #comments_data = self.extract_comments(fixed_html)
                if (news_data is None):
                    print("none")
                page_data.append(news_data)
            print(page_data)
        else:
            print("Failed to fetch pages:", response.status_code, response.text)
    
if __name__ == "__main__":
    processor = PageProcessor()
    processor.get_htmls()

#if __name__ == "__main__":
    #with open("../test.html", "r", encoding="utf-8") as file:
        #html_content = file.read()

    #soup = fix_html_structure(html_content)
    #extract_news_data(soup)
    #extract_comments(soup)

