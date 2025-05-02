import os
import re
import time
from xml.etree.ElementTree import fromstring
from bs4 import BeautifulSoup
import nltk
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer
import requests
from lxml import html, etree
import torch
from transformers import AutoTokenizer, AutoModel

class PageProcessor:
    def __init__(self):
        self.session = requests.Session()
        self.api_base_url = "http://localhost:5000"
        
        # Load embedding models
        self.tokenizer = AutoTokenizer.from_pretrained("EMBEDDIA/sloberta")
        self.sloberta_model = AutoModel.from_pretrained("EMBEDDIA/sloberta")
        
        self.local_model = SentenceTransformer('sentence-transformers/LaBSE')

    def _get_api(self, endpoint, params=None):
        """Make a GET request to the API with built-in retry and caching."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.get(url, params=params)
    
    def _put_api(self, endpoint, json=None):
        """Make a PUT request to the API with built-in retry."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.put(url, json=json)
    
    def fix_html_structure(self, html_content):
        """Clean and simplify HTML using BeautifulSoup by isolating main content"""
        html_content = html_content.replace('""', '"')
        soup = BeautifulSoup(html_content, 'html.parser')

        main_content = soup.find(id='content')
        if not main_content:
            return soup

        tags_to_remove = ['script', 'style', 'iframe', 'nav', 'aside', 'form']
        for tag in tags_to_remove:
            for element in main_content.find_all(tag):
                element.decompose()

        classes_to_remove = ['thread_nav', 'forums', 'sidebar', 'comments-nav', 'search', 'signature']
        for class_name in classes_to_remove:
            for element in main_content.find_all(class_=lambda x: x and class_name in x):
                element.decompose()

        # Remove any nested headers/footers not directly related to the article
        # Only remove footer, not header (header contains metadata we want)
        for tag in main_content.find_all(['footer']):
            tag.decompose()

        return main_content


    def extract_article_data_from_clean_html(self, clean_html):
        html_str = str(clean_html)
        tree = html.fromstring(html_str)

        title = tree.xpath('//h1[@class="current"]/text()')
        title = title[0].strip() if title else None

        author_match = re.search(r'<span itemprop="name">\s*(.*?)\s*</span>', html_str)
        author = author_match.group(1).strip() if author_match else None

        date_match = re.search(r'<time datetime="([^"]+)"', html_str)
        date = date_match.group(1) if date_match else None

        category = tree.xpath('//li[contains(@class, "categories")]/a/text()')
        category = category[0].strip() if category else None

        body_parts = tree.xpath('//div[@class="besediloNovice"]//text()')
        body = ' '.join(p.strip() for p in body_parts if p.strip())

        return {
            'type': 'article',
            'title': title,
            'author': author,
            'date': date,
            'category': category,
            'content': body
        }
        
    def extract_comments(self, clean_html):
        """Extracts user comments from Slo-Tech cleaned HTML"""
        tree = html.fromstring(str(clean_html))

        comments = []
        post_elements = tree.xpath('//div[contains(@class, "post")]')

        for post in post_elements:
            author = post.xpath('.//h4/a/text()')
            author = author[0].strip() if author else None

            datetime = post.xpath('.//time/@datetime')
            datetime = datetime[0] if datetime else None

            content_blocks = post.xpath('.//div[@class="content"]/text() | .//div[@class="content"]/*[not(self::p[@class="user-quoted"] or self::blockquote)]/text()')
            content = ' '.join([text.strip() for text in content_blocks if text.strip()])

            comments.append({
                'author': author,
                'datetime': datetime,
                'content': content
            })

        return comments


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
            for br in article_div.find_all('br'):
                br.replace_with(' NEWLINE ')
            raw_text = self.remove_whitespaces(article_div.get_text())
            article_text = raw_text.replace('NEWLINE', '\n').replace('\n \n', '\n')

        return { "title": title, "time": time, "category": category, "article": article_text}

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

    def chunk_by_paragraphs(self, text, max_words=256):
        paragraphs = [paragraph.strip() for paragraph in text.split('\n') if paragraph.strip()]
        chunks = []

        for paragraph in paragraphs:
            words = paragraph.split()
            if len(words) <= max_words:
                chunks.append(paragraph)
            else:
                smaller_chunks = self.chunk_segments(paragraph, max_words=max_words)
                chunks.extend(smaller_chunks)
        return chunks

    def get_chunks(self, news_data, comments_data):
        """Extract meaningful segments with enhanced context preservation"""
        chunks = []
        
        title = news_data.get("title", "")
        content = news_data.get("content", "")
        
        if content:
            if title:
                chunks.append({
                    "text": title,
                    "segment_type": "title",
                    "section": "title",
                    "title": title
                })
            
            paragraphs = content.split('\n')
            
            # Group paragraphs that are too short to maintain context
            current_chunk = ""
            current_chunk_length = 0
            min_chunk_length = 50  # Minimum characters for a reasonable chunk
            max_chunk_length = 500  # Maximum characters to avoid too large chunks
            
            for i, para in enumerate(paragraphs):
                para = para.strip()
                if not para:
                    continue
                    
                # If adding this paragraph would make the chunk too large, store current chunk and start new one
                if current_chunk_length + len(para) > max_chunk_length and current_chunk_length >= min_chunk_length:
                    # Add title context to the chunk to improve embedding context
                    chunk_text = current_chunk
                    if title:
                        chunk_text = f"From article: {title}. {chunk_text}"
                        
                    chunks.append({
                        "text": chunk_text,
                        "segment_type": "paragraph",
                        "section": f"content_{len(chunks)}",
                        "title": title
                    })
                    current_chunk = para
                    current_chunk_length = len(para)
                else:
                    if current_chunk:
                        current_chunk += " " + para
                    else:
                        current_chunk = para
                    current_chunk_length += len(para)
            
            if current_chunk and current_chunk_length >= min_chunk_length:
                chunk_text = current_chunk
                if title:
                    chunk_text = f"From article: {title}. {chunk_text}"
                    
                chunks.append({
                    "text": chunk_text,
                    "segment_type": "paragraph",
                    "section": f"content_{len(chunks)}",
                    "title": title
                })
        
        if comments_data:
            if isinstance(comments_data, list):
                for i, comment in enumerate(comments_data):
                    if isinstance(comment, dict):
                        comment_text = comment.get("text", "")
                        author = comment.get("author", "")
                        date = comment.get("date", "")
                    else:
                        comment_text = str(comment)
                        author = ""
                        date = ""
                    
                    if comment_text and comment_text.strip() != "":
                        enhanced_text = comment_text
                        if title:
                            enhanced_text = f"Comment on: {title}. {enhanced_text}"
                        
                        chunks.append({
                            "text": enhanced_text,
                            "segment_type": "comment",
                            "section": f"comment_{i}",
                            "title": title
                        })
            elif isinstance(comments_data, str) and comments_data.strip():
                chunks.append({
                    "text": f"Comment on: {title}. {comments_data}",
                    "segment_type": "comment",
                    "section": "comment_single",
                    "title": title
                })
        
        return chunks

    def get_embedding(self, text, model_name="LaBSE"):
        """Get embedding using LaBSE (or fallback to SloBERTa if needed)"""
        if not text or text.strip() == "":
            return [0.0] * 768 

        try:
            if model_name == "LaBSE":
                return self.local_model.encode(text, convert_to_numpy=True).tolist()
            else:
                inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
                with torch.no_grad():
                    outputs = self.sloberta_model(**inputs)
                attention_mask = inputs['attention_mask']
                token_embeddings = outputs.last_hidden_state
                input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
                sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
                sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
                mean_embedding = (sum_embeddings / sum_mask).squeeze().numpy()
                return mean_embedding.tolist()
        except Exception as e:
            print(f"Embedding error: {e}")
            return [0.0] * 768

    def calculate_embeddings_batch(self, chunks, batch_size=20, model_name="LaBSE"):
        """Calculate embeddings for chunks in batches using LaBSE"""
        processed_chunks = []

        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            texts = [chunk["text"] for chunk in batch]

            try:
                if model_name == "LaBSE":
                    embeddings = self.local_model.encode(texts, convert_to_numpy=True)
                else:
                    embeddings = [self.get_embedding(text, model_name="sloberta") for text in texts]

                for chunk, emb in zip(batch, embeddings):
                    chunk["embedding"] = emb.tolist() if hasattr(emb, "tolist") else emb

                processed_chunks.extend(batch)

            except Exception as e:
                print(f"Batch error {i}-{i+batch_size}: {e}")
                for chunk in batch:
                    chunk["embedding"] = self.get_embedding(chunk["text"], model_name=model_name)
                    processed_chunks.append(chunk)

        return processed_chunks


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
            for page in html_pages:
                page_id = page['id']
                html_content = page['html_content']

                # Clean and extract structured content
                fixed_html = self.fix_html_structure(html_content)
                news_data = self.extract_article_data_from_clean_html(fixed_html)
                comments = self.extract_comments(fixed_html)

                chunks = self.get_chunks(news_data, comments)

                # Compute embeddings using LaBSE (or sloberta optionally)
                chunks_with_embeddings = self.calculate_embeddings_batch(chunks, model_name="LaBSE")

                if chunks_with_embeddings:
                    try:
                        self._put_api("/page/update", json={
                            "page_id": page_id,
                            "cleaned_html": self.remove_whitespaces(fixed_html.get_text()),
                            "news_data": news_data,
                            "chunks": chunks_with_embeddings
                        })
                    except Exception as e:
                        print(f"[ERROR] Failed to update DB for page {page_id}: {e}")
        else:
            print("Failed to fetch pages:", response.status_code, response.text)


if __name__ == "__main__":
    processor = PageProcessor()
    processor.get_htmls()
