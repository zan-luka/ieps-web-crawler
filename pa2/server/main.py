import logging
import os
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import models
from datetime import datetime
import locale
from datetime import datetime, timedelta
import re
from sentence_transformers import SentenceTransformer
from sqlalchemy import text

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://avnadmin:AVNS_grJJWdHWiDIwlvllK50@pg-381112ff-web-crawler.g.aivencloud.com:15049/defaultdb?sslmode=require"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine))
locale.setlocale(locale.LC_TIME, 'sl_SI.UTF-8')

app = Flask(__name__)

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "maj": 5, "jun": 6,
    "jul": 7, "avg": 8, "sep": 9, "okt": 10, "nov": 11, "dec": 12
}

def convert_time_to_timestamp(time_str):
    """Converts Slovenian time strings to a datetime."""
    now = datetime.now()
    time_str = time_str.strip().lower()

    # danes ob HH:MM
    m = re.match(r"danes ob (\d{1,2}):(\d{2})", time_str)
    if m:
        hh, mm = map(int, m.groups())
        return now.replace(hour=hh, minute=mm, second=0, microsecond=0)

    # včeraj ob HH:MM
    m = re.match(r"včeraj ob (\d{1,2}):(\d{2})", time_str)
    if m:
        hh, mm = map(int, m.groups())
        dt = now - timedelta(days=1)
        return dt.replace(hour=hh, minute=mm, second=0, microsecond=0)

    # DD. mon YYYY ob HH:MM
    m = re.match(r"(\d{1,2})\.\s*([a-zčšž]+)\s+(\d{4})\s+ob\s+(\d{1,2}):(\d{2})", time_str)
    if m:
        day, mon, year, hh, mm = m.groups()
        mon = mon.rstrip(".")  # v primeru da ima pika
        month = MONTH_MAP.get(mon)
        if month:
            return datetime(
                year=int(year),
                month=month,
                day=int(day),
                hour=int(hh),
                minute=int(mm)
            )

    logger.error(f"Ne morem razčleniti časa: {time_str}")
    return None

@app.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Flask is working"})

@app.route("/query", methods=["POST"])
def query():
    """Handles a query, retrieves relevant results based on embedding similarity."""
    db = SessionLocal()
    try:
        data = request.get_json()
        query_text = data.get("query")
        metric = data.get("metric", "cosine").lower()
        model_choice = data.get("model", "sloberta").lower()
        limit = int(data.get("limit", 5))  # Default to 5 results

        if not query_text:
            return jsonify({"error": "Query text is required."}), 400

        # === Generate Embedding ===
        if model_choice == "labse":
            from sentence_transformers import SentenceTransformer
            labse_model = SentenceTransformer("sentence-transformers/LaBSE")
            query_embedding = labse_model.encode(query_text, convert_to_numpy=True)
            logger.info("Using LaBSE model for embedding.")
        else:
            from transformers import AutoTokenizer, AutoModel
            import torch
            tokenizer = AutoTokenizer.from_pretrained("EMBEDDIA/sloberta")
            model = AutoModel.from_pretrained("EMBEDDIA/sloberta")
            inputs = tokenizer(query_text, return_tensors="pt", padding=True, truncation=True, max_length=512)
            with torch.no_grad():
                outputs = model(**inputs)
            attention_mask = inputs['attention_mask']
            token_embeddings = outputs.last_hidden_state
            input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
            sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
            sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
            query_embedding = (sum_embeddings / sum_mask).squeeze().numpy()
            logger.info("Using SloBERTa model for embedding.")

        # Format the vector for pgvector
        vector_string = f"[{', '.join(str(float(val)) for val in query_embedding)}]"

        # === SQL Query Based on Metric ===
        if metric == "cosine":
            sql = text("""
                WITH similarities AS (
                    SELECT ps.page_id, ps.page_segment, ps.segment_type, ps.title, 
                           p.url, 1 - (embedding <=> :vector) AS similarity
                    FROM crawldb.page_segment ps
                    JOIN crawldb.page p ON ps.page_id = p.id
                )
                SELECT * FROM similarities
                ORDER BY similarity DESC
                LIMIT :limit
            """)
        elif metric == "l1":
            sql = text("""
                WITH distances AS (
                    SELECT ps.page_id, ps.page_segment, ps.segment_type, ps.title, 
                           p.url, (embedding <+> :vector) AS distance
                    FROM crawldb.page_segment ps
                    JOIN crawldb.page p ON ps.page_id = p.id
                )
                SELECT * FROM distances
                ORDER BY distance ASC
                LIMIT :limit
            """)
        elif metric == "inner":
            sql = text("""
                WITH distances AS (
                    SELECT ps.page_id, ps.page_segment, ps.segment_type, ps.title, 
                           p.url, -(embedding <#> :vector) AS distance
                    FROM crawldb.page_segment ps
                    JOIN crawldb.page p ON ps.page_id = p.id
                )
                SELECT * FROM distances
                ORDER BY distance DESC
                LIMIT :limit
            """)
        else:
            return jsonify({"error": f"Unsupported metric: {metric}"}), 400

        # Execute safely with parameters
        result = db.execute(sql, {"vector": vector_string, "limit": limit}).fetchall()

        # Format results
        formatted_results = []
        for row in result:
            result_entry = {
                "page_id": row[0],
                "segment": row[1],
                "segment_type": row[2],
                "title": row[3],
                "url": row[4]
            }
            if metric == "cosine":
                result_entry["similarity"] = float(row[5])
            else:
                result_entry["distance"] = float(row[5])
            formatted_results.append(result_entry)

        logger.info(f"Query returned {len(formatted_results)} results using model '{model_choice}' and metric '{metric}'")

        return jsonify({"results": formatted_results})

    except Exception as e:
        logger.error(f"Error in /query: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/pages/html", methods=["GET"])
def get_all_html_pages():
    db = SessionLocal()
    try:
        result = db.query(models.Page).filter(
            models.Page.page_type_code == 'HTML',
            models.Page.html_content.isnot(None),
            models.Page.http_status_code == 200
        )   # Limit set for testing

        pages = [
            {
                "id": page.id,
                "url": page.url,
                "html_content": page.html_content
            }
            for page in result
        ]

        return jsonify(pages)
    except Exception as e:
        logger.error(f"Error in /pages/html: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/page/update", methods=["PUT"])
def update_page():
    db = SessionLocal()
    try:
        data = request.get_json()
        page_id = data.get("page_id")
        cleaned_html = data.get("cleaned_html")
        news_data = data.get("news_data", None)
        chunks_with_embeddings = data.get("chunks", [])
        title = None
        category = None
        published_time = None

        # Parse metadata from news_data
        if news_data:
            title = news_data.get("title") if news_data.get("title") else None
            category = news_data.get("category") if news_data.get("category") else None
            time_str = news_data.get("time")
            published_time = convert_time_to_timestamp(time_str) if time_str else None

        # Update the Page table
        page = db.query(models.Page).filter(models.Page.id == page_id).first()
        if not page:
            return jsonify({"error": "Page not found"}), 404
        
        page.cleaned_content = cleaned_html
        db.commit()

        # DELETE existing page segments before inserting new ones
        db.query(models.PageSegment).filter(models.PageSegment.page_id == page_id).delete()
        db.commit()

        seen_texts = set()

        for chunk in chunks_with_embeddings:
            chunk_text = chunk.get("text", "").strip()
            if not chunk_text or chunk_text in seen_texts:
                continue  # Skip duplicates or empty

            seen_texts.add(chunk_text)

            segment_data = {
                "page_id": page_id,
                "page_segment": chunk_text,
                "segment_type": chunk.get("segment_type"),
                "embedding": chunk.get("embedding")
            }

            if title:
                segment_data["title"] = title
            if category:
                segment_data["category"] = category
            if published_time:
                segment_data["time"] = published_time

            page_segment = models.PageSegment(**segment_data)
            db.add(page_segment)

        db.commit()
        return jsonify({"message": "Page and segments updated successfully"}), 200
    except Exception as e:
        logger.error(f"Error in /page/update: {e}")
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


if __name__ == "__main__":
    app.run(debug=True)
