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
    
@app.route("/pages/html", methods=["GET"])
def get_all_html_pages():
    db = SessionLocal()
    try:
        result = db.query(models.Page).filter(
            models.Page.page_type_code == 'HTML',
            models.Page.html_content.isnot(None),
            models.Page.http_status_code == 200
        ).limit(1).all()      # Limit set for testing

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

        # Only set these values if news_data is not None
        if news_data:
            title = news_data.get("title") if news_data.get("title") else None
            category = news_data.get("category") if news_data.get("category") else None
            time_str = news_data.get("time")
            
            # Convert time to timestamp if it is provided
            published_time = convert_time_to_timestamp(time_str) if time_str else None

        # Update the Page table
        page = db.query(models.Page).filter(models.Page.id == page_id).first()
        if not page:
            return jsonify({"error": "Page not found"}), 404
        
        page.cleaned_content = cleaned_html
        db.commit()

        # Add corresponding PageSegment records
        for chunk in chunks_with_embeddings:
            segment_data = {
                "page_id": page_id,
                "page_segment": chunk.get("text"),
                "segment_type": chunk.get("segment_type"),
                "embedding": chunk.get("embedding")
            }

            # Only add title, category, and time if they are not None or empty
            if title:
                segment_data["title"] = title
            if category:
                segment_data["category"] = category
            if published_time:
                segment_data["time"] = published_time

            logger.info(f"Time is: {time_str}")
            logger.info(f"Time2 is: {published_time}")
            #logger.warning(f"Adding segment: {segment_data}")
            page_segment = models.PageSegment(**segment_data)
            db.add(page_segment)

        db.commit()
        return jsonify({"message": "Page and segments updated successfully"}), 200
    except Exception as e:
        logger.error(f"Error in /pages/update: {e}")
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

if __name__ == "__main__":
    app.run(debug=True)