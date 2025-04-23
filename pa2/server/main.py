import logging
import os
from flask import Flask, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import models

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

app = Flask(__name__)

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
        ).limit(100).all()      # Limit set to 100 pages for testing

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

if __name__ == "__main__":
    app.run(debug=True)