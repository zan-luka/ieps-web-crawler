import datetime
import logging
import os
from urllib.parse import urlparse

from flask import Flask, request, jsonify
from sqlalchemy import text, select, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker, scoped_session

import models  # assuming models.py is in the server folder
from schemas import SiteCreate, PageFrontier, PageCreate, PageDataCreate, ImageCreate, LinkCreate, DelayData

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://neondb_owner:npg_CurQ5oWtSTY3@ep-solitary-resonance-a9v88dmb-pooler.gwc.azure.neon.tech/neondb"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine))

app = Flask(__name__)

@app.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Flask is working"})

@app.route("/site", methods=["POST"])
def create_site():
    db = SessionLocal()
    try:
        data = request.get_json()
        site_data = SiteCreate(**data)
        new_site = models.Site(
            domain=site_data.domain,
            robots_content=site_data.robots_content,
            sitemap_content=site_data.sitemap_content,
        )
        db.add(new_site)
        db.commit()
        return jsonify({"id": new_site.id, "domain": new_site.domain})
    except Exception as e:
        db.rollback()
        logger.error(f'Error /site: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/site/delay", methods=["POST"])
def get_delay():
    db = SessionLocal()
    try:
        data = request.get_json()
        delay_data = DelayData(**data)
        print(delay_data)
        result = db.execute(
            text("""select p.accessed_time from crawldb.site s
                    inner join crawldb.page p on p.site_id = s.id
                    where p.accessed_ip = :ip
                    and s.domain like '%slo-tech.com%'
                    order by p.accessed_time desc
                    limit 1;"""),
            {"ip": delay_data.ip, "domain": delay_data.site_url}
        )
        last_accessed_time = result.scalar_one_or_none()
        if last_accessed_time:
            time_diff = (datetime.datetime.now() - last_accessed_time).total_seconds()
            if time_diff < 5:
                return jsonify({"delay": 5 - time_diff})
            else:
                return jsonify({"delay": 0})
        return jsonify({"delay": 0})
    except Exception as e:
        logger.error(f'Error /site/delay: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/page/frontier", methods=["POST"])
def create_page_frontier():
    db = SessionLocal()
    try:
        data = request.get_json()
        #result = db.execute(
        #    text("SELECT id FROM crawldb.site WHERE domain = :domain LIMIT 1"),
        #    {"domain": data["site_url"]}
        #)
        site_id = 3
        if site_id is None:
            return jsonify({"error": "Site not found"}), 404

        new_page = models.Page(
            site_id=site_id,
            page_type_code='FRONTIER',
            url=data["url"]
        )
        db.add(new_page)
        db.commit()
        return jsonify({"id": new_page.id, "url": new_page.url})
    except Exception as e:
        db.rollback()
        logger.error(f'Error /page/frontier: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

def get_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

@app.route("/page/frontierlinks", methods=["POST"])
def create_pages_frontier():
    db = SessionLocal()
    try:
        data = request.get_json()

        links = data["links"]
        # Prepare batch insert into Page table
        chunk_size = 1000
        for i in range(0, len(links), chunk_size):
            chunk = links[i: i + chunk_size]
            stmt = insert(models.Page).values([
                {
                    "site_id": None,
                    "page_type_code": "FRONTIER",
                    "url": link["url"],
                    "relevance": link["relevance"],
                }
                for link in chunk
            ]).on_conflict_do_nothing(index_elements=["url"])
            db.execute(stmt)

        db.commit()
        # Get ID-s for inserted pages
        from_page_id = data.get("from_page_id")

        urls = [link["url"] for link in links]
        pages = db.execute(
            select(models.Page.id, models.Page.url).where(models.Page.url.in_(urls))
        ).fetchall()

        url_to_id = {page.url: page.id for page in pages}

        # Insert into Link table
        if from_page_id is not None:
            link_stmt = insert(models.Link).values([
                {"from_page": from_page_id, "to_page": url_to_id[link["url"]]}
                for link in links
                if link["url"] in url_to_id
            ]).on_conflict_do_nothing(index_elements=["from_page", "to_page"])
            db.execute(link_stmt)

        db.commit()
        return jsonify({"status": "success", "inserted": len(data["links"])})

    except Exception as e:
        db.rollback()
        logger.error(f'Error /page/frontier: {e}')
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        db.close()

@app.route("/page/<int:page_id>", methods=["PUT"])
def update_page(page_id):
    db = SessionLocal()
    try:
        data = request.get_json()
        existing_page = db.query(models.Page).filter(models.Page.id == page_id).first()
        if not existing_page:
            return jsonify({"error": "Page not found"}), 404

        existing_page.page_type_code = data["page_type_code"]
        existing_page.http_status_code = data["http_status_code"]
        existing_page.accessed_time = datetime.datetime.now()
        existing_page.accessed_ip = data["accessed_ip"]

        if "site_id" in data:
            existing_page.site_id = data["site_id"]

        if data["page_type_code"] == 'HTML':
            existing_page.html_content = data["html_content"]
        else:
            existing_page.html_content = None

        existing_page.content_hash = data["content_hash"]  

        db.commit()
        return jsonify({"id": existing_page.id, "url": existing_page.url})
    except Exception as e:
        db.rollback()
        logger.error(f'Error /page/{page_id}: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/pagedata", methods=["POST"])
def create_page_data():
    db = SessionLocal()
    try:
        data = request.get_json()
        new_pagedata = models.PageData(
            page_id=data["page_id"],
            data_type_code=data["data_type_code"],
            data=data["data"]
        )
        db.add(new_pagedata)
        db.commit()
        return jsonify({"id": new_pagedata.id})
    except Exception as e:
        db.rollback()
        logger.error(f'Error /pagedata: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/image", methods=["POST"])
def create_image():
    db = SessionLocal()
    try:
        data = request.get_json()
        new_image = models.Image(
            page_id=data["page_id"],
            filename=data["filename"],
            content_type=data["content_type"],
            data=data["data"],
            accessed_time=data["accessed_time"],
        )
        db.add(new_image)
        db.commit()
        return jsonify({"id": new_image.id, "filename": new_image.filename})
    except Exception as e:
        db.rollback()
        logger.error(f'Error /image: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/link", methods=["POST"])
def create_link():
    db = SessionLocal()
    try:
        data = request.get_json()
        new_link = models.Link(
            from_page=data["from_page"],
            to_page=data["to_page"]
        )
        db.add(new_link)
        db.commit()
        return jsonify({"from_page": new_link.from_page, "to_page": new_link.to_page})
    except Exception as e:
        db.rollback()
        logger.error(f'Error /link: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/frontier", methods=["GET"])
def list_frontier_urls():
    db = SessionLocal()
    try:
        results = db.execute(
            text("""
                        SELECT id, url 
                        FROM crawldb.page 
                        WHERE page_type_code = 'FRONTIER' 
                        ORDER BY relevance DESC 
                        LIMIT 1 
                        FOR UPDATE SKIP LOCKED
                    """)
        ).fetchone()

        selected_row_id = results.id
        selected_row_url = results.url

        # update page type to "CRAWLED" and set accessed_time to current time
        try:
            db.execute(
                text("UPDATE crawldb.page SET page_type_code = 'CRAWLING', accessed_time = :accessed_time  WHERE id = :id"),
                {"id": selected_row_id, "accessed_time": datetime.datetime.now()}
            )
            db.commit()
        except:
            db.rollback()
            return jsonify({"error": "Could not update page type"}), 500

        return jsonify({"id": selected_row_id, "url": selected_row_url})
    except Exception as e:
        logger.error(f'Error /frontier-urls: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/page/html-count", methods=["GET"])
def count_html_pages():
    db = SessionLocal()
    try:
        result = db.execute(
            text("SELECT COUNT(*) FROM crawldb.page WHERE page_type_code = 'HTML';")
        )
        count = result.scalar()
        return jsonify({"html_page_count": count})
    except Exception as e:
        logger.error(f'Error /page/html-count: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/site/exists", methods=["GET"])
def site_exists():
    db = SessionLocal()
    try:
        domain = request.args.get("domain")
        site = db.query(models.Site).filter(models.Site.domain.ilike(f"%{domain}%")).first()
        if site:
            return jsonify({"exists": True, "site_id": site.id})
        else:
            return jsonify({"exists": False})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/page/exists", methods=["GET"])
def check_page_exists():
    db = SessionLocal()
    try:
        content_hash = request.args.get("content_hash")
        if not content_hash:
            return jsonify({"error": "Missing content_hash parameter"}), 400

        existing_page = db.query(models.Page).filter(models.Page.content_hash == content_hash).first()

        if existing_page:
            return jsonify({"exists": True, "page_id": existing_page.id}), 200
        else:
            return jsonify({"exists": False}), 200
    except Exception as e:
        logger.error(f"Error in /page/exists: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
