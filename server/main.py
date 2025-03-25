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
        site_id = 3
        if site_id is None:
            return jsonify({"error": "Site not found"}), 404

        # Filter links to only keep those from "slo-tech.com"
        valid_links = [link for link in data["links"] if get_domain(link) == "slo-tech.com"]

        if not valid_links:
            return jsonify({"status": "no valid links"}), 200

        # Prepare batch insert
        stmt = insert(models.Page).values([
            {"site_id": site_id, "page_type_code": "FRONTIER", "url": link}
            for link in valid_links
        ]).on_conflict_do_nothing(index_elements=["url"])  # Ignore duplicates

        db.execute(stmt)  # Execute batch insert
        db.commit()

        return jsonify({"status": "success", "inserted": len(valid_links)})

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
        existing_page.accessed_time = datetime.datetime.utcnow()
        existing_page.accessed_ip = data["accessed_ip"]
        if data["page_type_code"] == 'HTML':
            existing_page.html_content = data["html_content"]
        else:
            existing_page.html_content = None

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
            select(models.Page.id, models.Page.url).where(models.Page.page_type_code == "FRONTIER")
        ).fetchall()

        # TODO PREFERENTIAL LOGIC
        selected_row_id = results[0].id
        selected_row_url = results[0].url

        # update page type to "CRAWLED"
        db.execute(
            text("UPDATE crawldb.page SET page_type_code = 'CRAWLING' WHERE id = :id"),
            {"id": selected_row_id}
        )
        db.commit()

        return jsonify({"id": selected_row_id, "url": selected_row_url})
    except Exception as e:
        logger.error(f'Error /frontier-urls: {e}')
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

if __name__ == "__main__":
    app.run(debug=True)
