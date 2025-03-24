import datetime

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os

# ...existing imports...
import models  # assuming models.py is in the server folder
from schemas import SiteCreate, PageFrontier, PageCreate, PageDataCreate, ImageCreate, LinkCreate

# Update your DATABASE_URL to use the async driver (e.g., asyncpg for PostgreSQL)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://neondb_owner:npg_CurQ5oWtSTY3@ep-solitary-resonance-a9v88dmb-pooler.gwc.azure.neon.tech/neondb"
)

# Create an async engine and sessionmaker
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

app = FastAPI()

# Dependency: async generator for DB session
async def get_db():
    async with SessionLocal() as db:
        yield db

@app.post("/site")
async def create_site(site: SiteCreate, db: AsyncSession = Depends(get_db)):
    new_site = models.Site(
        domain=site.domain,
        robots_content=site.robots_content,
        sitemap_content=site.sitemap_content,
    )
    try:
        async with db.begin():
            db.add(new_site)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": new_site.id, "domain": new_site.domain}


@app.post("/page/frontier")
async def create_page(page: PageFrontier, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(text("id FROM crawldb.site WHERE domain = :domain LIMIT 1")).params(domain=page.site_url))
    site_id = result.scalar_one_or_none()

    new_page = models.Page(
        site_id=site_id,
        page_type_code='FRONTIER',
        url=page.url
    )
    try:
        db.add(new_page)
        await db.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": new_page.id, "url": new_page.url}


@app.put("/page/{page_id}")
async def update_page(page_id: int, page: PageCreate, db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(select(models.Page).where(models.Page.id == page_id))
        existing_page = result.scalar_one_or_none()

        if existing_page is None:
            raise HTTPException(status_code=404, detail="Page not found")

        if page.page_type_code == 'HTML':
            existing_page.page_type_code = page.page_type_code
            existing_page.html_content = page.html_content
            existing_page.http_status_code = page.http_status_code
            existing_page.accessed_time = datetime.datetime.now()

        elif page.page_type_code == 'BINARY':
            existing_page.page_type_code = page.page_type_code
            existing_page.html_content = None
            existing_page.http_status_code = page.http_status_code
            existing_page.accessed_time = datetime.datetime.now()

        elif page.page_type_code == 'DUPLICATE':
            existing_page.page_type_code = page.page_type_code
            existing_page.html_content = None
            existing_page.http_status_code = page.http_status_code
            existing_page.accessed_time = datetime.datetime.now()

        await db.commit()
        return {"id": existing_page.id, "url": existing_page.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pagedata")
async def create_page_data(pagedata: PageDataCreate, db: AsyncSession = Depends(get_db)):
    new_pagedata = models.PageData(
        page_id=pagedata.page_id,
        data_type_code=pagedata.data_type_code,
        data=pagedata.data,
    )
    try:
        async with db.begin():
            db.add(new_pagedata)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": new_pagedata.id}

@app.post("/image")
async def create_image(image: ImageCreate, db: AsyncSession = Depends(get_db)):
    new_image = models.Image(
        page_id=image.page_id,
        filename=image.filename,
        content_type=image.content_type,
        data=image.data,
        accessed_time=image.accessed_time,
    )
    try:
        async with db.begin():
            db.add(new_image)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"id": new_image.id, "filename": new_image.filename}

@app.post("/link")
async def create_link(link: LinkCreate, db: AsyncSession = Depends(get_db)):
    new_link = models.Link(from_page=link.from_page, to_page=link.to_page)
    try:
        async with db.begin():
            db.add(new_link)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"from_page": new_link.from_page, "to_page": new_link.to_page}


class LinkResponse(BaseModel):
    id: int
    url: str

@app.get("/frontier")
async def get_link_frontier(db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(select(text("id, url FROM crawldb.page WHERE page_type_code = 'FRONTIER' LIMIT 1")))
        link = result.fetchone()
        if link:
            return LinkResponse(id=link.id, url=link.url)
        else:
            return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))