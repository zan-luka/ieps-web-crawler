from sqlalchemy import Column, Integer, String, Text, ForeignKey, TIMESTAMP, LargeBinary
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class DataType(Base):
    __tablename__ = "data_type"
    __table_args__ = {"schema": "crawldb"}
    code = Column(String(20), primary_key=True)

class PageType(Base):
    __tablename__ = "page_type"
    __table_args__ = {"schema": "crawldb"}
    code = Column(String(20), primary_key=True)

class Site(Base):
    __tablename__ = "site"
    __table_args__ = {"schema": "crawldb"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(500))
    robots_content = Column(Text)
    sitemap_content = Column(Text)

class Page(Base):
    __tablename__ = "page"
    __table_args__ = {"schema": "crawldb"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, ForeignKey("crawldb.site.id"))
    page_type_code = Column(String(20), ForeignKey("crawldb.page_type.code"))
    url = Column(String(3000), unique=True)
    html_content = Column(Text)
    http_status_code = Column(Integer)
    accessed_time = Column(TIMESTAMP)
    accessed_ip = Column(String(16))

class PageData(Base):
    __tablename__ = "page_data"
    __table_args__ = {"schema": "crawldb"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    page_id = Column(Integer, ForeignKey("crawldb.page.id"))
    data_type_code = Column(String(20), ForeignKey("crawldb.data_type.code"))
    data = Column(LargeBinary)

class Image(Base):
    __tablename__ = "image"
    __table_args__ = {"schema": "crawldb"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    page_id = Column(Integer, ForeignKey("crawldb.page.id"))
    filename = Column(String(255))
    content_type = Column(String(50))
    data = Column(LargeBinary)
    accessed_time = Column(TIMESTAMP)

class Link(Base):
    __tablename__ = "link"
    __table_args__ = {"schema": "crawldb"}
    from_page = Column(Integer, ForeignKey("crawldb.page.id"), primary_key=True)
    to_page = Column(Integer, ForeignKey("crawldb.page.id"), primary_key=True)