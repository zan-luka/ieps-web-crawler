from pydantic import BaseModel
from typing import Optional

class SiteCreate(BaseModel):
    domain: str
    robots_content: str = None
    sitemap_content: str = None

class DelayData(BaseModel):
    site_url: str
    ip: str
    robots_delay: Optional[float] = None

class PageFrontier(BaseModel):
    site_url: str
    url: str


class PageCreate(BaseModel):
    page_type_code: str
    html_content: str = None
    http_status_code: int = None
    accessed_ip: str  # IPv4 address


class PageDataCreate(BaseModel):
    page_id: int
    data_type_code: str
    data: bytes


class ImageCreate(BaseModel):
    page_id: int
    filename: str
    content_type: str
    data: bytes
    accessed_time: str = None  # ISO formatted string


class LinkCreate(BaseModel):
    from_page: int
    to_page: int