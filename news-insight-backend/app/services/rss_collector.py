import feedparser
import html
import logging
import re
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from app.utils.text_cleaner import (
    is_probable_advertorial,
    make_article_hash_key,
    normalize_article_text,
)

logger = logging.getLogger(__name__)

OG_IMAGE_DOMAINS = {
    "khan.co.kr",
    "hankyung.com",
    "wowtv.co.kr",
    "mk.co.kr",
    "news1.kr",
    "naeil.com",
    "kado.net",
    "heraldcorp.com",
    "asiae.co.kr",
    "chosun.com",
    "jtbc.co.kr",
    "news.einfomax.co.kr",
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NewsInsightBot/1.0; +https://news-insight.local)",
}


def _should_fetch_og_image(link: Optional[str], feed_source: str) -> bool:
    if not link:
        return False
    domain = urlparse(link).netloc.lower()
    feed_domain = urlparse(feed_source).netloc.lower()
    for allowed in OG_IMAGE_DOMAINS:
        if domain.endswith(allowed) or feed_domain.endswith(allowed):
            return True
    return False


@lru_cache(maxsize=512)
def _fetch_og_metadata(link: str) -> Dict[str, Optional[str]]:
    try:
        resp = requests.get(link, headers=REQUEST_HEADERS, timeout=5)
        if resp.status_code != 200:
            logger.debug("OG 이미지 요청 실패(%s): status=%s", link, resp.status_code)
            return {"image": None, "description": None}

        content_type = resp.headers.get("Content-Type", "")
        if "html" not in content_type:
            return {"image": None, "description": None}

        soup = BeautifulSoup(resp.text, "lxml")

        candidates: List[str] = []
        description_candidates: List[str] = []

        for meta in soup.find_all("meta"):
            key = (meta.get("property") or meta.get("name") or "").lower()
            if key in {
                "og:image",
                "og:image:url",
                "og:image:secure_url",
                "twitter:image",
                "twitter:image:src",
            }:
                value = meta.get("content") or meta.get("value")
                if value:
                    candidates.append(value)
            if key in {"og:description", "twitter:description", "description"}:
                desc = meta.get("content") or meta.get("value")
                if desc:
                    description_candidates.append(desc)

        for link_tag in soup.find_all("link"):
            rel = [r.lower() for r in (link_tag.get("rel") or [])]
            if any(r in {"image_src", "thumbnail", "icon"} for r in rel):
                href = link_tag.get("href")
                if href:
                    candidates.append(href)

        if not candidates:
            img = soup.find("img", attrs={"src": True})
            if img:
                candidates.append(img.get("src"))
            else:
                img_lazy = soup.find("img", attrs={"data-src": True})
                if img_lazy:
                    candidates.append(img_lazy.get("data-src"))

        metadata = {"image": None, "description": None}

        for candidate in candidates:
            if not candidate:
                continue
            candidate = candidate.strip()
            if candidate.startswith("data:"):
                continue
            normalized = urljoin(link, candidate)
            if normalized.startswith("http"):
                metadata["image"] = normalized
                break

        if description_candidates and not metadata["description"]:
            metadata["description"] = html.unescape(description_candidates[0]).strip()

        if not metadata["description"]:
            main_desc = soup.find("meta", attrs={"name": "description"})
            if main_desc:
                content = main_desc.get("content")
                if content:
                    metadata["description"] = html.unescape(content).strip()

        return metadata
    except RequestException as exc:
        logger.debug("OG 이미지 요청 예외(%s): %s", link, exc)
    except Exception as exc:  # noqa: BLE001
        logger.debug("OG 이미지 파싱 예외(%s): %s", link, exc)

    return {"image": None, "description": None}


def _fetch_og_image(link: str) -> Optional[str]:
    metadata = _fetch_og_metadata(link)
    return metadata.get("image")


def _fetch_og_description(link: str) -> Optional[str]:
    metadata = _fetch_og_metadata(link)
    return metadata.get("description")


def fetch_rss_articles(rss_urls: List[str], limit_per_feed: int = 10) -> List[Dict]:
    """
    RSS 피드에서 기사 수집
    
    Args:
        rss_urls: RSS URL 리스트
        limit_per_feed: 피드당 최대 기사 수
    
    Returns:
        기사 리스트 (dict)
    """
    articles = []
    advertorial_skipped = 0
    
    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
            
            if feed.bozo and feed.bozo_exception:
                logger.warning(f"RSS 파싱 오류 ({url}): {feed.bozo_exception}")
                # 인코딩 문제로 인한 파싱 오류일 수 있으므로 직접 요청 후 재시도
                if isinstance(feed.bozo_exception, Exception) and "multi-byte encodings are not supported" in str(feed.bozo_exception):
                    try:
                        response = requests.get(url, headers=REQUEST_HEADERS, timeout=10)
                        if response.status_code == 200:
                            declared_encoding = None
                            detected_encoding = None
                            
                            if 'charset=' in response.headers.get('Content-Type', ''):
                                declared_encoding = response.headers['Content-Type'].split('charset=')[-1].split(';')[0].strip()
                            
                            match = re.search(r'encoding=["\\\']([^"\\\']+)["\\\']', response.text[:200], re.IGNORECASE)
                            if match:
                                declared_encoding = match.group(1)
                            
                            try:
                                detected_encoding = response.apparent_encoding
                            except AttributeError:
                                pass
                            
                            candidate_encodings = [declared_encoding, detected_encoding, "euc-kr", "cp949", "utf-8", "latin1"]
                            candidate_encodings = [enc for enc in candidate_encodings if enc]
                            
                            decoded_text = None
                            for enc in candidate_encodings:
                                try:
                                    decoded_text = response.content.decode(enc, errors="ignore")
                                    logger.info(f"RSS 재시도: {url} (encoding={enc})")
                                    break
                                except LookupError:
                                    continue
                            
                            if decoded_text is None:
                                decoded_text = response.content.decode("utf-8", errors="ignore")
                            
                            feed = feedparser.parse(decoded_text)
                            if feed.bozo and feed.bozo_exception:
                                logger.warning(f"RSS 재파싱 실패 ({url}): {feed.bozo_exception}")
                                continue
                        else:
                            logger.warning(f"RSS 재요청 실패 ({url}): status={response.status_code}")
                            continue
                    except Exception as retry_exc:
                        logger.warning(f"RSS 재시도 오류 ({url}): {retry_exc}")
                        continue
                else:
                    continue
            
            source = urlparse(url).netloc or urlparse(url).path
            
            for entry in feed.entries[:limit_per_feed]:
                try:
                    raw_title = getattr(entry, "title", "") or ""
                    raw_summary = getattr(entry, "summary", "") or ""

                    content_snippets: List[str] = []
                    if hasattr(entry, "content") and entry.content:
                        for content_item in entry.content:
                            value = getattr(content_item, "value", None)
                            if value is None and isinstance(content_item, dict):
                                value = content_item.get("value")
                            if value:
                                content_snippets.append(str(value))

                    if is_probable_advertorial(raw_title, raw_summary, " ".join(content_snippets)):
                        advertorial_skipped += 1
                        logger.info(
                            "광고성 기사 스킵: %s (%s)",
                            normalize_article_text(raw_title)[:80],
                            getattr(entry, "link", None),
                        )
                        continue

                    # published_at 파싱
                    published_at = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published_at = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'published'):
                        try:
                            published_at = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %Z')
                        except:
                            published_at = datetime.now()
                    else:
                        published_at = datetime.now()
                    
                    # 기본 텍스트 정리
                    title = html.unescape(raw_title).strip()
                    summary_text = html.unescape(raw_summary).strip()

                    if summary_text:
                        summary_text = re.sub(r'\s+', ' ', summary_text)

                    # 이미지 URL 추출 (다양한 소스 확인)
                    image_url = None
                    
                    # 1. media_content에서 추출 (RSS 2.0)
                    if hasattr(entry, 'media_content') and entry.media_content:
                        for media in entry.media_content:
                            if media.get('type', '').startswith('image/'):
                                image_url = media.get('url')
                                break
                    
                    # 2. enclosures에서 추출
                    if not image_url and hasattr(entry, 'enclosures') and entry.enclosures:
                        for enclosure in entry.enclosures:
                            if enclosure.get('type', '').startswith('image/'):
                                image_url = enclosure.get('href')
                                break
                    
                    # 3. summary에서 이미지 태그 추출 (더 포괄적인 패턴)
                    if not image_url:
                        summary_text = raw_summary
                        if summary_text:
                            # 여러 패턴 시도
                            patterns = [
                                r'<img[^>]+src=["\']([^"\']+)["\']',  # 기본 패턴
                                r'src=["\']([^"\']+\.(jpg|jpeg|png|gif|webp)[^"\']*)["\']',  # 이미지 확장자 포함
                                r'data-src=["\']([^"\']+)["\']',  # lazy loading
                                r'background-image:\s*url\(["\']?([^"\'()]+)["\']?\)',  # CSS background
                            ]
                            for pattern in patterns:
                                img_match = re.search(pattern, summary_text, re.IGNORECASE)
                                if img_match:
                                    image_url = img_match.group(1)
                                    break
                    
                    # 4. content 필드에서 이미지 추출 (Atom 피드 등)
                    if not image_url and hasattr(entry, 'content') and entry.content:
                        patterns = [
                            r'<img[^>]+src=["\']([^"\']+)["\']',
                            r'src=["\']([^"\']+\.(jpg|jpeg|png|gif|webp)[^"\']*)["\']',
                            r'data-src=["\']([^"\']+)["\']',
                            r'background-image:\s*url\(["\']?([^"\'()]+)["\']?\)',
                        ]
                        for content_item in entry.content:
                            if hasattr(content_item, 'value'):
                                content_text = content_item.value
                                for pattern in patterns:
                                    img_match = re.search(pattern, content_text, re.IGNORECASE)
                                    if img_match:
                                        image_url = img_match.group(1)
                                        break
                                if image_url:
                                    break
                    
                    # 5. links에서 이미지 링크 찾기
                    if not image_url and hasattr(entry, 'links') and entry.links:
                        for link in entry.links:
                            if link.get('rel') == 'enclosure' and link.get('type', '').startswith('image/'):
                                image_url = link.get('href')
                                break
                    
                    # 6. 이미지 URL 정규화 (상대 경로 -> 절대 경로)
                    if image_url:
                        # 상대 경로인 경우
                        if image_url.startswith('//'):
                            image_url = 'https:' + image_url
                        elif image_url.startswith('/'):
                            # 피드 URL의 도메인 추출
                            feed_domain = urlparse(url).scheme + '://' + urlparse(url).netloc
                            image_url = feed_domain + image_url
                        elif not image_url.startswith('http'):
                            # 상대 경로인 경우
                            feed_domain = urlparse(url).scheme + '://' + urlparse(url).netloc
                            feed_path = '/'.join(urlparse(url).path.split('/')[:-1])
                            if feed_path:
                                image_url = feed_domain + feed_path + '/' + image_url
                            else:
                                image_url = feed_domain + '/' + image_url

                    # 7. OG 태그 기반 이미지 추출 (일부 언론사 대응)
                    if not image_url:
                        article_link = getattr(entry, "link", None)
                        if _should_fetch_og_image(article_link, url):
                            image_url = _fetch_og_image(article_link)
                    
                    hash_key = make_article_hash_key(title, image_url)
                    article_link = getattr(entry, "link", None)

                    # OG 메타데이터 보완 (이미지/요약)
                    og_metadata: Optional[Dict[str, Optional[str]]] = None
                    if (not image_url or not summary_text) and article_link and _should_fetch_og_image(article_link, url):
                        og_metadata = _fetch_og_metadata(article_link)
                        if not image_url:
                            image_url = og_metadata.get("image")
                        if not summary_text:
                            summary_text = og_metadata.get("description") or ""
                    
                    articles.append({
                        "source": source,
                        "title": title,
                        "summary": summary_text,
                        "link": article_link,
                        "published_at": published_at.isoformat(),
                        "image_url": image_url,
                        "hash_key": hash_key,
                    })
                except Exception as e:
                    logger.error(f"기사 파싱 오류 ({url}): {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"RSS 수집 실패 ({url}): {e}")
            continue
    
    if advertorial_skipped:
        logger.info("광고성 기사 %d건 스킵됨", advertorial_skipped)
    
    logger.info(f"총 {len(articles)}개 기사 수집 완료")
    return articles

