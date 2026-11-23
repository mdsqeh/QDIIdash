"""CSRC fund-disclosure adapter for quota announcements."""
import io
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .parser import is_relevant_title, is_share_class_relevant, normalize_text


CSRC_BASE = 'http://eid.csrc.gov.cn/fund/disclose/'
CSRC_INDEX = 'http://eid.csrc.gov.cn/fund/disclose/index.html'
USER_AGENT = 'Mozilla/5.0 (compatible; QDII-Dashboard/1.0)'


def make_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Referer': CSRC_INDEX,
    })
    return session


def fetch_csrc_fund_id(session, fund_code, timeout=30):
    response = session.post(
        urljoin(CSRC_BASE, 'validate_fund.do'),
        data={'cFundCode': str(fund_code)},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get('isSuccess'):
        raise ValueError(f'CSRC 未找到基金代码 {fund_code}')
    return str(payload['fundId'])


def parse_csrc_announcement_rows(html, fund, limit=30):
    """Parse the CSRC fund detail page and return relevant announcement rows."""
    soup = BeautifulSoup(html or '', 'html.parser')
    rows = []
    seen = set()
    for row in soup.select('table.m-table3 tr'):
        link = row.find('a', href=True)
        if not link or 'instance_show_pdf_id' not in link['href']:
            continue
        title = normalize_text(link.get_text(' ', strip=True))
        if not title or not is_relevant_title(title):
            continue
        if not is_share_class_relevant(title, fund):
            continue
        row_text = normalize_text(row.get_text(' ', strip=True))
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', row_text)
        external_match = re.search(r'instanceid=(\d+)', link['href'])
        if not external_match:
            continue
        external_id = external_match.group(1)
        if external_id in seen:
            continue
        seen.add(external_id)
        rows.append({
            'external_id': external_id,
            'title': title,
            'published_at': date_match.group(1) if date_match else '',
            'source_url': urljoin(CSRC_BASE, link['href']),
            'source_type': 'csrc',
        })
    rows.sort(key=lambda item: item.get('published_at') or '', reverse=True)
    return rows[:limit]


def fetch_csrc_fund_detail(session, fund_id, timeout=30):
    """Fetch the raw CSRC announcement page for one parent fund id."""
    response = session.get(
        urljoin(CSRC_BASE, 'fund_detail_search.do'),
        params={'cFundCode': fund_id},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_csrc_announcements(session, fund, fund_id=None, limit=30, timeout=30, html=None):
    if not isinstance(fund, dict):
        fund = dict(fund)
    if not fund_id:
        fund_id = fetch_csrc_fund_id(session, fund['code'], timeout=timeout)
    if html is None:
        html = fetch_csrc_fund_detail(session, fund_id, timeout=timeout)
    return fund_id, parse_csrc_announcement_rows(html, fund, limit=limit)


def fetch_pdf_text(url, timeout=45, max_pages=20):
    """Download a CSRC PDF and extract text.  Returns '' on unsupported PDFs."""
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=timeout)
        response.raise_for_status()
        if not response.content.lstrip().startswith(b'%PDF'):
            return ''
        try:
            from pypdf import PdfReader
        except ImportError:
            return ''
        reader = PdfReader(io.BytesIO(response.content))
        pages = []
        for page in reader.pages[:max_pages]:
            pages.append(page.extract_text() or '')
        return normalize_text('\n'.join(pages))
    except Exception:
        return ''
