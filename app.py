#!/usr/bin/env python3
# Мониторинг интернет цен — сравнение цен по брендам на нескольких площадках.
# Команды: discover|daily|compare|report|selftest|init|selfupdate|postupdate
import os, re, sys, json, time, random, hashlib, gzip, base64, ast, traceback
import datetime as dt
import math
import xml.etree.ElementTree as ET
from html import unescape as _un
from pathlib import Path

import httpx
import psycopg
from psycopg.rows import dict_row

BASE = Path(os.environ.get("PM_BASE", "/opt/pricemon"))
DATA = Path(os.environ.get("PM_DATA", "/var/lib/pricemon"))
RAW = DATA / "raw"
PUB = DATA / "pub"
ARCHIVE = DATA / "archive"
ARCHIVE_RAW = ARCHIVE / "raw"
ARCHIVE_PRICES = ARCHIVE / "prices"
ARCHIVE_RUNS = ARCHIVE / "runs"
PUSH_QUEUE = DATA / "push-queue"
REFERENCE = DATA / "reference"
for p in (RAW, PUB, ARCHIVE_RAW, ARCHIVE_PRICES, ARCHIVE_RUNS, PUSH_QUEUE, REFERENCE):
    p.mkdir(parents=True, exist_ok=True)

def env(k, d=""):
    return os.environ.get(k, d)

DSN = env("PM_DSN", "postgresql:///pricemon")
# паузы между запросами; обход чередует площадки, поэтому пауза небольшая
DELAY_MIN = float(env("PM_PAUSE_MIN", "1.5"))
DELAY_MAX = float(env("PM_PAUSE_MAX", "3.5"))
HTTP2 = env("PM_HTTP2", "0") == "1"
SOURCE_GIVEUP = int(env("PM_SOURCE_GIVEUP", "12"))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ---------- бренды ----------
# ключ — как показываем, значения — как бренд пишут в адресах страниц
BRANDS = {
    "unox": ("unox", "унокс"),
    "carboma": ("carboma", "polyus", "polus", "карбома", "polair-carboma"),
}
BRAND_RE = {b: re.compile("|".join(v), re.I) for b, v in BRANDS.items()}

def brand_of(text):
    low = (text or "").lower()
    for b, rx in BRAND_RE.items():
        if rx.search(low):
            return b
    return None

# ---------- площадки ----------
# mode: entero | slug (бренд в адресе товара) | brandpage (бренд только у разделов)
SOURCES = {
    "entero": dict(base="https://entero.ru", mode="entero", title="Энтеро"),
    "rkomplekt": dict(base="https://r-komplekt.ru", sm="/sitemap.xml", mode="slug",
                      item="/catalog/", title="Ресторан Комплект",
                      sect=r'/catalog/[^"\s<>]*/proizvoditel_[a-z]+/?$',
                      link=r'/catalog/[^"?#\s<>]+/[^"?#\s<>]+/'),
    "zamoroz": dict(base="https://zamoroz.ru", sm="/sitemap.xml", mode="slug",
                    item="/catalog/", title="Заморозь.ру"),
    "refro": dict(base="https://www.refro.ru", sm="/sitemap.xml", mode="brandpage",
                  title="Рефро",
                  sect=r'/category/[^"\s<>]*(?:unox|carboma|polyus)[^"\s<>]*/?$',
                  link=r'/product/[^"?#\s<>]+/?'),
}
OFF = set(x for x in env("PM_SOURCES_OFF", "").split(",") if x)

SCHEMA = """
CREATE TABLE IF NOT EXISTS product (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL, ext_id TEXT NOT NULL, url TEXT NOT NULL,
  name TEXT, model_code TEXT, brand TEXT, category TEXT,
  status TEXT DEFAULT 'active',
  first_seen TIMESTAMPTZ DEFAULT now(), last_seen TIMESTAMPTZ DEFAULT now(),
  UNIQUE (source, ext_id)
);
CREATE TABLE IF NOT EXISTS price_snapshot (
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT REFERENCES product(id) ON DELETE CASCADE,
  captured_at TIMESTAMPTZ DEFAULT now(), captured_date DATE DEFAULT current_date,
  price NUMERIC, old_price NUMERIC, availability TEXT,
  in_stock BOOLEAN, ok BOOLEAN DEFAULT true,
  UNIQUE (product_id, captured_date)
);
CREATE TABLE IF NOT EXISTS crawl_run (
  id BIGSERIAL PRIMARY KEY, kind TEXT, started_at TIMESTAMPTZ DEFAULT now(),
  finished_at TIMESTAMPTZ, ok INT DEFAULT 0, failed INT DEFAULT 0, note TEXT
);
ALTER TABLE product ADD COLUMN IF NOT EXISTS brand TEXT;
ALTER TABLE product ADD COLUMN IF NOT EXISTS watch TEXT;
ALTER TABLE price_snapshot ADD COLUMN IF NOT EXISTS in_stock BOOLEAN;
CREATE INDEX IF NOT EXISTS ix_snap_prod ON price_snapshot(product_id, captured_date DESC);
CREATE INDEX IF NOT EXISTS ix_prod_brand ON product(brand, status);
"""

def db():
    return psycopg.connect(DSN, row_factory=dict_row, autocommit=True)

def init_db():
    with db() as c:
        c.execute(SCHEMA)

def sleep_polite():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

class Blocked(Exception):
    pass

THROTTLE_SIG = re.compile(
    r"ConnectionTerminated|ENHANCE_YOUR_CALM|GOAWAY|error_code:11|"
    r"RemoteProtocolError|ConnectError|ReadTimeout|ConnectTimeout", re.I)

# страница-капча приходит с кодом 200 — ловим её по тексту
ANTIBOT = re.compile(
    r"fail2ban|need_captcha|выглядят\s+автоматизированными|"
    r"подтвердите,?\s*что\s+вы\s+человек|доступ\s+временно\s+ограничен", re.I)

def fetch(client, url, tries=2):
    last = None
    for i in range(tries):
        try:
            r = client.get(url, headers=HEADERS, timeout=40, follow_redirects=True)
            if r.status_code in (403, 429, 503):
                raise Blocked(str(r.status_code))
            if r.status_code == 404:
                return None
            r.raise_for_status()
            html = r.text
            if ANTIBOT.search(html[:6000]):
                raise Blocked("капча: сайт просит подтвердить, что мы человек")
            return html
        except Blocked as e:
            last = e
            time.sleep(20 * (i + 1))
        except Exception as e:
            last = e
            if THROTTLE_SIG.search(repr(e)):
                last = Blocked(repr(e)[:80])
                time.sleep(20 * (i + 1))
            else:
                time.sleep(5 * (i + 1))
        finally:
            sleep_polite()
    raise last if last else RuntimeError("fetch failed")

def strip_tags(s):
    s = re.sub(r"<script.*?</script>|<style.*?</style>", " ", s or "", flags=re.S | re.I)
    return re.sub(r"\s+", " ", _un(re.sub(r"<[^>]+>", " ", s))).strip()

def to_num(s):
    if s is None:
        return None
    t = re.sub(r"[^\d,.]", "", str(s)).replace("\xa0", "")
    t = t.replace(",", ".") if t.count(",") == 1 and t.count(".") == 0 else t.replace(",", "")
    try:
        v = float(t)
    except Exception:
        return None
    return v if v > 0 else None

def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default

def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def reference_items(name):
    data = read_json(REFERENCE / name, {}) or {}
    return data.get("items", data if isinstance(data, list) else [])

def reference_index(name):
    return {flat(str(item.get("product_code") or item.get("model") or "")): item
            for item in reference_items(name)
            if item.get("product_code") or item.get("model")}

def fetch_policy():
    """Политика принадлежит Product Center; локально хранится только последний ответ."""
    url = env("PM_POLICY_URL").strip()
    token = env("PM_READ_TOKEN", env("PM_WATCH_TOKEN")).strip()
    cache = REFERENCE / "policy-cache.json"
    if not url:
        saved = read_json(cache)
        if saved:
            return saved, "cache"
        raise RuntimeError("PM_POLICY_URL не настроен и кэша политики нет")
    try:
        headers = {"Authorization": "Bearer " + token} if token else {}
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        policy = response.json()
        required = ("version", "market_multiplier", "cost_floor_multiplier", "rounding",
                    "min_platforms", "derived_coefficients", "anomaly_cost_dealer_ratio")
        absent = [key for key in required if key not in policy]
        if absent:
            raise ValueError("неполная политика: " + ", ".join(absent))
        write_json(cache, policy)
        return policy, "product-center"
    except Exception:
        saved = read_json(cache)
        if saved:
            return saved, "cache"
        raise

def refresh_eur_rate():
    """Получает официальный EUR/RUB ЦБ; при ошибке возвращает последнее значение."""
    url = "https://www.cbr.ru/scripts/XML_daily.asp"
    cache = REFERENCE / "eur-rate.json"
    try:
        response = httpx.get(url, timeout=30, headers={"User-Agent": UA})
        response.raise_for_status()
        root = ET.fromstring(response.content)
        node = next((item for item in root.findall("Valute")
                     if (item.findtext("CharCode") or "").strip() == "EUR"), None)
        if node is None:
            raise ValueError("EUR отсутствует в ответе ЦБ")
        nominal = int(node.findtext("Nominal") or "1")
        value = float((node.findtext("Value") or "").replace(",", ".")) / nominal
        result = {"currency": "EUR", "rate_rub": value, "effective_date": root.attrib.get("Date"),
                  "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(), "source": url}
        write_json(cache, result)
        return result, "cbr"
    except Exception:
        saved = read_json(cache)
        if saved:
            return saved, "cache"
        raise

EXCLUDED_PRICE_NAME = re.compile(r"уценк|разбит|\bб\s*/\s*у\b|\bбрак\b", re.I)

def derived_k(brand, dealer_rub, policy):
    coefficients = policy["derived_coefficients"]
    normalized = (brand or "").strip().lower()
    if normalized in ("carboma", "полюс", "polus"):
        return float(coefficients["carboma"])
    if normalized != "unox":
        return None
    for tier in coefficients["unox"]:
        maximum = tier.get("max_dealer_price")
        if maximum is None or dealer_rub <= float(maximum):
            return float(tier["k"])
    return None

def ceil_policy(value, policy):
    step = float(policy["rounding"]["step"])
    if policy["rounding"].get("mode") != "ceil" or step <= 0:
        raise ValueError("неподдерживаемое округление политики")
    return int(math.ceil(value / step) * step)

def calculate_price_item(item, policy, eur_rate):
    """Чистый расчёт одной позиции; чувствительные входы не возвращаются."""
    name = str(item.get("name") or "")
    if (item.get("kind") == "part" or item.get("usable") is False or is_part(name)
            or EXCLUDED_PRICE_NAME.search(name) or "+" in name):
        return {"price": None, "price_source": None, "price_rule": None,
                "publishable": False, "reason": "excluded_item"}
    offers = [offer for offer in item.get("offers", [])
              if offer.get("usable", True) and offer.get("in_stock", True)
              and offer.get("match_confidence", "exact") == "exact" and to_num(offer.get("price"))]
    platforms = len({offer.get("source") for offer in offers})
    multiplier = float(policy["market_multiplier"])
    base = source = rule = None
    min_offer = min(offers, key=lambda offer: float(offer["price"])) if offers else None
    if min_offer and platforms >= int(policy["min_platforms"]):
        base = float(min_offer["price"]) * multiplier
        source, rule = "monitor", "market_plus"
    dealer = to_num(item.get("dealer_price"))
    dealer_currency = str(item.get("dealer_currency") or "RUB").upper()
    dealer_rub = dealer
    if dealer and dealer_currency == "EUR":
        dealer_rub = dealer * float(eur_rate["rate_rub"])
    if base is None and dealer_rub:
        coefficient = derived_k(item.get("brand"), dealer_rub, policy)
        if coefficient is not None:
            base = dealer_rub * coefficient * multiplier
            source, rule = "derived", "derived"
    if base is None:
        return {"price": None, "price_source": None, "price_rule": None,
                "publishable": False, "reason": "no_price"}
    cost = to_num(item.get("cost"))
    cost_currency = str(item.get("cost_currency") or "RUB").upper()
    cost_rub = cost
    if cost and cost_currency == "EUR":
        cost_rub = cost * float(policy["eur_import_coefficient"]) * float(eur_rate["rate_rub"])
    if cost_rub and dealer_rub:
        ratio = cost_rub / dealer_rub
        bounds = policy["anomaly_cost_dealer_ratio"]
        if ratio < float(bounds["min"]) or ratio > float(bounds["max"]):
            return {"price": None, "price_source": source, "price_rule": rule,
                    "publishable": False, "reason": "check_cost", "platforms": platforms}
    if cost_rub and base < cost_rub * float(policy["cost_floor_multiplier"]):
        base = cost_rub * float(policy["cost_floor_multiplier"])
        rule = "cost_floor"
    return {"price": ceil_policy(base, policy), "price_source": source, "price_rule": rule,
            "publishable": True, "reason": "ready", "platforms": platforms,
            "min_price": float(min_offer["price"]) if min_offer else None,
            "min_source": min_offer.get("source") if min_offer else None}

def cmd_rate():
    rate, source = refresh_eur_rate()
    print("EUR %.4f RUB (%s, %s)" % (rate["rate_rub"], rate.get("effective_date"), source))

def cmd_policy():
    policy, source = fetch_policy()
    print("policy version=%s market_multiplier=%s source=%s" %
          (policy["version"], policy["market_multiplier"], source))

def cmd_calculate():
    policy, policy_source = fetch_policy()
    eur_rate, rate_source = refresh_eur_rate()
    market = read_json(PUB / "compare.json", {}) or {}
    dealers, costs, stock = (reference_index("dealer-prices.json"),
                             reference_index("costs.json"), reference_index("stock.json"))
    items = []
    for row in market.get("rows", []):
        code = flat(str(row.get("code") or ""))
        dealer, cost, inventory = dealers.get(code, {}), costs.get(code, {}), stock.get(code, {})
        calculation = calculate_price_item({
            "brand": row.get("brand"), "name": row.get("name"), "kind": dealer.get("kind", "device"),
            "offers": row.get("offers", []), "dealer_price": dealer.get("dealer_price"),
            "dealer_currency": dealer.get("dealer_currency"), "cost": cost.get("cost"),
            "cost_currency": cost.get("cost_currency"), "in_stock": bool(
                to_num(inventory.get("free_qty")) or to_num(inventory.get("reserved_qty"))),
        }, policy, eur_rate)
        items.append({"product_id": dealer.get("product_id"), "brand": row.get("brand"),
                      "model_code": row.get("code"), **calculation})
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    payload = {"generated": generated, "policy_version": policy["version"],
               "policy_source": policy_source, "eur_rate_source": rate_source, "items": items}
    write_json(PUB / "prices.json", payload)
    write_json(ARCHIVE_PRICES / (generated[:10] + ".json"), payload)
    print("calculate: %d items, %d prices" % (len(items), sum(x["price"] is not None for x in items)))

# ---------- разбор карточки ----------
IN_STOCK = re.compile(r"instock|в\s*наличи|есть\s*в\s*наличии|на\s*складе", re.I)
NO_STOCK = re.compile(r"outofstock|нет\s*в\s*наличии|под\s*заказ|ожидается|preorder|снят", re.I)

# Запчасть узнаём по ПЕРВОМУ слову названия — по товарной категории, а не по любому
# упоминанию. «Витрина ... (с боковинами)» — витрина, а не боковина.
# Подставки, направляющие и гастроёмкости — самостоятельный товар, их оставляем.
PART = re.compile(
    r"стеклопакет|боковин|делител|перегородк|стыковочн|ручка|кронштейн|"
    r"\bтэн\b|двигател|актуатор|каркас|термостат|уплотнен|уплотнит|прокладк|"
    r"\bпетл|фильтр|решётк|решетк|противень|поддон", re.I)

# эти слова однозначны в любом месте названия
PART_ANY = re.compile(r"запчаст|комплектующ", re.I)

# «… для XF043», «… к XEVC-1011»: название несёт чужой код модели — это принадлежность
PART_FOR = re.compile(r"\b(?:для|к)\s+[A-ZА-ЯЁ]{2,6}[\s-]?\d{2,}", re.I)

def head_of(name):
    """Начало названия до бренда, латиницы или скобки — товарная категория."""
    return re.split(r"[(\[]|[A-Za-z]", name or "", 1)[0].strip().lower()

def is_part(name):
    n = name or ""
    return bool(PART.search(head_of(n)) or PART_ANY.search(n) or PART_FOR.search(n))

def stock_of(text):
    t = text or ""
    if NO_STOCK.search(t):
        return False
    if IN_STOCK.search(t):
        return True
    return None

def _jsonld(html):
    for m in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.S | re.I):
        try:
            data = json.loads(m.group(1).strip())
        except Exception:
            continue
        stack = [data]
        while stack:
            d = stack.pop()
            if isinstance(d, list):
                stack.extend(d)
                continue
            if not isinstance(d, dict):
                continue
            t = d.get("@type")
            t = t if isinstance(t, str) else (t[0] if isinstance(t, list) and t else "")
            if str(t).lower() == "product":
                return d
            stack.extend(v for v in d.values() if isinstance(v, (dict, list)))
    return None

def parse_shop(html):
    """JSON-LD → микроразметка → запасные варианты."""
    out = {"name": None, "price": None, "old_price": None,
           "availability": None, "in_stock": None, "problems": [], "how": None}
    d = _jsonld(html)
    if d:
        out["name"] = d.get("name")
        offers = d.get("offers")
        if isinstance(offers, list):
            offers = offers[0] if offers else None
        if isinstance(offers, dict):
            out["price"] = to_num(offers.get("price") or offers.get("lowPrice"))
            av = str(offers.get("availability") or "")
            out["availability"] = av.rsplit("/", 1)[-1] or None
            out["in_stock"] = stock_of(av)
            out["how"] = "json-ld"
    if out["price"] is None:
        m = (re.search(r'itemprop="price"[^>]*content="([\d\s.,]+)"', html)
             or re.search(r'content="([\d\s.,]+)"[^>]*itemprop="price"', html)
             or re.search(r'property="product:price:amount"[^>]*content="([\d\s.,]+)"', html)
             or re.search(r'itemprop="price"[^>]*>\s*([\d\s.,]+)', html))
        if m:
            out["price"] = to_num(m.group(1))
            out["how"] = out["how"] or "микроразметка"
    if out["price"] is None:
        m = re.search(r'data-(?:price|value)="([\d\s.,]{4,})"', html)
        if m:
            out["price"] = to_num(m.group(1))
            out["how"] = out["how"] or "data-атрибут"
    if out["price"] is None:
        m = re.search(r'class="[^"]*price[^"]*"[^>]*>[^<]*?([\d][\d\s\xa0]{3,})\s*(?:руб|₽|р\.)', html, re.I)
        if m:
            out["price"] = to_num(m.group(1))
            out["how"] = out["how"] or "текст цены"
    if not out["name"]:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S | re.I)
        out["name"] = strip_tags(m.group(1)) if m else None
    if out["in_stock"] is None:
        m = re.search(r'itemprop="availability"[^>]*(?:content|href)="([^"]+)"', html)
        if m:
            out["availability"] = m.group(1).rsplit("/", 1)[-1]
            out["in_stock"] = stock_of(m.group(1))
    if out["in_stock"] is None:
        head = strip_tags(html)[:6000]
        out["in_stock"] = stock_of(head)
        if out["in_stock"] is not None:
            out["availability"] = "В наличии" if out["in_stock"] else "Под заказ"
    if out["price"] is None:
        out["problems"].append("цена не найдена")
    if not out["name"]:
        out["problems"].append("название не найдено")
    return out

def parse_entero(html):
    out = parse_shop(html)
    if out["price"] is None:
        m = re.search(r'"price"\s*:\s*"?([\d.]+)', html)
        if m:
            out["price"] = to_num(m.group(1))
            out["how"] = "entero json"
            out["problems"] = [x for x in out["problems"] if "цена" not in x]
    return out

PARSERS = {"entero": parse_entero}

def parse_card(source, html):
    return PARSERS.get(source, parse_shop)(html)

def is_empty(d):
    return d.get("price") is None and not d.get("name")

# ---------- код модели: по нему сводим одну и ту же вещь с разных площадок ----------
STOP_TOK = {"EU", "RU", "GN", "XL", "LUX", "ЛЮКС", "СТАНДАРТ", "STANDARD", "TECHNO",
            "ТЕХНО", "ЭКО", "ECO", "CUBE", "PLUS", "МСК", "ВЕРСИЯ", "V2", "2.0",
            "ONE", "PRO", "NEW", "БУ", "ГАЗ", "GAS", "МПС", "MP"}

def norm_code(s):
    s = (s or "").upper().replace("Ё", "Е").replace("\xa0", " ")
    s = s.replace(",", ".").replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", s).strip()

def model_code(name, brand=None):
    n = norm_code(name)
    if not n:
        return None
    for rx in BRAND_RE.values():
        n = rx.sub(" ", n)
    n = re.sub(r"\([^)]*\)", " ", n)
    if brand == "unox":
        m = re.search(r"\bX[A-Z]{1,4}\s?[- ]?\d{2,4}[A-Z0-9\-]*", n)
        if m:
            return re.sub(r"[\s]+", "", m.group(0))
    toks = []
    for t in re.findall(r"[A-ZА-Я0-9][A-ZА-Я0-9./\-]*", n):
        if t in STOP_TOK or len(t) < 2:
            continue
        if re.search(r"\d", t) or (len(t) >= 2 and re.match(r"^[A-Z]+$", t)):
            toks.append(t)
    if not toks:
        return None
    return "-".join(toks[:5])

# ---------- поиск товаров ----------
RE_LOC = re.compile(r"<loc>\s*([^<]+)\s*</loc>")
NOT_ITEM = re.compile(r"proizvoditel_|/filter/|/brand[s]?/|/vendors?/|/category/|/list/|/news/|/stati/", re.I)
RE_HREF = re.compile(r'href="([^"#]+)"')
ENTERO = "https://entero.ru"
RE_ITEM = re.compile(r'href="(/item/(\d+))"')

def sitemap_urls(client, cfg, log):
    idx = fetch(client, cfg["base"] + cfg["sm"])
    maps = [u.strip() for u in RE_LOC.findall(idx or "")]
    if not maps:
        return []
    if not any(m.endswith(".xml") for m in maps):
        return maps
    out = []
    for m in maps:
        if "geo" in m or "files" in m:
            continue
        try:
            xml = fetch(client, m)
        except Exception as e:
            log("  sitemap %s: %s" % (m, repr(e)[:60]))
            continue
        out += [u.strip() for u in RE_LOC.findall(xml or "")]
    return out

def discover_entero(client, log):
    urls = {}
    for brand, keys in BRANDS.items():
        for key in keys[:1]:
            hubs = ["%s/vendors/%s" % (ENTERO, key)] + \
                   ["%s/vendors/%s/%d" % (ENTERO, key, n) for n in (500, 501, 502)]
            lists = set()
            for h in hubs:
                try:
                    html = fetch(client, h)
                except Exception:
                    continue
                if not html:
                    continue
                for m in RE_ITEM.finditer(html):
                    urls[m.group(2)] = (ENTERO + m.group(1), brand)
                for m in re.finditer(r'href="(/list/[^"#?]*%s[^"#?]*/?)"' % key, html, re.I):
                    lists.add(ENTERO + m.group(1))
            for lu in sorted(lists):
                for page in range(1, 60):
                    u = lu if page == 1 else "%s?p=%d" % (lu, page)
                    try:
                        html = fetch(client, u)
                    except Exception:
                        break
                    if not html:
                        break
                    found = {m.group(2): (ENTERO + m.group(1), brand)
                             for m in RE_ITEM.finditer(html)}
                    new = set(found) - set(urls)
                    urls.update(found)
                    if not new:
                        break
            log("  entero/%s: накопилось %d" % (brand, len(urls)))
    return [("entero", k, v[0], v[1]) for k, v in urls.items()]

def discover_slug(name, cfg, client, log):
    urls = {}
    for u in sitemap_urls(client, cfg, log):
        low = u.lower()
        if cfg["item"] not in low or NOT_ITEM.search(low):
            continue
        b = brand_of(low)
        if not b:
            continue
        key = low.rstrip("/").rsplit("/", 1)[-1]
        urls[key] = (u, b)
    log("  %s: %d карточек из карты сайта" % (name, len(urls)))
    return [(name, k, v[0], v[1]) for k, v in urls.items()]

def discover_brandpage(name, cfg, client, log):
    sect_rx = re.compile(cfg["sect"], re.I)
    link_rx = re.compile(cfg["link"], re.I)
    sects = []
    for u in sitemap_urls(client, cfg, log):
        if sect_rx.search(u) and brand_of(u):
            sects.append(u)
    log("  %s: разделов бренда в карте сайта — %d" % (name, len(sects)))
    urls = {}
    for su in sects:
        b = brand_of(su)
        for page in range(1, 30):
            u = su if page == 1 else "%s?PAGEN_1=%d" % (su.rstrip("/") + "/", page)
            try:
                html = fetch(client, u)
            except Exception as e:
                log("  %s: %s — %s" % (name, u, repr(e)[:60]))
                break
            if not html:
                break
            found = {}
            for m in RE_HREF.finditer(html):
                h = m.group(1)
                if not link_rx.fullmatch(h) and not link_rx.fullmatch(h.rstrip("/") + "/"):
                    continue
                if "proizvoditel_" in h or "/filter/" in h:
                    continue
                full = h if h.startswith("http") else cfg["base"] + h
                found[h.rstrip("/").rsplit("/", 1)[-1]] = (full, b)
            new = set(found) - set(urls)
            urls.update(found)
            if not new:
                break
    log("  %s: %d карточек со страниц бренда" % (name, len(urls)))
    return [(name, k, v[0], v[1]) for k, v in urls.items()]

def cmd_discover():
    init_db()
    lines = []
    log = lambda s: (lines.append(s), print(s, flush=True))
    rows = []
    with httpx.Client(http2=HTTP2) as client, db() as c:
        for name, cfg in SOURCES.items():
            if name in OFF:
                log("%s: выключен настройкой" % name)
                continue
            log("%s (%s):" % (cfg["title"], name))
            try:
                if cfg["mode"] == "entero":
                    rows += discover_entero(client, log)
                elif cfg["mode"] == "slug":
                    rows += discover_slug(name, cfg, client, log)
                else:
                    rows += discover_brandpage(name, cfg, client, log)
            except Exception as e:
                log("  ОШИБКА %s: %s" % (name, repr(e)[:160]))
        for src, ext, url, brand in rows:
            c.execute(
                "INSERT INTO product(source, ext_id, url, brand) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (source, ext_id) DO UPDATE SET url=EXCLUDED.url, "
                "brand=COALESCE(product.brand, EXCLUDED.brand), last_seen=now()",
                (src, ext[:200], url, brand))
        stat = c.execute("SELECT source, brand, count(*) n FROM product WHERE status='active'"
                         " GROUP BY source, brand ORDER BY source, brand").fetchall()
    log("")
    log("ИТОГО в базе:")
    for r in stat:
        log("  %-11s %-9s %5d" % (r["source"], r["brand"] or "?", r["n"]))
    (PUB / "discovery.txt").write_text("\n".join(lines), encoding="utf-8")

def interleave(rows):
    """Чередуем площадки, чтобы не бить одну подряд."""
    by = {}
    for r in rows:
        by.setdefault(r["source"], []).append(r)
    qs = list(by.values())
    out = []
    for i in range(max((len(q) for q in qs), default=0)):
        for q in qs:
            if i < len(q):
                out.append(q[i])
    return out

def cmd_daily(only=None, allrows=None, limit=None, minutes=None):
    """Сначала наши позиции в наличии, потом — в оставшееся время — всё остальное."""
    init_db()
    ok = fail = skipped = 0
    bad, closed, report = {}, set(), []
    # запас времени на «остальное»: по умолчанию 5 часов, свои позиции обходим всегда
    budget = float(minutes or env("PM_REST_MINUTES", "300") or 300)
    started = time.monotonic()
    # перед каждым обходом сверяем наш список с карточками: за неделю площадки
    # могли завести новые страницы под наши же модели
    if env("PM_WATCH_URL") or (BASE / "watch.txt").is_file():
        try:
            cmd_watch()
        except Exception as e:
            print("WATCH_ERR", repr(e)[:120])
    with httpx.Client(http2=HTTP2) as client, db() as c:
        q = "SELECT * FROM product WHERE status='active'"
        if only:
            q += " AND source='%s'" % re.sub(r"\W", "", only)
        rows = [p for p in c.execute(q + " ORDER BY source, id").fetchall()
                if p["source"] in SOURCES and p["source"] not in OFF]
        ours = interleave([p for p in rows if p["watch"] is not None])
        # остальные — начиная с тех, которые дольше всех не проверялись
        rest = interleave(sorted([p for p in rows if p["watch"] is None],
                                 key=lambda p: (p["last_seen"] is not None, p["last_seen"])))
        if allrows or not ours:      # список не задан или явно просят обойти всё подряд
            ours, rest = interleave(rows), []
        prods = ours + rest
        n_ours = len(ours)
        if limit:
            prods = prods[:int(limit)]
        n = len(prods)
        run = c.execute("INSERT INTO crawl_run(kind) VALUES ('daily') RETURNING id").fetchone()["id"]
        for i, p in enumerate(prods):
            # свои позиции обходим полностью, у остальных есть лимит по времени
            if i >= n_ours and (time.monotonic() - started) / 60 > budget:
                report.append("СТОП: время на «остальное» вышло (%.0f мин), "
                              "проверено %d из %d прочих" % (budget, i - n_ours, n - n_ours))
                skipped += n - i
                break
            if p["source"] in closed:
                skipped += 1
                continue
            if i % 25 == 0:
                (PUB / "daily-progress.txt").write_text(
                    "наши позиции: %d из %d · остальные: %d из %d · ошибок %d · %s" %
                    (min(i, n_ours), n_ours, max(0, i - n_ours), n - n_ours, fail,
                     dt.datetime.now().isoformat(timespec="seconds")), encoding="utf-8")
            try:
                html = fetch(client, p["url"])
                if not html:
                    c.execute("UPDATE product SET status='gone' WHERE id=%s", (p["id"],))
                    continue
                d = parse_card(p["source"], html)
                # пустая страница не должна стирать вчерашнюю цену
                if is_empty(d):
                    raise Blocked("страница прочиталась пустой — прежние данные не трогаю")
                bad[p["source"]] = 0
                nm = d.get("name") or p["name"]
                br = p["brand"] or brand_of(nm) or brand_of(p["url"])
                c.execute("UPDATE product SET name=COALESCE(%s,name), brand=COALESCE(%s,brand),"
                          " model_code=%s, last_seen=now() WHERE id=%s",
                          (nm, br, model_code(nm, br), p["id"]))
                c.execute(
                    "INSERT INTO price_snapshot(product_id,price,old_price,availability,in_stock,ok)"
                    " VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (product_id,captured_date) DO UPDATE"
                    " SET price=EXCLUDED.price, availability=EXCLUDED.availability,"
                    " in_stock=EXCLUDED.in_stock, ok=EXCLUDED.ok",
                    (p["id"], d.get("price"), d.get("old_price"), d.get("availability"),
                     d.get("in_stock"), not d["problems"]))
                ok += 1
                report.append("OK  %-11s %-30s %10s  %s" %
                              (p["source"], (nm or "")[:30], d.get("price"), d.get("availability")))
            except Exception as e:
                fail += 1
                src = p["source"]
                bad[src] = bad.get(src, 0) + 1
                report.append("ERR %-11s %s %s" % (src, p["ext_id"], repr(e)[:100]))
                if bad[src] >= SOURCE_GIVEUP and src not in closed:
                    closed.add(src)
                    report.append("SKIP: %s отказывает подряд %d раз — откладываю, "
                                  "прежние цены остаются как есть" % (src, bad[src]))
                    _step("SKIP_" + src)
        c.execute("UPDATE crawl_run SET finished_at=now(), ok=%s, failed=%s WHERE id=%s",
                  (ok, fail, run))
    summary = ("наши позиции: %d карточек · остальные: %d · проверено %d, ошибок %d, "
            "не дошли %d\n%s, завершён"
            % (n_ours, n - n_ours, ok, fail, skipped,
               dt.datetime.now().isoformat(timespec="seconds")))
    (PUB / "daily-progress.txt").write_text(summary, encoding="utf-8")
    (PUB / "daily.txt").write_text(summary + "\n\n" + "\n".join(report), encoding="utf-8")
    print("daily: свои=%d ok=%d fail=%d skipped=%d" % (n_ours, ok, fail, skipped))

# ---------- сравнение цен ----------
def latest():
    """Последнее удачное чтение по каждой карточке."""
    with db() as c:
        rows = c.execute("""
            SELECT DISTINCT ON (s.product_id) s.product_id, s.captured_date, s.price,
                   s.availability, s.in_stock, p.source, p.name, p.url, p.brand, p.model_code
            FROM price_snapshot s JOIN product p ON p.id=s.product_id
            WHERE p.status='active' AND s.ok AND s.price IS NOT NULL
            ORDER BY s.product_id, s.captured_date DESC
        """).fetchall()
    return rows

def build_table(in_stock_only=True):
    rows = latest()
    groups = {}
    skipped_parts = 0
    skipped_names = []
    for r in rows:
        # отключённые площадки в сводку не попадают
        if r["source"] not in SOURCES or r["source"] in OFF:
            continue
        if is_part(r["name"] or ""):
            skipped_parts += 1
            if len(skipped_names) < 40:
                skipped_names.append(r["name"] or "")
            continue
        if in_stock_only and r["in_stock"] is False:
            continue
        key = (r["brand"] or "?", r["model_code"] or (r["name"] or "")[:40].upper())
        groups.setdefault(key, []).append(r)
    out = []
    for (brand, code), rs in groups.items():
        best = min(rs, key=lambda x: float(x["price"]))
        worst = max(rs, key=lambda x: float(x["price"]))
        by = {}
        for r in rs:
            s = r["source"]
            if s not in by or float(r["price"]) < float(by[s]["price"]):
                by[s] = r
        out.append({
            "brand": brand, "code": code,
            "name": best["name"] or code,
            "sources": by,
            "n": len(by),
            "min": float(best["price"]), "min_src": best["source"], "min_url": best["url"],
            "max": float(worst["price"]),
            "spread": float(worst["price"]) - float(best["price"]),
            "spread_pct": (float(worst["price"]) - float(best["price"])) / float(best["price"]) * 100,
        })
    out.sort(key=lambda x: (-x["n"], -x["spread_pct"]))
    (PUB / "parts.txt").write_text(
        "отсеяно как запчасти и принадлежности: %d\nпримеры (до 40):\n%s\n"
        % (skipped_parts, "\n".join(skipped_names)), encoding="utf-8")
    return out

def cmd_compare(in_stock="1"):
    init_db()
    tbl = build_table(in_stock_only=(str(in_stock) != "0"))
    import csv
    names = [s for s in SOURCES if s not in OFF]
    with (PUB / "compare.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["Бренд", "Код модели", "Название", "Площадок"] +
                   [SOURCES[s]["title"] for s in names] +
                   ["Минимум", "У кого", "Разброс, ₽", "Разброс, %", "Ссылка на минимум"])
        for t in tbl:
            w.writerow([t["brand"], t["code"], t["name"], t["n"]] +
                       [("%.0f" % float(t["sources"][s]["price"])) if s in t["sources"] else ""
                        for s in names] +
                       ["%.0f" % t["min"], SOURCES[t["min_src"]]["title"],
                        "%.0f" % t["spread"], "%.1f" % t["spread_pct"], t["min_url"]])
    multi = [t for t in tbl if t["n"] > 1]
    generated = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    payload = {"generated": generated,
               "brands": sorted(set(t["brand"] for t in tbl)),
               "positions": len(tbl), "comparable": len(multi),
               "rows": [{**{k: v for k, v in t.items() if k != "sources"},
                         "offers": [{"source": source, "price": float(offer["price"]),
                                     "url": offer["url"], "in_stock": offer["in_stock"],
                                     "match_confidence": "exact", "usable": True}
                                    for source, offer in t["sources"].items()]}
                        for t in tbl]}
    json.dump(payload, (PUB / "compare.json").open("w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("compare: позиций %d, сравнимых %d" % (len(tbl), len(multi)))
    push_result = push_snapshot(generated=generated)
    archive_compare_run(tbl, payload, push_result)
    return tbl

def run_id_from(generated):
    return re.sub(r"[^0-9A-Za-z]+", "-", generated).strip("-")

def _post_snapshot(src, generated, run_id):
    url = env("PM_PRODUCT_CENTER_URL").strip()
    tok = env("PM_PRODUCT_CENTER_TOKEN").strip()
    waits = (1, 2, 4, 8, 16)
    last = None
    for attempt, wait in enumerate(waits, 1):
        try:
            r = httpx.post(url, content=src.read_bytes(), headers={
                "Authorization": "Bearer " + tok,
                "Content-Type": "text/csv; charset=utf-8",
                "Idempotency-Key": run_id,
                "X-Filename": src.name,
                "X-Snapshot-Generated": generated,
                "User-Agent": "BIO-price-monitor/2.0",
            }, timeout=60)
            r.raise_for_status()
            return {"ok": True, "status": r.status_code, "attempts": attempt,
                    "response": r.text[:1000], "run_id": run_id}
        except Exception as e:
            last = repr(e)[:500]
            if attempt < len(waits):
                time.sleep(wait)
    return {"ok": False, "attempts": len(waits), "error": last, "run_id": run_id}

def _queue_snapshot(src, generated, run_id):
    csv_path = PUSH_QUEUE / (run_id + ".csv")
    meta_path = PUSH_QUEUE / (run_id + ".json")
    csv_path.write_bytes(src.read_bytes())
    meta_path.write_text(json.dumps({"run_id": run_id, "generated": generated,
                                     "csv": csv_path.name}, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")

def drain_push_queue():
    delivered = []
    for meta_path in sorted(PUSH_QUEUE.glob("*.json")):
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        csv_path = PUSH_QUEUE / meta["csv"]
        if not csv_path.is_file():
            continue
        result = _post_snapshot(csv_path, meta["generated"], meta["run_id"])
        if not result["ok"]:
            break
        csv_path.unlink()
        meta_path.unlink()
        delivered.append(meta["run_id"])
    return delivered

def push_snapshot(generated=None):
    """Отправляет утренний compare.csv в BIO Product Center по защищённому push-контракту."""
    url = env("PM_PRODUCT_CENTER_URL").strip()
    tok = env("PM_PRODUCT_CENTER_TOKEN").strip()
    src = PUB / "compare.csv"
    status = PUB / "product-center-push.txt"
    if not (url and tok):
        status.write_text("push не настроен\n%s\n" % dt.datetime.now().isoformat(timespec="seconds"), encoding="utf-8")
        return False
    if not src.is_file():
        status.write_text("compare.csv не найден\n%s\n" % dt.datetime.now().isoformat(timespec="seconds"), encoding="utf-8")
        return False
    generated = generated or dt.datetime.now().astimezone().isoformat(timespec="seconds")
    run_id = run_id_from(generated)
    delivered = drain_push_queue()
    result = _post_snapshot(src, generated, run_id)
    result["delivered_from_queue"] = delivered
    if result["ok"]:
        status.write_text("успешно: HTTP %d, попыток %d\n%s\n%s\n" %
                          (result["status"], result["attempts"], generated, result["response"]), encoding="utf-8")
        print("product-center push: HTTP", result["status"])
    else:
        _queue_snapshot(src, generated, run_id)
        status.write_text("отложено в очередь после %d попыток: %s\n%s\n" %
                          (result["attempts"], result["error"], generated), encoding="utf-8")
        print("product-center push queued:", result["error"])
    return result

def archive_compare_run(tbl, payload, push_result):
    """Сохраняет сырой рынок, нормализованный снимок и отчёт запуска без внутренних цен."""
    day = payload["generated"][:10]
    raw = {"generated": payload["generated"], "offers": []}
    for item in tbl:
        for source, offer in item["sources"].items():
            raw["offers"].append({
                "brand": item["brand"], "model_code": item["code"], "name": item["name"],
                "source": source, "price": float(offer["price"]), "url": offer["url"],
                "seen_at": str(offer["captured_date"]), "in_stock": offer["in_stock"],
            })
    with gzip.open(ARCHIVE_RAW / (day + ".json.gz"), "wt", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, separators=(",", ":"))
    (ARCHIVE_PRICES / (day + ".json")).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = {
        "run_id": push_result.get("run_id"), "generated": payload["generated"],
        "agent_version": "v42", "positions": payload["positions"],
        "comparable": payload["comparable"], "push": push_result,
    }
    (ARCHIVE_RUNS / (day + ".json")).write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

# ---------- утреннее письмо ----------
def coverage_line():
    """Главная строка письма: сколько наших позиций проверено и сколько не нашлось."""
    try:
        with db() as c:
            r = c.execute(
                "SELECT count(DISTINCT watch) codes, count(*) cards,"
                " count(*) FILTER (WHERE last_seen > now() - interval '30 hours') fresh"
                " FROM product WHERE status='active' AND watch IS NOT NULL").fetchone()
    except Exception:
        return "Список наших позиций не задан — смотрим всё, что нашли на площадках."
    if not r or not r["codes"]:
        return ("Список наших позиций не задан — смотрим всё, что нашли на площадках. "
                "Пока это не наличие, а весь каталог площадок.")
    miss = 0
    f = PUB / "watch.txt"
    if f.is_file():
        m = re.search(r"не нашлось нигде:\s*(\d+)", f.read_text(encoding="utf-8"))
        miss = int(m.group(1)) if m else 0
    return ("<b>Наши позиции в наличии: %d.</b> Карточек по ним на площадках %d, "
            "из них проверено за сутки %d. Не нашлось ни на одной площадке: %d."
            % (r["codes"] + miss, r["cards"], r["fresh"], miss))

def cmd_report(in_stock="1"):
    tbl = cmd_compare(in_stock)
    multi = [t for t in tbl if t["n"] > 1]
    names = [s for s in SOURCES if s not in OFF]
    date = dt.date.today().strftime("%d.%m")
    per_brand = {}
    for t in tbl:
        per_brand.setdefault(t["brand"], []).append(t)
    subj = "Цены %s · %s" % (date, " · ".join(
        "%s %d" % (b, len(v)) for b, v in sorted(per_brand.items())))

    def head():
        return "".join("<th align=right>%s</th>" % SOURCES[s]["title"] for s in names)

    def body(rs):
        out = []
        for t in rs[:200]:
            cells = []
            for s in names:
                if s in t["sources"]:
                    p = float(t["sources"][s]["price"])
                    hit = " style='background:#E8F5E9;font-weight:600'" if s == t["min_src"] else ""
                    cells.append("<td align=right%s>%s</td>" % (hit, "{:,.0f}".format(p).replace(",", " ")))
                else:
                    cells.append("<td align=right style='color:#C7CFD3'>—</td>")
            out.append("<tr><td><a href='%s' style='color:#12181B'>%s</a><br>"
                       "<small style='color:#6B7C84'>%s</small></td>%s"
                       "<td align=right>%s</td></tr>"
                       % (t["min_url"], t["name"][:70], t["code"], "".join(cells),
                          "+%.0f%%" % t["spread_pct"] if t["spread_pct"] else "—"))
        return "".join(out) or "<tr><td colspan=9>нет данных</td></tr>"

    def table(rs):
        return ("<table width='100%%' cellpadding='6' "
                "style='border-collapse:collapse;font-size:13px'>"
                "<tr style='background:#F2F4F5'><th align=left>Модель</th>%s"
                "<th align=right>Разброс</th></tr>%s</table>" % (head(), body(rs)))

    blocks = ""
    for b, rs in sorted(per_brand.items()):
        m = [t for t in rs if t["n"] > 1]
        one = [t for t in rs if t["n"] == 1]
        blocks += ("<h3 style='margin:24px 0 6px'>%s — %d позиций, из них на нескольких "
                   "площадках %d</h3>%s" % (b.capitalize(), len(rs), len(m), table(m)))
        if one:
            blocks += ("<p style='color:#6B7C84;margin:14px 0 4px;font-size:13px'>"
                       "%s: только на одной площадке — %d позиций, сравнивать не с чем</p>%s"
                       % (b.capitalize(), len(one), table(one)))

    html = ("<html><body style=\"font-family:Arial,sans-serif;font-size:14px;color:#12181B\">"
            "<h2 style='margin:0 0 4px'>Мониторинг цен — %s</h2>"
            "<p style='color:#12181B;margin:0 0 6px'>%s</p>"
            "<p style='color:#6B7C84;margin:0'>Позиций в наличии: %d. Есть с чем сравнить "
            "(две площадки и больше): %d. Зелёным — самая низкая цена.</p>%s"
            "<p style='color:#6B7C84;font-size:12px;margin-top:24px'>"
            "Полная таблица: compare.csv и compare.json на сервере · %s</p>"
            "</body></html>" % (date, coverage_line(), len(tbl), len(multi), blocks,
                                dt.datetime.now().strftime("%Y-%m-%d %H:%M")))
    (PUB / "report.html").write_text(html, encoding="utf-8")
    send_mail(subj, html)
    print("report:", subj)

# сервер отправки подставляем по адресу отправителя
SMTP_BY_DOMAIN = {
    "gmail.com": ("smtp.gmail.com", 465),
    "yandex.ru": ("smtp.yandex.ru", 465), "ya.ru": ("smtp.yandex.ru", 465),
    "mail.ru": ("smtp.mail.ru", 465), "bk.ru": ("smtp.mail.ru", 465),
    "list.ru": ("smtp.mail.ru", 465), "inbox.ru": ("smtp.mail.ru", 465),
}

def smtp_target():
    user = env("PM_SMTP_USER").strip()
    dom = user.rsplit("@", 1)[-1].lower() if "@" in user else ""
    host, port = SMTP_BY_DOMAIN.get(dom, (env("PM_SMTP_HOST"), int(env("PM_SMTP_PORT", "465") or 465)))
    pwd = env("PM_SMTP_PASS").strip()
    # пароль приложения Google — ровно 16 букв, разделители убираем
    cleaned = re.sub(r"[^A-Za-z0-9]", "", pwd)
    if len(cleaned) == 16 and len(pwd) != 16:
        pwd = cleaned
    return host, port, user, pwd, env("PM_MAIL_TO").strip()

def send_mail(subject, html):
    import smtplib
    from email.message import EmailMessage
    host, port, user, pwd, to = smtp_target()
    if not (host and user and pwd and to):
        print("SMTP не настроен — письмо не отправлено, отчёт лежит в pub/report.html")
        return False
    m = EmailMessage()
    m["Subject"] = subject
    m["From"] = user
    m["To"] = to
    m.set_content("Отчёт в формате HTML.")
    m.add_alternative(html, subtype="html")
    try:
        with smtplib.SMTP_SSL(host, port, timeout=40) as s:
            s.login(user, pwd)
            s.send_message(m)
        print("письмо отправлено на", to)
        return True
    except Exception as e:
        msg = "%s: %s" % (type(e).__name__, str(e)[:300])
        print("письмо не ушло:", msg)
        return msg


_LOG = []

def log(t):
    """Протокол разведки — накапливаем и сразу кладём в /pub/discovery.txt."""
    _LOG.append(str(t))
    print(t)
    (PUB / "discovery.txt").write_text("\n".join(_LOG) + "\n", encoding="utf-8")

def _step(msg):
    with (PUB / "steps.txt").open("a", encoding="utf-8") as f:
        f.write("%s %s\n" % (msg, dt.datetime.now().isoformat(timespec="seconds")))


RUN_OK = {"daily", "discover", "report", "compare", "secure", "stats", "watch",
          "rate", "policy", "calculate"}

def _why(t):
    (PUB / "run.txt").write_text("%s\n%s\n" % (t, dt.datetime.now().isoformat(timespec="seconds")),
                                 encoding="utf-8")

def _maybe_run(ci):
    m = re.search(r"^#\s*PM_RUN:\s*(.+)$", ci, re.M)
    if not m:
        return _why("строки PM_RUN в cloud-init нет")
    line = m.group(0)
    tag = hashlib.sha256(line.encode()).hexdigest()[:10]
    mark = BASE / (".run-" + tag)
    if mark.exists():
        return _why("уже выполнялось: %s (метка %s)" % (line, tag))
    parts = m.group(1).split("#")[0].split()
    if not parts or parts[0] not in RUN_OK:
        mark.write_text("skip")
        return _why("команда не разрешена: %s" % line)
    _why("запускаю: %s" % line)
    _step("RUN_" + parts[0])
    # Метка создаётся только главным диспетчером после успешного завершения.
    # Иначе NameError внутри команды навсегда маскируется как выполненная команда.
    os.environ["PM_RUN_SUCCESS_MARK"] = str(mark)
    os.execv(sys.executable, [sys.executable, str(BASE / "app.py")] + parts)

def _absorb_secrets(ci):
    """Переносим строки «# PM_...:» из cloud-init в .env и убираем из памяти."""
    got = {}
    for key in ("PM_SMTP_USER", "PM_SMTP_PASS", "PM_MAIL_TO",
                "PM_GIT_URL", "PM_GIT_TOKEN",
                "PM_PRODUCT_CENTER_URL", "PM_PRODUCT_CENTER_TOKEN",
                "PM_WATCH_URL", "PM_WATCH_TOKEN", "PM_POLICY_URL", "PM_READ_TOKEN"):
        # берём последнюю такую строку
        vals = [v.strip() for v in re.findall(r"^#\s*%s:[ \t]*(.+?)[ \t]*$" % key, ci, re.M)]
        vals = [v for v in vals if v and not v.startswith("__")]
        if vals:
            got[key] = vals[-1]
    if not got:
        return
    envf = BASE / ".env"
    try:
        lines = envf.read_text(encoding="utf-8").splitlines()
    except Exception:
        lines = []
    cur = {}
    for ln in lines:
        if "=" in ln and not ln.lstrip().startswith("#"):
            k, v = ln.split("=", 1)
            cur[k.strip()] = v
    if all(cur.get(k) == v for k, v in got.items()):
        for k, v in got.items():
            os.environ[k] = v
        return
    keep = [ln for ln in lines if ln.split("=", 1)[0].strip() not in got]
    envf.write_text("\n".join(keep + ["%s=%s" % (k, v) for k, v in got.items()]) + "\n",
                    encoding="utf-8")
    for k, v in got.items():
        os.environ[k] = v
    _step("ENV_SET_" + "_".join(sorted(got)))

def _absorb_watch(ci):
    """Список наших позиций приходит строками «# PM_WATCH:» — он приватный,
    поэтому живёт в настройках сервера, а не в репозитории."""
    vals = [v.strip() for v in re.findall(r"^#\s*PM_WATCH:[ \t]*(.+?)[ \t]*$", ci, re.M)]
    vals = [v for v in vals if v and not v.startswith("__")]
    if not vals:
        return
    codes = []
    for v in vals:
        codes += [c.strip() for c in re.split(r"[,;\s]+", v) if c.strip()]
    if not codes:
        return
    f = BASE / "watch.txt"
    new = "\n".join(codes) + "\n"
    if f.is_file() and f.read_text(encoding="utf-8") == new:
        return
    f.write_text(new, encoding="utf-8")
    _step("WATCH_SET_%d" % len(codes))

TW_API = "https://api.timeweb.cloud/api/v1/servers/%s"

# ---------- наш список наличия ----------
def flat(s):
    """Код без пробелов и дефисов: XEBC-06EU → XEBC06EU. Кириллицу приводим к латинице."""
    s = norm_code(s)
    for a, b in zip("АВЕКМНОРСТУХ", "ABEKMHOPCTYX"):
        s = s.replace(a, b)
    return re.sub(r"[^A-Z0-9]", "", s)

def code_matches(code, model, name):
    """Коды короче шести знаков сопоставляются только целиком."""
    if not code or not model:
        return False
    if len(code) < 6:
        return model == code
    return (model == code
            or (model.startswith(code) and len(model) <= len(code) + 4)
            or (code.startswith(model) and len(model) >= 5 and len(code) <= len(model) + 2)
            or code in name)

def watch_codes():
    """Список наших позиций: сперва Product Center, иначе файл на сервере."""
    url = env("PM_WATCH_URL")
    if url:
        h = {"Accept": "application/json"}
        tok = env("PM_WATCH_TOKEN")
        if tok:
            h["Authorization"] = "Bearer " + tok
        r = httpx.get(url, headers=h, timeout=30)
        r.raise_for_status()
        try:
            data = r.json()
        except Exception:
            return [x.strip() for x in r.text.splitlines() if x.strip()], "адрес (текст)"
        items = data.get("items", data if isinstance(data, list) else [])
        out = []
        for it in items:
            code = it.get("internal_code") or it.get("article") or it.get("code") or ""
            if code.strip():
                out.append(code.strip())
        return out, "Product Center"
    f = BASE / "watch.txt"
    if f.is_file():
        return [x.strip() for x in f.read_text(encoding="utf-8").splitlines()
                if x.strip() and not x.startswith("#")], "файл на сервере"
    return [], "источника нет"

def cmd_watch():
    """Отмечаем в базе карточки, которые соответствуют нашим позициям в наличии."""
    codes, src = watch_codes()
    if not codes:
        (PUB / "watch.txt").write_text(
            "список наших позиций пуст (%s) — мониторинг идёт по всем карточкам\n%s\n"
            % (src, dt.datetime.now().isoformat(timespec="seconds")), encoding="utf-8")
        print("watch: список пуст (%s)" % src)
        return
    hit, miss, per_src = {}, [], {}
    with db() as c:
        c.execute("UPDATE product SET watch=NULL WHERE watch IS NOT NULL")
        rows = c.execute("SELECT id, source, name, model_code FROM product"
                         " WHERE status='active'").fetchall()
        idx = [(r, flat(r["model_code"] or ""), flat(r["name"] or "")) for r in rows]
        for code in codes:
            f = flat(code)
            if len(f) < 4:
                miss.append(code + " (код короче четырёх знаков — не ищу)")
                continue
            # точное совпадение; либо код модели начинается с нашего и дописан
            # немногим (D4VM4 → D4VM4001), но не разросся (GC111 → GC1110VV061);
            # либо длинный код целиком встречается в названии
            found = [r for r, mc, nm in idx if code_matches(f, mc, nm)]
            if not found:
                # подсказка: что похожее вообще есть на площадках — сразу видно,
                # товара нет в продаже или это мы не сумели сопоставить код
                near = [r["name"] for r, mc, nm in idx if mc and mc[:4] == f[:4]][:3]
                miss.append(code + (" → рядом: " + "; ".join(n[:60] for n in near)
                                    if near else " → похожего нет"))
                continue
            hit[code] = len(found)
            for r in found:
                per_src[r["source"]] = per_src.get(r["source"], 0) + 1
                c.execute("UPDATE product SET watch=%s WHERE id=%s", (code, r["id"]))
    (PUB / "watch.txt").write_text(
        "наш список: %d позиций (источник: %s)\n"
        "нашлось карточек хотя бы на одной площадке: %d\n"
        "не нашлось нигде: %d\n\nпо площадкам:\n%s\n\nне нашлось:\n%s\n%s\n"
        % (len(codes), src, len(hit), len(miss),
           "\n".join("  %-14s %d" % (k, v) for k, v in sorted(per_src.items())),
           "\n".join("  " + m for m in miss),
           dt.datetime.now().isoformat(timespec="seconds")), encoding="utf-8")
    print("watch: %d кодов, найдено %d, не найдено %d" % (len(codes), len(hit), len(miss)))

def cmd_stats():
    """Сколько карточек и сколько из них в списке — по площадкам и брендам."""
    with db() as c:
        rows = c.execute(
            "SELECT source, coalesce(brand,'?') b, count(*) n, count(watch) w,"
            " count(*) FILTER (WHERE model_code IS NOT NULL) mc"
            " FROM product WHERE status='active' GROUP BY 1,2 ORDER BY 1,2").fetchall()
        pr = c.execute(
            "SELECT p.source, coalesce(p.brand,'?') b, count(DISTINCT p.id) n"
            " FROM product p JOIN price_snapshot s ON s.product_id=p.id"
            " WHERE s.ok AND s.price IS NOT NULL GROUP BY 1,2 ORDER BY 1,2").fetchall()
    have = {(x["source"], x["b"]): x["n"] for x in pr}
    out = ["площадка   бренд    карточек  в списке  с кодом  с ценой"]
    out += ["%-10s %-8s %8d %9d %8d %8d"
            % (x["source"], x["b"], x["n"], x["w"], x["mc"],
               have.get((x["source"], x["b"]), 0)) for x in rows]
    (PUB / "stats.txt").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("\n".join(out))

def cmd_secure():
    """Убираем из публичной папки то, чего там быть не должно."""
    moved = []
    for f in ("db.sql.gz", "catalog.json", "catalog.xlsx"):
        p = PUB / f
        if p.is_file():
            p.rename(DATA / f)
            moved.append(f)
    # каталог вместо файла: ночной дамп больше сюда не запишется
    d = PUB / "db.sql.gz"
    if not d.exists():
        d.mkdir()
    (PUB / "secure.txt").write_text(
        "перенесено из публичной папки: %s\n%s\n" %
        (", ".join(moved) or "нечего", dt.datetime.now().isoformat(timespec="seconds")),
        encoding="utf-8")
    print("secure:", moved)

def cmd_selfupdate():
    sid, tok = env("PM_TW_SERVER_ID"), env("PM_TW_TOKEN")
    if not (sid and tok):
        return
    try:
        r = httpx.get(TW_API % sid, headers={"Authorization": "Bearer " + tok}, timeout=30)
        ci = r.json().get("server", {}).get("cloud_init") or ""
    except Exception as e:
        print("SU_ERR", repr(e)[:120])
        return
    _absorb_secrets(ci)
    _absorb_watch(ci)
    code = None
    gu = env("PM_GIT_URL")
    if gu:
        try:
            h = {}
            gt = env("PM_GIT_TOKEN")
            if gt:
                h["Authorization"] = "Bearer " + gt
            g = httpx.get(gu, headers=h, timeout=30)
            g.raise_for_status()
            ast.parse(g.content)
            code = g.content
            (PUB / "git.txt").write_text(
                "код берётся из git\nадрес: %s\nразмер: %d байт\nключ: %s\n%s\n"
                % (re.sub(r"://[^@]*@", "://", gu.split("?")[0]), len(code),
                   "задан (%d знаков)" % len(gt) if gt else "не нужен",
                   dt.datetime.now().isoformat(timespec="seconds")), encoding="utf-8")
        except Exception as e:
            print("SU_GITERR", repr(e)[:120])
    if code is None:
        m = re.search(r"<<'B64'\n(.*?)\nB64\n", ci, re.S)
        if not m:
            return
        try:
            code = gzip.decompress(base64.b64decode(m.group(1)))
            ast.parse(code)
        except Exception as e:
            print("SU_BADCODE", repr(e)[:120])
            return
    me = BASE / "app.py"
    cur = me.read_bytes() if me.exists() else b""
    # в настройках сервера лежит короткая заглушка-загрузчик: она не должна
    # затирать полную программу, если репозиторий на минуту оказался недоступен
    if len(code) < 5000 and len(cur) > 20000:
        print("SU_SKIP_STUB", len(code), len(cur))
        _maybe_run(ci)
        return
    if hashlib.sha256(cur).digest() == hashlib.sha256(code).digest():
        _maybe_run(ci)
        return
    me.write_bytes(code)
    _step("SU_UPDATED_" + hashlib.sha256(code).hexdigest()[:8])
    os.execv(sys.executable, [sys.executable, str(me), "postupdate"])

def cmd_postupdate():
    sid, tok = env("PM_TW_SERVER_ID"), env("PM_TW_TOKEN")
    if not (sid and tok):
        return
    r = httpx.get(TW_API % sid, headers={"Authorization": "Bearer " + tok}, timeout=30)
    r.raise_for_status()
    ci = r.json().get("server", {}).get("cloud_init") or ""
    _absorb_secrets(ci)
    _maybe_run(ci)

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    args = sys.argv[2:]
    fn = {"discover": cmd_discover, "daily": cmd_daily, "compare": cmd_compare,
          "report": cmd_report, "init": init_db, "secure": cmd_secure,
          "stats": cmd_stats, "watch": cmd_watch,
          "rate": cmd_rate, "policy": cmd_policy, "calculate": cmd_calculate,
          "selfupdate": cmd_selfupdate, "postupdate": cmd_postupdate,
          # старые таймеры с каталожных времён — чтобы не падали, ведут на новое
          "crawl": cmd_daily, "export": cmd_compare,
          "media": lambda *a: print("media больше не нужна — каталог собран")}.get(cmd)
    if not fn:
        print("неизвестная команда:", cmd)
        sys.exit(2)
    # любая поломка должна быть видна снаружи, а не только в системном журнале
    try:
        fn(*args)
        success_mark = env("PM_RUN_SUCCESS_MARK").strip()
        if success_mark:
            Path(success_mark).write_text(
                "%s\n%s\n" % (cmd, dt.datetime.now().isoformat(timespec="seconds")),
                encoding="utf-8")
        (PUB / ("last-%s.txt" % cmd)).write_text(
            "%s %s — успешно\n%s\n" % (cmd, " ".join(args),
                                       dt.datetime.now().isoformat(timespec="seconds")),
            encoding="utf-8")
    except Exception:
        (PUB / ("last-%s.txt" % cmd)).write_text(
            "%s %s — ОШИБКА\n%s\n\n%s" % (cmd, " ".join(args),
                                           dt.datetime.now().isoformat(timespec="seconds"),
                                           traceback.format_exc()[-2000:]),
            encoding="utf-8")
        raise
