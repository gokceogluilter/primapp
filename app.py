import csv
import io
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from flask import Flask, request, redirect, url_for, render_template_string, abort, flash, Response
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = Path("primapp.sqlite3")

app = Flask(__name__)
app.secret_key = "CHANGE_ME__REPLACE_WITH_LONG_RANDOM_SECRET"

login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)

ADMIN_USERNAME = "ilter"
DEFAULT_ADMIN_PASSWORD = "Specifo1"

# --- DEFAULT RATES (admin settings ekranından değişir, yüzde olarak girilir) ---
DEFAULT_STANDARD_RATE_PCT = 3.0   # %3
DEFAULT_PROJECT_RATE_PCT = 3.0    # %3
DEFAULT_OVERRIDE_RATE_PCT = 1.0   # Nilüfer override %1 (Pınar/Burcu)
MIN_MARGIN = 0.20                # %20

LOCAL_TL_KEYS = [
    "ic_nakliye_tl",
    "gumruk_vergisi_tl",
    "ek_gumruk_tl",
    "gumruk_masraf_tl",
    "komisyon_tl",
    "diger_tl",
]

# ---------- helpers ----------
def parse_date(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None

def safe_float(s: str) -> Optional[float]:
    s = (s or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def eur_from_tl(tl: Optional[float], kur: Optional[float]) -> float:
    if tl is None or kur is None or kur == 0:
        return 0.0
    return tl / kur

def quarter_from_iso(iso_date: Optional[str]) -> Optional[str]:
    if not iso_date:
        return None
    dt = datetime.fromisoformat(iso_date).date()
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{q}"

def current_quarter() -> str:
    today = date.today()
    q = (today.month - 1) // 3 + 1
    return f"{today.year}-Q{q}"

def later_date(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if not a and not b:
        return None
    if a and not b:
        return a
    if b and not a:
        return b
    return max(a, b)

def pct_to_rate(pct: float) -> float:
    return (pct or 0.0) / 100.0

# ---------- DB ----------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db():
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT UNIQUE NOT NULL,
              full_name TEXT NOT NULL,
              password_hash TEXT NOT NULL,
              role TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sales (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sale_id TEXT,
              seller_username TEXT NOT NULL,
              customer TEXT,
              job_type TEXT, -- Proje / Standart
              invoice_no TEXT,

              sale_date TEXT,
              collection_done_date TEXT,
              delivery_done_date TEXT,

              sale_eur REAL,
              purchase_eur REAL,
              international_shipping_eur REAL,

              local_kur REAL,

              ic_nakliye_tl REAL,
              gumruk_vergisi_tl REAL,
              ek_gumruk_tl REAL,
              gumruk_masraf_tl REAL,
              komisyon_tl REAL,
              diger_tl REAL,

              notes TEXT,
              created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sales_seller ON sales(seller_username);
            CREATE INDEX IF NOT EXISTS idx_sales_dates ON sales(collection_done_date, delivery_done_date);

            CREATE TABLE IF NOT EXISTS settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS targets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              quarter TEXT NOT NULL,
              seller_username TEXT NOT NULL,
              target_eur REAL NOT NULL,
              UNIQUE(quarter, seller_username)
            );
            """
        )
        conn.commit()

def ensure_defaults():
    with db() as conn:
        cnt = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        if cnt == 0:
            users = [
                (ADMIN_USERNAME, "İlter", DEFAULT_ADMIN_PASSWORD, "admin"),
                ("nilufer", "Nilüfer", "nilufer123", "seller"),
                ("pinar", "Pınar", "pinar123", "seller"),
                ("burcu", "Burcu", "burcu123", "seller"),
            ]
            for u, name, pw, role in users:
                conn.execute(
                    "INSERT INTO users (username, full_name, password_hash, role) VALUES (?,?,?,?)",
                    (u, name, generate_password_hash(pw), role),
                )

        def set_if_missing(k: str, v: str):
            r = conn.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
            if not r:
                conn.execute("INSERT INTO settings (key,value) VALUES (?,?)", (k, v))

        # yüzde olarak saklıyoruz (örn 3 = %3)
        set_if_missing("standard_rate_pct", str(DEFAULT_STANDARD_RATE_PCT))
        set_if_missing("project_rate_pct", str(DEFAULT_PROJECT_RATE_PCT))
        set_if_missing("override_rate_pct", str(DEFAULT_OVERRIDE_RATE_PCT))

        conn.commit()

# ---------- Auth ----------
@dataclass
class User(UserMixin):
    id: int
    username: str
    full_name: str
    role: str

@login_manager.user_loader
def load_user(user_id: str):
    with db() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        return None
    return User(id=row["id"], username=row["username"], full_name=row["full_name"], role=row["role"])

def is_admin() -> bool:
    return getattr(current_user, "role", "") == "admin" and getattr(current_user, "username", "") == ADMIN_USERNAME

# ---------- Settings ----------
def get_settings() -> Dict[str, float]:
    with db() as conn:
        rows = conn.execute("SELECT key,value FROM settings").fetchall()
    out: Dict[str, float] = {}
    for r in rows:
        try:
            out[r["key"]] = float(r["value"])
        except Exception:
            out[r["key"]] = 0.0
    return out

def get_rates() -> Tuple[float, float, float]:
    s = get_settings()
    std_pct = float(s.get("standard_rate_pct", DEFAULT_STANDARD_RATE_PCT))
    prj_pct = float(s.get("project_rate_pct", DEFAULT_PROJECT_RATE_PCT))
    ovr_pct = float(s.get("override_rate_pct", DEFAULT_OVERRIDE_RATE_PCT))
    return std_pct, prj_pct, ovr_pct

# ---------- Target performance ----------
def fetch_targets() -> Dict[Tuple[str, str], float]:
    with db() as conn:
        rows = conn.execute("SELECT quarter, seller_username, target_eur FROM targets").fetchall()
    t: Dict[Tuple[str, str], float] = {}
    for r in rows:
        t[(r["seller_username"], r["quarter"])] = float(r["target_eur"] or 0.0)
    return t

def fetch_eligible_sales_totals() -> Dict[Tuple[str, str], float]:
    """
    totals by seller_username and quarter based on hakediş date = max(collection_done_date, delivery_done_date)
    and only when both dates present.
    """
    sql = """
    WITH elig AS (
      SELECT
        seller_username,
        sale_eur,
        max(collection_done_date, delivery_done_date) AS hak_date,
        CAST(substr(max(collection_done_date, delivery_done_date), 6, 2) AS INTEGER) AS m,
        substr(max(collection_done_date, delivery_done_date), 1, 4) AS y
      FROM sales
      WHERE
        collection_done_date IS NOT NULL AND collection_done_date <> ''
        AND delivery_done_date IS NOT NULL AND delivery_done_date <> ''
        AND sale_eur IS NOT NULL
    )
    SELECT
      seller_username,
      (y || '-Q' || (CAST(((m-1)/3) AS INTEGER) + 1)) AS quarter,
      SUM(sale_eur) AS total_sales
    FROM elig
    GROUP BY seller_username, quarter
    """
    with db() as conn:
        rows = conn.execute(sql).fetchall()
    out: Dict[Tuple[str, str], float] = {}
    for r in rows:
        out[(r["seller_username"], r["quarter"])] = float(r["total_sales"] or 0.0)
    return out

def seller_target_factor(seller: str, quarter: Optional[str], totals: Dict[Tuple[str, str], float], targets: Dict[Tuple[str, str], float]) -> float:
    """
    >=100% => 1
    80-100 => 0.5
    <80 => 0
    """
    if not quarter:
        return 0.0
    tgt = targets.get((seller, quarter))
    if not tgt or tgt <= 0:
        return 0.0
    total = totals.get((seller, quarter), 0.0)
    ratio = total / tgt if tgt > 0 else 0.0
    if ratio >= 1.0:
        return 1.0
    if ratio >= 0.8:
        return 0.5
    return 0.0

# ---------- Metrics ----------
def commission_rate(margin: Optional[float], job_type: str, std_pct: float, prj_pct: float) -> float:
    """
    Standart: margin < %20 => 0, değilse std rate
    Proje: prj rate, margin %20 altı ise oransal düşer
    """
    if margin is None:
        return 0.0
    jt = (job_type or "").strip().lower()
    std = pct_to_rate(std_pct)
    prj = pct_to_rate(prj_pct)

    if jt == "standart":
        if margin < MIN_MARGIN:
            return 0.0
        return std

    # Proje / diğer
    if margin >= MIN_MARGIN:
        return prj
    return prj * (margin / MIN_MARGIN) if MIN_MARGIN > 0 else 0.0

def compute_sale_metrics(r: sqlite3.Row, totals: Dict[Tuple[str, str], float], targets: Dict[Tuple[str, str], float]) -> Dict[str, Any]:
    std_pct, prj_pct, ovr_pct = get_rates()

    sale_eur = float(r["sale_eur"] or 0.0)
    purchase_eur = float(r["purchase_eur"] or 0.0)
    intl = float(r["international_shipping_eur"] or 0.0)
    local_kur = r["local_kur"]

    local_sum = 0.0
    for tl_key in LOCAL_TL_KEYS:
        local_sum += eur_from_tl(r[tl_key], local_kur)

    total_cost = purchase_eur + intl + local_sum
    profit = sale_eur - total_cost
    margin = (profit / sale_eur) if sale_eur > 0 else None

    eligible = bool(r["collection_done_date"]) and bool(r["delivery_done_date"])
    hak_tarih = later_date(r["collection_done_date"], r["delivery_done_date"]) if eligible else None
    quarter = quarter_from_iso(hak_tarih) if hak_tarih else None

    rate = commission_rate(margin, r["job_type"] or "", std_pct, prj_pct)
    factor = seller_target_factor(r["seller_username"], quarter, totals, targets)

    seller_comm = (sale_eur * rate * factor) if eligible else 0.0

    # Nilüfer override only on Pınar/Burcu AND their target factor applies
    override = 0.0
    if eligible and r["seller_username"] in ("pinar", "burcu"):
        override = sale_eur * pct_to_rate(ovr_pct) * factor

    # form uyarısı için neden prim doğar/doğmaz:
    reason = ""
    if not eligible:
        reason = "Hakediş yok (tahsilat + teslim tamam değil)."
    elif quarter is None:
        reason = "Hakediş çeyreği bulunamadı."
    else:
        tgt = targets.get((r["seller_username"], quarter))
        if not tgt or tgt <= 0:
            reason = "Bu çeyrek hedefi girilmedi (hedef yoksa prim yok)."
        elif factor == 0.0:
            reason = "Hedef %80 altı (prim yok)."
        else:
            if (r["job_type"] or "").strip().lower() == "standart" and (margin is not None) and margin < MIN_MARGIN:
                reason = "Standart işte kârlılık %20 altı (prim yok)."
            else:
                reason = "Prim doğar (hakediş + hedef + kârlılık koşulları sağlandı)."

    return {
        "sale_eur": sale_eur,
        "total_cost": total_cost,
        "profit": profit,
        "margin": margin,
        "eligible": eligible,
        "hak_tarih": hak_tarih,
        "quarter": quarter,
        "target_factor": factor,
        "rate": rate,
        "seller_comm": seller_comm,
        "override": override,
        "reason": reason,
    }

# ---------- UI ----------
BASE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Prim Sistemi</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; gap:16px; }
    .muted { color:#666; font-size: 12px; }
    .card { border:1px solid #ddd; padding:12px; border-radius:10px; margin-bottom:16px; }
    .btn { padding:10px 14px; background:#1F4E79; color:#fff; border:none; border-radius:8px; cursor:pointer; }
    .btn2 { padding:10px 14px; background:#666; color:#fff; border:none; border-radius:8px; cursor:pointer; text-decoration:none; display:inline-block; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; font-size: 13px; }
    th { background:#1F4E79; color:#fff; position: sticky; top: 0; }
    input, select, textarea { width:100%; padding:6px; box-sizing:border-box; }
    .grid { display:grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    .grid2 { display:grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
    .ok { color:#0a7; font-weight:bold; }
    .bad { color:#c00; font-weight:bold; }
    .flash { background:#fff2cc; padding:10px; border-radius:8px; border:1px solid #f0d37a; margin-bottom:10px; }
    .barwrap{background:#eee;border-radius:10px;overflow:hidden;height:14px;}
    .bar{background:#1F4E79;height:14px;}
  </style>
</head>
<body>
  <div class="top">
    <div>
      <div><b>Prim Sistemi</b></div>
      <div class="muted">
        Kur: TL giderler ödeme günündeki <b>Garanti BBVA EUR Satış Kuru</b> ile EUR’a çevrilir (manual).<br>
        Hedef çarpanı: >=100% tam, 80–100 %50, <80 yok. Standart: %20 altı prim yok. Proje: oransal düşer.
      </div>
    </div>
    <div>
      {% if current_user.is_authenticated %}
        <div class="muted">Giriş: <b>{{ current_user.full_name }}</b> ({{ current_user.username }})</div>
        <div style="display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap;margin-top:6px;">
          <a class="btn2" href="{{ url_for('dashboard') }}">Satışlar</a>
          <a class="btn2" href="{{ url_for('summary') }}">Özet</a>
          {% if is_admin %}
            <a class="btn2" href="{{ url_for('settings') }}">Ayarlar</a>
            <a class="btn2" href="{{ url_for('quarterly') }}">Çeyrek Ödeme</a>
          {% endif %}
          <a class="btn2" href="{{ url_for('logout') }}">Çıkış</a>
        </div>
      {% endif %}
    </div>
  </div>

  {% for m in get_flashed_messages() %}
    <div class="flash">{{ m }}</div>
  {% endfor %}

  {{ content|safe }}
</body>
</html>
"""

LOGIN_TPL = """
<div class="card" style="max-width:420px;">
  <form method="post">
    <div style="margin-bottom:10px;"><b>Giriş</b></div>
    <div style="margin-bottom:10px;">
      <label>Kullanıcı adı</label>
      <input name="username" required />
    </div>
    <div style="margin-bottom:10px;">
      <label>Şifre</label>
      <input name="password" type="password" required />
    </div>
    <button class="btn" type="submit">Giriş</button>
    <div class="muted" style="margin-top:10px;">
      İlk şifreler: ilter/Specifo1, nilufer/nilufer123, pinar/pinar123, burcu/burcu123
    </div>
  </form>
</div>
"""

def totals_card(rows: List[Dict[str, Any]]) -> str:
    total_sale = sum(r["m"]["sale_eur"] for r in rows)
    total_cost = sum(r["m"]["total_cost"] for r in rows)
    total_profit = sum(r["m"]["profit"] for r in rows)
    avg_margin = (total_profit / total_sale) if total_sale > 0 else 0.0
    return render_template_string("""
    <div class="card">
      <b>Toplamlar</b>
      <div class="grid2" style="margin-top:8px;">
        <div>Toplam Satış: <b>{{ "%.2f"|format(ts) }}</b> EUR</div>
        <div>Toplam Maliyet: <b>{{ "%.2f"|format(tc) }}</b> EUR</div>
        <div>Toplam Kâr: <b>{{ "%.2f"|format(tp) }}</b> EUR</div>
        <div>Ortalama Kârlılık: <b>{{ (am*100)|round(2) }}</b>%</div>
      </div>
    </div>
    """, ts=total_sale, tc=total_cost, tp=total_profit, am=avg_margin)

DASH_TPL = """
<div class="card">
  <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
    {% if not is_admin %}
      <a class="btn2" href="{{ url_for('new_sale') }}">+ Yeni Satış</a>
    {% endif %}
    {% if is_admin %}
      <a class="btn2" href="{{ url_for('export_csv') }}">CSV Export</a>
    {% endif %}
  </div>
</div>

<div class="card">
  <form method="get" class="grid">
    <div>
      <label>Arama</label>
      <input name="q" value="{{ q }}" placeholder="müşteri, fatura, not, satışçı...">
    </div>
    <div>
      <label>Satışçı</label>
      <select name="seller">
        <option value="">Hepsi</option>
        {% for s in sellers %}
          <option value="{{ s }}" {% if seller==s %}selected{% endif %}>{{ s }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Hakediş</label>
      <select name="eligible">
        <option value="">Hepsi</option>
        <option value="1" {% if eligible=='1' %}selected{% endif %}>Evet</option>
        <option value="0" {% if eligible=='0' %}selected{% endif %}>Hayır</option>
      </select>
    </div>
    <div style="display:flex; align-items:end;">
      <button class="btn" type="submit">Uygula</button>
    </div>
  </form>
</div>

{{ totals_html|safe }}

<div class="card">
  <b>Satışlar</b>
  <table>
    <thead>
      <tr>
        <th>ID</th><th>Satışçı</th><th>Müşteri</th><th>İş</th><th>Fatura</th>
        <th>Satış</th><th>Maliyet</th><th>Kâr</th><th>Kârlılık</th>
        <th>Hakediş</th><th>Çeyrek</th><th>Hedef</th><th>Oran</th><th>Prim</th>
        {% if is_admin %}<th>Nilüfer Ek</th>{% endif %}
        <th></th>
      </tr>
    </thead>
    <tbody>
      {% for r in rows %}
      <tr>
        <td>{{ r["id"] }}</td>
        <td>{{ r["seller_username"] }}</td>
        <td>{{ r["customer"] or "" }}</td>
        <td>{{ r["job_type"] or "" }}</td>
        <td>{{ r["invoice_no"] or "" }}</td>
        <td>{{ "%.2f"|format(r["m"]["sale_eur"]) }}</td>
        <td>{{ "%.2f"|format(r["m"]["total_cost"]) }}</td>
        <td>{{ "%.2f"|format(r["m"]["profit"]) }}</td>
        <td>{{ (r["m"]["margin"]*100)|round(2) if r["m"]["margin"] is not none else "" }}%</td>
        <td class="{{ 'ok' if r['m']['eligible'] else 'bad' }}">{{ "E" if r["m"]["eligible"] else "H" }}</td>
        <td>{{ r["m"]["quarter"] or "" }}</td>
        <td>{{ (r["m"]["target_factor"]*100)|round(0) }}%</td>
        <td>{{ (r["m"]["rate"]*100)|round(2) }}%</td>
        <td>{{ "%.2f"|format(r["m"]["seller_comm"]) }}</td>
        {% if is_admin %}<td>{{ "%.2f"|format(r["m"]["override"]) }}</td>{% endif %}
        <td>
          {% if is_admin %}
            <a class="btn2" href="{{ url_for('view_sale_admin', sale_id=r['id']) }}">Gör</a>
          {% else %}
            <a class="btn2" href="{{ url_for('edit_sale', sale_id=r['id']) }}">Düzenle</a>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
"""

SALE_FORM_TPL = """
<div class="card">
  <div style="display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;">
    <b>{{ title }}</b>
    <a class="btn2" href="{{ url_for('dashboard') }}">Geri</a>
  </div>
  {% if metrics %}
    <div class="muted" style="margin-top:8px;">
      <b>Prim durumu:</b> {{ metrics["reason"] }}
    </div>
  {% endif %}
</div>

<form method="post" class="card">
  <div class="grid">
    <div><label>Satış ID</label><input name="sale_id" value="{{ v.get('sale_id','') }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Müşteri</label><input name="customer" value="{{ v.get('customer','') }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>İş Tipi</label>
      <select name="job_type" {{ 'disabled' if readonly else '' }}>
        <option value="">-</option>
        <option value="Proje" {% if v.get('job_type')=='Proje' %}selected{% endif %}>Proje</option>
        <option value="Standart" {% if v.get('job_type')=='Standart' %}selected{% endif %}>Standart</option>
      </select>
    </div>
    <div><label>Fatura No</label><input name="invoice_no" value="{{ v.get('invoice_no','') }}" {{ 'disabled' if readonly else '' }}></div>
  </div>

  <div class="grid">
    <div><label>Satış Tarihi</label><input type="date" name="sale_date" value="{{ v.get('sale_date') or '' }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Tahsilat Tamam</label><input type="date" name="collection_done_date" value="{{ v.get('collection_done_date') or '' }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Teslim Tamam</label><input type="date" name="delivery_done_date" value="{{ v.get('delivery_done_date') or '' }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Not</label><input name="notes" value="{{ v.get('notes','') }}" {{ 'disabled' if readonly else '' }}></div>
  </div>

  <div class="grid">
    <div><label>Satış EUR</label><input name="sale_eur" value="{{ v.get('sale_eur','') }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Alış EUR</label><input name="purchase_eur" value="{{ v.get('purchase_eur','') }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Dış Nakliye EUR</label><input name="international_shipping_eur" value="{{ v.get('international_shipping_eur','') }}" {{ 'disabled' if readonly else '' }}></div>
    <div><label>Lokal Kur</label><input name="local_kur" value="{{ v.get('local_kur','') }}" {{ 'disabled' if readonly else '' }}></div>
  </div>

  <div class="card">
    <b>Lokal Giderler (TL)</b>
    <div class="grid">
      <div><label>İç Nakliye</label><input name="ic_nakliye_tl" value="{{ v.get('ic_nakliye_tl','') }}" {{ 'disabled' if readonly else '' }}></div>
      <div><label>Gümrük Vergisi</label><input name="gumruk_vergisi_tl" value="{{ v.get('gumruk_vergisi_tl','') }}" {{ 'disabled' if readonly else '' }}></div>
      <div><label>Ek Gümrük</label><input name="ek_gumruk_tl" value="{{ v.get('ek_gumruk_tl','') }}" {{ 'disabled' if readonly else '' }}></div>
      <div><label>Gümrük Masraf</label><input name="gumruk_masraf_tl" value="{{ v.get('gumruk_masraf_tl','') }}" {{ 'disabled' if readonly else '' }}></div>
      <div><label>Komisyon</label><input name="komisyon_tl" value="{{ v.get('komisyon_tl','') }}" {{ 'disabled' if readonly else '' }}></div>
      <div><label>Diğer</label><input name="diger_tl" value="{{ v.get('diger_tl','') }}" {{ 'disabled' if readonly else '' }}></div>
    </div>
  </div>

  {% if metrics %}
  <div class="card">
    <b>Önizleme</b>
    <div class="grid2">
      <div>Toplam Maliyet: <b>{{ "%.2f"|format(metrics["total_cost"]) }}</b> EUR</div>
      <div>Kâr: <b>{{ "%.2f"|format(metrics["profit"]) }}</b> EUR</div>
      <div>Kârlılık: <b>{{ (metrics["margin"]*100)|round(2) if metrics["margin"] is not none else "" }}</b>%</div>
      <div>Hakediş: <b class="{{ 'ok' if metrics['eligible'] else 'bad' }}">{{ "E" if metrics["eligible"] else "H" }}</b></div>
      <div>Çeyrek: <b>{{ metrics["quarter"] or "" }}</b></div>
      <div>Hedef çarpanı: <b>{{ (metrics["target_factor"]*100)|round(0) }}</b>%</div>
      <div>Prim oranı: <b>{{ (metrics["rate"]*100)|round(2) }}</b>%</div>
      <div>Prim (satışçı): <b>{{ "%.2f"|format(metrics["seller_comm"]) }}</b> EUR</div>
    </div>
  </div>
  {% endif %}

  {% if not readonly %}
    <button class="btn" type="submit">Kaydet</button>
  {% endif %}
</form>
"""

SETTINGS_TPL = """
<div class="card">
  <div style="display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;">
    <b>Ayarlar (İlter)</b>
    <a class="btn2" href="{{ url_for('dashboard') }}">Geri</a>
  </div>
  <div class="muted" style="margin-top:8px;">Oranları artık yüzde olarak giriyorsun. Örn: 1 = %1, 5 = %5</div>
</div>

<div class="card">
  <b>Prim Oranları</b>
  <form method="post" action="{{ url_for('save_rates') }}" class="grid">
    <div><label>Standart Prim (%)</label><input name="standard_rate_pct" value="{{ s.get('standard_rate_pct',0) }}"></div>
    <div><label>Proje Prim (%)</label><input name="project_rate_pct" value="{{ s.get('project_rate_pct',0) }}"></div>
    <div><label>Nilüfer Override (%)</label><input name="override_rate_pct" value="{{ s.get('override_rate_pct',0) }}"></div>
    <div style="display:flex;align-items:end;"><button class="btn" type="submit">Kaydet</button></div>
  </form>
</div>

<div class="card">
  <b>Çeyrek Hedefleri</b>
  <form method="post" action="{{ url_for('save_targets') }}" class="grid">
    <div><label>Çeyrek (YYYY-Qn)</label><input name="quarter" value="{{ q }}" placeholder="2026-Q1" required></div>
    <div></div><div></div><div style="display:flex;align-items:end;"><button class="btn" type="submit">Kaydet</button></div>
    {% for u in seller_users %}
      <div><label>{{ u }} hedef (EUR)</label><input name="t_{{ u }}" value="{{ targets.get((u,q), '') }}"></div>
    {% endfor %}
  </form>
</div>

<div class="card">
  <b>Kullanıcılar</b>
  <div class="muted">Yeni kullanıcı ekleme ve şifre değiştirme.</div>
  <form method="post" action="{{ url_for('create_user') }}" class="grid" style="margin-top:10px;">
    <div><label>Yeni Kullanıcı Adı</label><input name="username" required></div>
    <div><label>Ad Soyad</label><input name="full_name" required></div>
    <div><label>Şifre</label><input name="password" type="password" required></div>
    <div style="display:flex;align-items:end;"><button class="btn" type="submit">Ekle</button></div>
  </form>

  <table style="margin-top:10px;">
    <thead><tr><th>Username</th><th>Ad</th><th>Rol</th><th></th></tr></thead>
    <tbody>
      {% for u in users %}
      <tr>
        <td>{{ u['username'] }}</td>
        <td>{{ u['full_name'] }}</td>
        <td>{{ u['role'] }}</td>
        <td><a class="btn2" href="{{ url_for('user_password', username=u['username']) }}">Şifre</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
"""

PASS_TPL = """
<div class="card">
  <div style="display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;">
    <b>Şifre Değiştir: {{ username }}</b>
    <a class="btn2" href="{{ url_for('settings') }}">Geri</a>
  </div>
  <form method="post" style="margin-top:10px; max-width:420px;">
    <label>Yeni Şifre</label>
    <input name="password" type="password" required>
    <button class="btn" type="submit" style="margin-top:10px;">Kaydet</button>
  </form>
</div>
"""

SUMMARY_TPL = """
<div class="card">
  <div style="display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;">
    <b>Özet</b>
    <a class="btn2" href="{{ url_for('dashboard') }}">Geri</a>
  </div>

  <form method="get" style="margin-top:10px; display:flex; gap:10px; flex-wrap:wrap; align-items:end;">
    <div>
      <label>Çeyrek</label>
      <input name="q" value="{{ q }}" placeholder="2026-Q1">
    </div>
    {% if is_admin %}
    <div>
      <label>Satışçı</label>
      <select name="seller">
        <option value="">Hepsi</option>
        {% for s in sellers %}
          <option value="{{ s }}" {% if seller==s %}selected{% endif %}>{{ s }}</option>
        {% endfor %}
      </select>
    </div>
    {% endif %}
    <button class="btn" type="submit">Göster</button>
  </form>
</div>

<div class="card">
  <b>{{ q }}</b> çeyrek ilerleme
  <table style="margin-top:10px;">
    <thead><tr><th>Kişi</th><th>Satış (hakedişli)</th><th>Hedef</th><th>Gerçekleşme</th><th>Hedefe Kalan</th><th>Bar</th></tr></thead>
    <tbody>
    {% for k,v in rows.items() %}
      <tr>
        <td>{{ k }}</td>
        <td>{{ "%.2f"|format(v.sales) }}</td>
        <td>{{ "%.2f"|format(v.target) }}</td>
        <td>{{ (v.ratio*100)|round(1) }}%</td>
        <td>{{ "%.2f"|format(v.left) }}</td>
        <td style="min-width:220px;">
          <div class="barwrap"><div class="bar" style="width: {{ v.bar_pct }}%;"></div></div>
        </td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
  <div class="muted" style="margin-top:8px;">Not: satış toplamı sadece hakediş olmuş satışlardan gelir (tahsilat+teslim tamam).</div>
</div>
"""

QUARTER_TPL = """
<div class="card">
  <div style="display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;">
    <b>Çeyrek Ödeme Listesi (İlter)</b>
    <a class="btn2" href="{{ url_for('dashboard') }}">Geri</a>
  </div>
  <form method="get" style="margin-top:10px; display:flex; gap:10px; align-items:end; flex-wrap:wrap;">
    <div>
      <label>Çeyrek (YYYY-Qn)</label>
      <input name="q" value="{{ q or '' }}" placeholder="2026-Q1">
    </div>
    <button class="btn" type="submit">Göster</button>
  </form>
</div>

{% if q %}
<div class="card">
  <b>{{ q }}</b> — hedef çarpanı uygulanmış primler
  <table>
    <thead>
      <tr><th>Kişi</th><th>Satış</th><th>Hedef</th><th>%</th><th>Çarpan</th><th>Satış Primi</th><th>Nilüfer Ek</th><th>Toplam</th></tr>
    </thead>
    <tbody>
      {% for k,v in summary.items() %}
      <tr>
        <td>{{ k }}</td>
        <td>{{ "%.2f"|format(v["sales"]) }}</td>
        <td>{{ "%.2f"|format(v["target"]) }}</td>
        <td>{{ (v["ratio"]*100)|round(1) }}%</td>
        <td>{{ (v["factor"]*100)|round(0) }}%</td>
        <td>{{ "%.2f"|format(v["seller_comm"]) }}</td>
        <td>{{ "%.2f"|format(v["override"]) }}</td>
        <td><b>{{ "%.2f"|format(v["seller_comm"] + v["override"]) }}</b></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}
"""

# ---------- Routes ----------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username", "").strip().lower()
        p = request.form.get("password", "")
        with db() as conn:
            row = conn.execute("SELECT * FROM users WHERE username=?", (u,)).fetchone()
        if not row or not check_password_hash(row["password_hash"], p):
            flash("Hatalı kullanıcı adı / şifre.")
            return render_template_string(BASE, content=render_template_string(LOGIN_TPL), is_admin=False)
        user = User(id=row["id"], username=row["username"], full_name=row["full_name"], role=row["role"])
        login_user(user)
        return redirect(url_for("dashboard"))
    return render_template_string(BASE, content=render_template_string(LOGIN_TPL), is_admin=False)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

def build_filters() -> Tuple[str, List[Any], Dict[str, str]]:
    where = []
    params: List[Any] = []

    q = (request.args.get("q") or "").strip()
    seller = (request.args.get("seller") or "").strip().lower()
    eligible = (request.args.get("eligible") or "").strip()

    if not is_admin():
        where.append("seller_username=?")
        params.append(current_user.username)

    if seller:
        where.append("seller_username=?")
        params.append(seller)

    if q:
        like = f"%{q}%"
        where.append("""
          (
            COALESCE(customer,'') LIKE ? OR
            COALESCE(invoice_no,'') LIKE ? OR
            COALESCE(notes,'') LIKE ? OR
            COALESCE(job_type,'') LIKE ? OR
            COALESCE(seller_username,'') LIKE ? OR
            COALESCE(sale_id,'') LIKE ?
          )
        """)
        params.extend([like, like, like, like, like, like])

    if eligible == "1":
        where.append("collection_done_date IS NOT NULL AND collection_done_date<>'' AND delivery_done_date IS NOT NULL AND delivery_done_date<>''")
    elif eligible == "0":
        where.append("(collection_done_date IS NULL OR collection_done_date='' OR delivery_done_date IS NULL OR delivery_done_date='')")

    sql_where = ("WHERE " + " AND ".join(where)) if where else ""
    return sql_where, params, {"q": q, "seller": seller, "eligible": eligible}

@app.route("/")
@login_required
def dashboard():
    sql_where, params, flt = build_filters()

    totals = fetch_eligible_sales_totals()
    targets = fetch_targets()

    with db() as conn:
        rows = conn.execute(f"SELECT * FROM sales {sql_where} ORDER BY id DESC LIMIT 300", params).fetchall()
        sellers = [r["username"] for r in conn.execute("SELECT username FROM users WHERE role='seller' ORDER BY username").fetchall()]

    out = []
    for r in rows:
        out.append({**dict(r), "m": compute_sale_metrics(r, totals, targets)})

    totals_html = totals_card(out)

    return render_template_string(
        BASE,
        content=render_template_string(
            DASH_TPL,
            rows=out,
            is_admin=is_admin(),
            q=flt["q"],
            seller=flt["seller"],
            eligible=flt["eligible"],
            sellers=sellers,
            totals_html=totals_html,
        ),
        is_admin=is_admin(),
    )

@app.route("/sale/new", methods=["GET", "POST"])
@login_required
def new_sale():
    if is_admin():
        abort(403)

    if request.method == "POST":
        return save_sale()

    return render_template_string(
        BASE,
        content=render_template_string(SALE_FORM_TPL, title="Yeni Satış", v={}, readonly=False, metrics=None),
        is_admin=is_admin(),
    )

@app.route("/sale/<int:sale_id>/edit", methods=["GET", "POST"])
@login_required
def edit_sale(sale_id: int):
    if is_admin():
        abort(403)

    with db() as conn:
        r = conn.execute("SELECT * FROM sales WHERE id=?", (sale_id,)).fetchone()
    if not r:
        abort(404)
    if r["seller_username"] != current_user.username:
        abort(403)

    totals = fetch_eligible_sales_totals()
    targets = fetch_targets()

    if request.method == "POST":
        return save_sale(sale_id)

    m = compute_sale_metrics(r, totals, targets)

    return render_template_string(
        BASE,
        content=render_template_string(SALE_FORM_TPL, title=f"Satış Düzenle (#{sale_id})", v=dict(r), readonly=False, metrics=m),
        is_admin=is_admin(),
    )

@app.route("/sale/<int:sale_id>/view")
@login_required
def view_sale_admin(sale_id: int):
    if not is_admin():
        abort(403)

    with db() as conn:
        r = conn.execute("SELECT * FROM sales WHERE id=?", (sale_id,)).fetchone()
    if not r:
        abort(404)

    totals = fetch_eligible_sales_totals()
    targets = fetch_targets()
    m = compute_sale_metrics(r, totals, targets)

    return render_template_string(
        BASE,
        content=render_template_string(SALE_FORM_TPL, title=f"Satış Gör (#{sale_id})", v=dict(r), readonly=True, metrics=m),
        is_admin=is_admin(),
    )

def save_sale(sale_id: Optional[int] = None):
    if is_admin():
        abort(403)

    f = request.form
    payload = {
        "sale_id": (f.get("sale_id") or "").strip(),
        "seller_username": current_user.username,
        "customer": (f.get("customer") or "").strip(),
        "job_type": (f.get("job_type") or "").strip(),
        "invoice_no": (f.get("invoice_no") or "").strip(),
        "sale_date": parse_date(f.get("sale_date") or ""),
        "collection_done_date": parse_date(f.get("collection_done_date") or ""),
        "delivery_done_date": parse_date(f.get("delivery_done_date") or ""),
        "sale_eur": safe_float(f.get("sale_eur") or ""),
        "purchase_eur": safe_float(f.get("purchase_eur") or ""),
        "international_shipping_eur": safe_float(f.get("international_shipping_eur") or ""),
        "local_kur": safe_float(f.get("local_kur") or ""),
        "ic_nakliye_tl": safe_float(f.get("ic_nakliye_tl") or ""),
        "gumruk_vergisi_tl": safe_float(f.get("gumruk_vergisi_tl") or ""),
        "ek_gumruk_tl": safe_float(f.get("ek_gumruk_tl") or ""),
        "gumruk_masraf_tl": safe_float(f.get("gumruk_masraf_tl") or ""),
        "komisyon_tl": safe_float(f.get("komisyon_tl") or ""),
        "diger_tl": safe_float(f.get("diger_tl") or ""),
        "notes": (f.get("notes") or "").strip(),
    }

    if payload["sale_eur"] is None or payload["sale_eur"] <= 0:
        flash("Satış (EUR) zorunlu ve 0'dan büyük olmalı.")
        return redirect(request.url)

    now = datetime.utcnow().isoformat(timespec="seconds")

    with db() as conn:
        if sale_id is None:
            conn.execute(
                """
                INSERT INTO sales (
                  sale_id, seller_username, customer, job_type, invoice_no,
                  sale_date, collection_done_date, delivery_done_date,
                  sale_eur, purchase_eur, international_shipping_eur,
                  local_kur,
                  ic_nakliye_tl, gumruk_vergisi_tl, ek_gumruk_tl, gumruk_masraf_tl, komisyon_tl, diger_tl,
                  notes, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    payload["sale_id"], payload["seller_username"], payload["customer"], payload["job_type"], payload["invoice_no"],
                    payload["sale_date"], payload["collection_done_date"], payload["delivery_done_date"],
                    payload["sale_eur"], payload["purchase_eur"], payload["international_shipping_eur"],
                    payload["local_kur"],
                    payload["ic_nakliye_tl"], payload["gumruk_vergisi_tl"], payload["ek_gumruk_tl"], payload["gumruk_masraf_tl"], payload["komisyon_tl"], payload["diger_tl"],
                    payload["notes"], now
                )
            )
        else:
            row = conn.execute("SELECT seller_username FROM sales WHERE id=?", (sale_id,)).fetchone()
            if not row:
                abort(404)
            if row["seller_username"] != current_user.username:
                abort(403)
            conn.execute(
                """
                UPDATE sales SET
                  sale_id=?, customer=?, job_type=?, invoice_no=?,
                  sale_date=?, collection_done_date=?, delivery_done_date=?,
                  sale_eur=?, purchase_eur=?, international_shipping_eur=?,
                  local_kur=?,
                  ic_nakliye_tl=?, gumruk_vergisi_tl=?, ek_gumruk_tl=?, gumruk_masraf_tl=?, komisyon_tl=?, diger_tl=?,
                  notes=?
                WHERE id=?
                """,
                (
                    payload["sale_id"], payload["customer"], payload["job_type"], payload["invoice_no"],
                    payload["sale_date"], payload["collection_done_date"], payload["delivery_done_date"],
                    payload["sale_eur"], payload["purchase_eur"], payload["international_shipping_eur"],
                    payload["local_kur"],
                    payload["ic_nakliye_tl"], payload["gumruk_vergisi_tl"], payload["ek_gumruk_tl"], payload["gumruk_masraf_tl"], payload["komisyon_tl"], payload["diger_tl"],
                    payload["notes"], sale_id
                )
            )
        conn.commit()

    flash("Kaydedildi.")
    return redirect(url_for("dashboard"))

# --- Settings (admin) ---
@app.route("/settings")
@login_required
def settings():
    if not is_admin():
        abort(403)
    s = get_settings()
    q = (request.args.get("q") or current_quarter()).strip()
    targets = fetch_targets()
    with db() as conn:
        users = conn.execute("SELECT username, full_name, role FROM users ORDER BY username").fetchall()
        seller_users = [r["username"] for r in conn.execute("SELECT username FROM users WHERE role='seller' ORDER BY username").fetchall()]
    return render_template_string(
        BASE,
        content=render_template_string(
            SETTINGS_TPL,
            s=s,
            q=q,
            targets=targets,
            seller_users=seller_users,
            users=users,
        ),
        is_admin=is_admin(),
    )

@app.route("/settings/save_rates", methods=["POST"])
@login_required
def save_rates():
    if not is_admin():
        abort(403)
    f = request.form
    keys = ["standard_rate_pct", "project_rate_pct", "override_rate_pct"]
    with db() as conn:
        for k in keys:
            v = safe_float(f.get(k, ""))
            if v is None or v < 0:
                flash("Oranlar sayı olmalı ve negatif olamaz.")
                return redirect(url_for("settings"))
            conn.execute(
                "INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (k, str(v)),
            )
        conn.commit()
    flash("Oranlar kaydedildi.")
    return redirect(url_for("settings"))

@app.route("/settings/save_targets", methods=["POST"])
@login_required
def save_targets():
    if not is_admin():
        abort(403)
    quarter = (request.form.get("quarter") or "").strip()
    if not quarter or "-Q" not in quarter:
        flash("Çeyrek formatı: 2026-Q1 gibi olmalı.")
        return redirect(url_for("settings"))

    with db() as conn:
        seller_users = [r["username"] for r in conn.execute("SELECT username FROM users WHERE role='seller' ORDER BY username").fetchall()]
        for u in seller_users:
            val = safe_float(request.form.get(f"t_{u}", ""))
            if val is None:
                continue
            if val < 0:
                flash("Hedef negatif olamaz.")
                return redirect(url_for("settings", q=quarter))
            conn.execute(
                """
                INSERT INTO targets (quarter, seller_username, target_eur)
                VALUES (?,?,?)
                ON CONFLICT(quarter, seller_username) DO UPDATE SET target_eur=excluded.target_eur
                """,
                (quarter, u, val),
            )
        conn.commit()

    flash("Hedefler kaydedildi.")
    return redirect(url_for("settings", q=quarter))

@app.route("/settings/create_user", methods=["POST"])
@login_required
def create_user():
    if not is_admin():
        abort(403)
    username = (request.form.get("username") or "").strip().lower()
    full_name = (request.form.get("full_name") or "").strip()
    password = request.form.get("password") or ""

    if not username or not full_name or len(password) < 6:
        flash("Kullanıcı adı/ad/şifre zorunlu. Şifre en az 6 karakter olmalı.")
        return redirect(url_for("settings"))

    if username == ADMIN_USERNAME:
        flash("Bu kullanıcı adı admin için ayrılmış.")
        return redirect(url_for("settings"))

    with db() as conn:
        try:
            conn.execute(
                "INSERT INTO users (username, full_name, password_hash, role) VALUES (?,?,?,?)",
                (username, full_name, generate_password_hash(password), "seller"),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            flash("Bu kullanıcı adı zaten var.")
            return redirect(url_for("settings"))

    flash("Kullanıcı eklendi.")
    return redirect(url_for("settings"))

@app.route("/users/<username>/password", methods=["GET", "POST"])
@login_required
def user_password(username: str):
    if not is_admin():
        abort(403)
    username = username.strip().lower()

    with db() as conn:
        u = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not u:
        abort(404)

    if request.method == "POST":
        pw = request.form.get("password", "")
        if len(pw) < 6:
            flash("Şifre en az 6 karakter olmalı.")
            return redirect(request.url)
        with db() as conn:
            conn.execute("UPDATE users SET password_hash=? WHERE username=?", (generate_password_hash(pw), username))
            conn.commit()
        flash("Şifre güncellendi.")
        return redirect(url_for("settings"))

    return render_template_string(BASE, content=render_template_string(PASS_TPL, username=username), is_admin=is_admin())

# --- Summary (1 + 6) ---
@app.route("/summary")
@login_required
def summary():
    q = (request.args.get("q") or current_quarter()).strip()
    seller = (request.args.get("seller") or "").strip().lower()

    totals = fetch_eligible_sales_totals()
    targets = fetch_targets()

    with db() as conn:
        sellers = [r["username"] for r in conn.execute("SELECT username FROM users WHERE role='seller' ORDER BY username").fetchall()]

    # admin seçmediyse hepsi; satışçı ise sadece kendisi
    visible = sellers
    if not is_admin():
        visible = [current_user.username]
    elif seller:
        visible = [seller] if seller in sellers else sellers

    class R: pass
    rows: Dict[str, Any] = {}
    for u in visible:
        sales = float(totals.get((u, q), 0.0))
        tgt = float(targets.get((u, q), 0.0))
        ratio = (sales / tgt) if tgt > 0 else 0.0
        left = max(tgt - sales, 0.0) if tgt > 0 else 0.0
        bar_pct = max(0.0, min(ratio * 100.0, 100.0)) if tgt > 0 else 0.0
        rr = R()
        rr.sales = sales
        rr.target = tgt
        rr.ratio = ratio
        rr.left = left
        rr.bar_pct = bar_pct
        rows[u] = rr

    return render_template_string(
        BASE,
        content=render_template_string(
            SUMMARY_TPL,
            q=q,
            seller=seller,
            sellers=sellers,
            rows=rows,
            is_admin=is_admin(),
        ),
        is_admin=is_admin(),
    )

# --- Quarterly (admin) ---
@app.route("/quarterly")
@login_required
def quarterly():
    if not is_admin():
        abort(403)

    q = (request.args.get("q") or "").strip()
    summary: Dict[str, Dict[str, float]] = {}

    totals = fetch_eligible_sales_totals()
    targets = fetch_targets()

    if q:
        with db() as conn:
            sellers = [r["username"] for r in conn.execute("SELECT username FROM users WHERE role='seller' ORDER BY username").fetchall()]
            rows = conn.execute(
                "SELECT * FROM sales WHERE collection_done_date IS NOT NULL AND collection_done_date<>'' AND delivery_done_date IS NOT NULL AND delivery_done_date<>''"
            ).fetchall()

        base = {"sales": 0.0, "target": 0.0, "ratio": 0.0, "factor": 0.0, "seller_comm": 0.0, "override": 0.0}
        for s in sellers:
            tgt = targets.get((s, q), 0.0)
            tot = totals.get((s, q), 0.0)
            ratio = (tot / tgt) if tgt and tgt > 0 else 0.0
            factor = seller_target_factor(s, q, totals, targets)
            summary[s] = {**base, "sales": tot, "target": tgt, "ratio": ratio, "factor": factor}

        for r in rows:
            m = compute_sale_metrics(r, totals, targets)
            if m["quarter"] != q:
                continue
            s = r["seller_username"]
            if s in summary:
                summary[s]["seller_comm"] += m["seller_comm"]
            if r["seller_username"] in ("pinar", "burcu"):
                if "nilufer" not in summary:
                    summary["nilufer"] = base.copy()
                summary["nilufer"]["override"] += m["override"]

    return render_template_string(BASE, content=render_template_string(QUARTER_TPL, q=q, summary=summary), is_admin=is_admin())

# --- CSV Export (3) ---
@app.route("/export.csv")
@login_required
def export_csv():
    if not is_admin():
        abort(403)

    totals = fetch_eligible_sales_totals()
    targets = fetch_targets()

    with db() as conn:
        rows = conn.execute("SELECT * FROM sales ORDER BY id DESC").fetchall()

    output = io.StringIO()
    w = csv.writer(output)

    w.writerow([
        "id","seller","customer","job_type","invoice_no",
        "sale_date","collection_done_date","delivery_done_date",
        "sale_eur","total_cost_eur","profit_eur","margin_pct",
        "eligible","quarter","target_factor_pct","commission_rate_pct",
        "seller_commission_eur","nilufer_override_eur",
        "notes"
    ])

    for r in rows:
        m = compute_sale_metrics(r, totals, targets)
        margin_pct = (m["margin"] * 100.0) if m["margin"] is not None else ""
        w.writerow([
            r["id"], r["seller_username"], r["customer"] or "", r["job_type"] or "", r["invoice_no"] or "",
            r["sale_date"] or "", r["collection_done_date"] or "", r["delivery_done_date"] or "",
            f"{m['sale_eur']:.2f}", f"{m['total_cost']:.2f}", f"{m['profit']:.2f}", (f"{margin_pct:.2f}" if margin_pct != "" else ""),
            "1" if m["eligible"] else "0",
            m["quarter"] or "",
            f"{m['target_factor']*100:.0f}",
            f"{m['rate']*100:.2f}",
            f"{m['seller_comm']:.2f}",
            f"{m['override']:.2f}",
            r["notes"] or ""
        ])

    resp = Response(output.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = "attachment; filename=primapp_export.csv"
    return resp

# ---------- Start ----------
if __name__ == "__main__":
    init_db()
    ensure_defaults()
    app.run(host="0.0.0.0", port=5001, debug=True)
