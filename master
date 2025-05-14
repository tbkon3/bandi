"""bandi_scraper_dashboard.py
-------------------------------------------------------------------
Dashboard interattiva (Streamlit) + scraper bandi Trentino‑Alto Adige
-------------------------------------------------------------------

• Avvia con:
    streamlit run bandi_scraper_dashboard.py

• Dipendenze:
    pip install streamlit pandas requests python-dateutil beautifulsoup4 lxml

• Fonti integrate (maggio 2025):
    1. API Open Data – Comune di Trento (classi=bando)
    2. CKAN dati.trentino.it – dataset "bandi-di-gara" (param resource_id)
    3. Portale bandi Alto Adige (scraping HTML, paginazione)
    4. Portale Amministrazione Trasparente PAT (scraping RSS/HTML)

Puoi aggiungere/estendere i parser nelle rispettive funzioni.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

###############################################################################
# CONFIG                                                                    #
###############################################################################

ST_TAGS = {
    "eventi": ["evento", "manifestazione", "festival", "fiera"],
    "turismo": ["turismo", "turistica"],
    "tech": ["tech", "digital", "ict", "hackathon"],
    "marketing": ["marketing", "promozione", "comunicazione"],
    "cultura": ["cultur", "museo", "spettacolo"],
}

# intervallo: mostra solo bandi con scadenza entro 30 giorni o ancora aperti
SCADENZA_GIORNI = 30

###############################################################################
# HELPERS                                                                    #
###############################################################################

def _clean_html(txt: str | None) -> str:
    if not txt:
        return ""
    return re.sub(r"<[^>]+>", " ", txt).strip()


def _guess_tags(text: str) -> List[str]:
    text_l = text.lower()
    tags = []
    for tag, keywords in ST_TAGS.items():
        if any(k in text_l for k in keywords):
            tags.append(tag)
    return tags or ["varie"]


def _within_next_days(date_str: str | None, days: int = 30) -> bool:
    if not date_str:
        return True  # nessuna data: consideriamo aperto
    try:
        dt = dtparser.parse(date_str, dayfirst=True, fuzzy=True)
        return dt <= (datetime.today() + timedelta(days=days))
    except Exception:
        return True

###############################################################################
# PARSER – COMUNE DI TRENTO                                                  #
###############################################################################

def fetch_trento(max_items: int = 200) -> pd.DataFrame:
    """API Comune di Trento OpenData: /content/search?classes=bando"""
    url = (
        "https://www.comune.trento.it/api/opendata/v2/content/search?classes=bando&page_size="
        + str(max_items)
    )
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json().get("items", [])
    items = []
    for itm in data:
        prop = itm.get("properties", {})
        title = _clean_html(itm.get("title"))
        deadline = prop.get("dataScadenza", "")
        link = itm.get("url", itm.get("id", ""))
        entity = "Comune di Trento"
        amount = prop.get("importoBase", "") or "-"
        if _within_next_days(deadline, SCADENZA_GIORNI):
            items.append(
                {
                    "id": itm.get("uid", str(uuid.uuid4())),
                    "title": title,
                    "entity": entity,
                    "deadline": deadline or "Aperto",
                    "amount": amount,
                    "tags": _guess_tags(title),
                    "link": link,
                }
            )
    return pd.DataFrame(items)

###############################################################################
# PARSER – CKAN DATI.TRENTINO.IT                                             #
###############################################################################

CKAN_ENDPOINT = "https://dati.trentino.it/api/3/action/datastore_search"
CKAN_RESOURCE_ID = "e989dd00-c4ce-48a2-88f7-16a0518c026a"  # bandi di gara


def fetch_ckan(rows: int = 300) -> pd.DataFrame:
    params = {"resource_id": CKAN_RESOURCE_ID, "limit": rows}
    resp = requests.get(CKAN_ENDPOINT, params=params, timeout=20)
    resp.raise_for_status()
    records = resp.json()["result"]["records"]
    items: List[Dict] = []
    for rec in records:
        title = rec.get("oggetto", "")
        deadline = rec.get("scadenza", "")
        if not _within_next_days(deadline, SCADENZA_GIORNI):
            continue
        items.append(
            {
                "id": rec.get("idGara", str(uuid.uuid4())),
                "title": title,
                "entity": rec.get("stazioneAppaltante", "PAT"),
                "deadline": deadline or "Aperto",
                "amount": rec.get("importoBaseAsta", "-"),
                "tags": _guess_tags(title),
                "link": rec.get("urlBando", rec.get("urlGara", "")),
            }
        )
    return pd.DataFrame(items)

###############################################################################
# PARSER – PORTALE BANDI ALTO ADIGE                                          #
###############################################################################

def fetch_altoadige(pages: int = 1) -> pd.DataFrame:
    """Scraping semplificato del portale bandi Alto Adige."""
    items = []
    base = "https://www.bandi-altoadige.it/"
    for p in range(1, pages + 1):
        url = f"{base}?page={p}&search=eventi"  # query generica
        try:
            html = requests.get(url, timeout=20).text
        except Exception:
            continue
        soup = BeautifulSoup(html, "lxml")
        for card in soup.select("div.bando-card"):
            title_el = card.select_one("h2")
            title = title_el.get_text(strip=True) if title_el else "(senza titolo)"
            link = base + title_el.find("a")["href"] if title_el and title_el.find("a") else url
            entity = "Provincia Autonoma di Bolzano / Altri"
            deadline_el = card.select_one("span[data-field='scadenza']")
            deadline = deadline_el.get_text(strip=True) if deadline_el else "Aperto"
            if not _within_next_days(deadline, SCADENZA_GIORNI):
                continue
            items.append(
                {
                    "id": str(uuid.uuid4()),
                    "title": title,
                    "entity": entity,
                    "deadline": deadline,
                    "amount": "-",
                    "tags": _guess_tags(title),
                    "link": link,
                }
            )
    return pd.DataFrame(items)

###############################################################################
# PARSER – AMMINISTRAZIONE TRASPARENTE PAT (RSS)                             #
###############################################################################

def fetch_pat(limit: int = 50) -> pd.DataFrame:
    """RSS PAT – solo titoli e link (semplificato)."""
    rss_url = "https://provinciaditrento.portaleamministrazionetrasparente.it/feeds/bandigara"
    try:
        xml = requests.get(rss_url, timeout=20).text
    except Exception:
        return pd.DataFrame()
    soup = BeautifulSoup(xml, "xml")
    items = []
    for item in soup.find_all("item")[:limit]:
        title = _clean_html(item.title.text)
        link = item.link.text
        deadline = re.search(r"scadenza:?\s*(\d{2}/\d{2}/\d{4})", title, re.I)
        deadline_str = deadline.group(1) if deadline else "Aperto"
        if not _within_next_days(deadline_str, SCADENZA_GIORNI):
            continue
        items.append(
            {
                "id": link,
                "title": title,
                "entity": "Provincia Autonoma di Trento (PAT)",
                "deadline": deadline_str,
                "amount": "-",
                "tags": _guess_tags(title),
                "link": link,
            }
        )
    return pd.DataFrame(items)

###############################################################################
# AGGREGATORE                                                                #
###############################################################################

def load_bandi() -> pd.DataFrame:
    """Scarica e unisce tutte le fonti."""
    dfs = []
    for fetch_fn in (fetch_trento, fetch_ckan, fetch_altoadige, fetch_pat):
        try:
            dfs.append(fetch_fn())
        except Exception as exc:
            st.warning(f"Errore in {fetch_fn.__name__}: {exc}")
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    # dedup per id o titolo+ente
    df.drop_duplicates(subset=["id"], inplace=True)
    # ordina per scadenza (Aperto in fondo)
    def _key(row):
        if row["deadline"].lower().startswith("aperto"):
            return datetime.max
        try:
            return dtparser.parse(row["deadline"], dayfirst=True)
        except Exception:
            return datetime.max
    df["_sort"] = df.apply(_key, axis=1)
    df.sort_values("_sort", inplace=True)
    return df.drop(columns="_sort")

###############################################################################
# STREAMLIT UI                                                               #
###############################################################################

def main():
    st.set_page_config(
        page_title="Dashboard Bandi – Trentino-Alto Adige",
        layout="wide",
    )

    st.title("📑 Dashboard Bandi – Trentino‑Alto Adige")
    st.caption("Bandi aperti o in scadenza entro 30 giorni – settori eventi, fiere, marketing, cultura, turismo")

    if st.button("Aggiorna ↻", use_container_width=True):
        st.session_state["_reload"] = True

    if st.session_state.get("_reload") or "df" not in st.session_state:
        with st.spinner("Caricamento bandi…"):
            st.session_state["df"] = load_bandi()
        st.session_state.pop("_reload", None)

    df: pd.DataFrame = st.session_state.get("df", pd.DataFrame())

    # filtri
    tag_opzioni = sorted({t for tags in df.tags.dropna() for t in tags})
    selected_tags = st.multiselect("Filtra per tag", options=tag_opzioni, default=[])
    query = st.text_input("Cerca titolo…", "")

    filtered = df.copy()
    if selected_tags:
        filtered = filtered[filtered.tags.apply(lambda ts: any(t in ts for t in selected_tags))]
    if query:
        ql = query.lower()
        filtered = filtered[filtered.title.str.lower().str.contains(ql)]

    st.metric("Bandi trovati", len(filtered))

    st.dataframe(
        filtered.drop(columns="tags").assign(tags=filtered.tags.apply(lambda lst: ", ".join(lst))),
        use_container_width=True,
    )

    st.markdown("---")
    st.markdown("*Dati ottenuti da: Comune di Trento, dati.trentino.it, Portale bandi Alto Adige, Trasparenza PAT.*")


if __name__ == "__main__":
    main()
