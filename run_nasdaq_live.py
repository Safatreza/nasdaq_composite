"""
RSL Live Run — Nasdaq-100: fetches real-time data and produces rankings.
Same Relative Strength Levy strategy as the S&P 500 version.
"""

import pandas as pd
import numpy as np
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import warnings
import os
import io

warnings.filterwarnings('ignore')

# ── Config ───────────────────────────────────────────────────────────────────
RSL_PERIODE      = 26
MA_50            = 50
MA_200           = 200
RUECKBLICK_TAGE  = 400
API_VERZOEGERUNG = 0.25
TOP_PROZENT      = 0.25
ZEITSTEMPEL      = datetime.now().strftime('%Y%m%d_%H%M')
AUSGABE_DATEI    = f"RSL_Nasdaq100_Rangliste_{ZEITSTEMPEL}.xlsx"

print("=" * 70)
print("RSL NASDAQ-100 SCREENING — LIVE RUN")
print(f"Datum: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
print("=" * 70)

# ── Helpers ──────────────────────────────────────────────────────────────────
def berechne_rsl(kurse, periode):
    if kurse is None or len(kurse) < periode:
        return None
    try:
        sma = kurse.iloc[-periode:].mean()
        if sma == 0 or pd.isna(sma):
            return None
        return round(kurse.iloc[-1] / sma, 4)
    except Exception:
        return None

def berechne_aenderung(kurse, tage):
    if kurse is None or len(kurse) < tage:
        return None
    try:
        prev = kurse.iloc[-tage]
        if prev == 0:
            return None
        return round(((kurse.iloc[-1] - prev) / prev) * 100, 2)
    except Exception:
        return None

def berechne_ma(kurse, periode):
    if kurse is None or len(kurse) < periode:
        return None
    try:
        return round(kurse.iloc[-periode:].mean(), 2)
    except Exception:
        return None

# ── Ticker fetch ─────────────────────────────────────────────────────────────
def hole_nasdaq100_ticker():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    print("\n[1/4] Lade Nasdaq-100 Ticker von Wikipedia...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        soup  = BeautifulSoup(r.text, 'lxml')
        table = soup.find('table', {'id': 'constituents'})
        if table is None:
            for t in soup.find_all('table', {'class': 'wikitable'}):
                if t.find('th', string=lambda x: x and 'Ticker' in x):
                    table = t
                    break
        df = pd.read_html(io.StringIO(str(table)))[0]
        df.columns = df.columns.str.strip()

        # Normalise column names — Wikipedia uses different names over time
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if 'ticker' in cl or 'symbol' in cl:
                col_map[c] = 'Symbol'
            elif 'company' in cl or 'security' in cl or 'name' in cl:
                col_map[c] = 'Unternehmen'
            elif 'sector' in cl or 'gics sector' in cl:
                col_map[c] = 'Sektor'
            elif 'sub' in cl or 'industry' in cl:
                col_map[c] = 'Branche'
        df = df.rename(columns=col_map)

        result = pd.DataFrame({
            'Symbol':      df['Symbol'].str.strip().str.replace('.', '-', regex=False),
            'Unternehmen': df['Unternehmen'].str.strip() if 'Unternehmen' in df.columns else df['Symbol'],
            'Sektor':      df['Sektor'].str.strip()      if 'Sektor'      in df.columns else 'K.A.',
            'Branche':     df['Branche'].str.strip()     if 'Branche'     in df.columns else 'K.A.',
        })
        print(f"  {len(result)} Ticker geladen.")
        return result
    except Exception as e:
        print(f"  Fehler: {e} — Verwende Fallback.")
        fallback = ['AAPL','MSFT','NVDA','AMZN','META','TSLA','GOOGL','GOOG',
                    'AVGO','COST','NFLX','ASML','TMUS','AMD','PEP']
        return pd.DataFrame({'Symbol': fallback, 'Unternehmen': fallback,
                             'Sektor': ['Diverse']*len(fallback),
                             'Branche': ['Diverse']*len(fallback)})

def hole_ndx_kurse(start, end):
    try:
        h = yf.Ticker('^NDX').history(start=start, end=end, auto_adjust=True)
        return h['Close'] if not h.empty else None
    except Exception:
        return None

# ── Per-stock data ────────────────────────────────────────────────────────────
def hole_aktien_daten(ticker, start, end, ndx_kurse=None, max_versuche=3):
    hist = None
    for versuch in range(max_versuche):
        try:
            hist = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
            break
        except Exception:
            if versuch < max_versuche - 1:
                time.sleep(2 ** versuch)
            else:
                return None

    if hist is None or hist.empty or len(hist) < RSL_PERIODE:
        return None

    try:
        closes = hist['Close']
        volume = hist['Volume']

        try:
            info = yf.Ticker(ticker).info
        except Exception:
            info = {}

        kurs   = closes.iloc[-1]
        hoch52 = closes.max()
        tief52 = closes.min()

        rsl      = berechne_rsl(closes, RSL_PERIODE)
        aend_26t = berechne_aenderung(closes, RSL_PERIODE)
        aend_1m  = berechne_aenderung(closes, 20)
        aend_3m  = berechne_aenderung(closes, 60)
        aend_6m  = berechne_aenderung(closes, 130)

        ma50     = berechne_ma(closes, MA_50)
        ma200    = berechne_ma(closes, MA_200)
        pct_ma50  = round(((kurs - ma50)  / ma50)  * 100, 2) if ma50  else None
        pct_ma200 = round(((kurs - ma200) / ma200) * 100, 2) if ma200 else None

        rel_ndx = None
        if ndx_kurse is not None and len(ndx_kurse) >= RSL_PERIODE:
            ap = berechne_aenderung(closes, RSL_PERIODE)
            np_ = berechne_aenderung(ndx_kurse, RSL_PERIODE)
            if ap is not None and np_ is not None:
                rel_ndx = round(ap - np_, 2)

        try:
            hoch_idx       = closes.idxmax()
            tage_seit_hoch = (closes.index[-1] - hoch_idx).days
        except Exception:
            tage_seit_hoch = None

        vol_ratio = None
        if len(volume) >= 50:
            avg50 = volume.iloc[-50:].mean()
            avg5  = volume.iloc[-5:].mean()
            if avg50 > 0:
                vol_ratio = round(avg5 / avg50, 2)

        avg_vol = volume.iloc[-20:].mean() if len(volume) >= 20 else volume.mean()
        div_raw = info.get('dividendYield', None)

        return {
            'Aktueller_Kurs':       round(kurs, 2),
            'Marktkapitalisierung': info.get('marketCap', None),
            'RSL':                  rsl,
            'Aenderung_26T':        aend_26t,
            'Aenderung_1M':         aend_1m,
            'Aenderung_3M':         aend_3m,
            'Aenderung_6M':         aend_6m,
            'MA_50':                ma50,
            'MA_200':               ma200,
            'Proz_ueber_MA50':      pct_ma50,
            'Proz_ueber_MA200':     pct_ma200,
            '52W_Hoch':             round(hoch52, 2),
            '52W_Tief':             round(tief52, 2),
            'Proz_vom_Hoch':        round(((kurs - hoch52) / hoch52) * 100, 2),
            'Proz_vom_Tief':        round(((kurs - tief52) / tief52) * 100, 2),
            'Tage_seit_Hoch':       tage_seit_hoch,
            'Volumen_Ratio':        vol_ratio,
            'Durchschn_Volumen':    int(avg_vol) if avg_vol else None,
            'Beta':                 info.get('beta', None),
            'KGV':                  info.get('trailingPE', None),
            'Dividendenrendite':    round(div_raw * 100, 2) if div_raw else 0.0,
            'Rel_Staerke_NDX':      rel_ndx,
            'Datenpunkte':          len(closes),
        }
    except Exception:
        return None

# ── Batch processing ──────────────────────────────────────────────────────────
def verarbeite_alle(ticker_df, start, end, ndx_kurse):
    ergebnisse     = []
    fehlgeschlagen = []
    print(f"\n[3/4] Verarbeite {len(ticker_df)} Aktien "
          f"(Zeitraum: {start.strftime('%d.%m.%Y')} – {end.strftime('%d.%m.%Y')})...")

    for _, zeile in tqdm(ticker_df.iterrows(), total=len(ticker_df), desc="Lade Daten"):
        daten = hole_aktien_daten(zeile['Symbol'], start, end, ndx_kurse)
        if daten and daten.get('RSL') is not None:
            ergebnisse.append({'Ticker': zeile['Symbol'], 'Unternehmen': zeile['Unternehmen'],
                               'Sektor': zeile['Sektor'], 'Branche': zeile['Branche'], **daten})
        else:
            fehlgeschlagen.append(zeile['Symbol'])
        time.sleep(API_VERZOEGERUNG)

    df = pd.DataFrame(ergebnisse).sort_values('RSL', ascending=False).reset_index(drop=True)
    df.insert(0, 'Rang', range(1, len(df) + 1))
    df['Perzentil'] = df['RSL'].rank(pct=True).apply(lambda x: round(x * 100, 1))
    print(f"\n  Erfolgreich: {len(df)}  |  Fehlgeschlagen: {len(fehlgeschlagen)}")
    return df, fehlgeschlagen

# ── Excel report ──────────────────────────────────────────────────────────────
def formatiere_mktcap(v):
    if v is None or pd.isna(v): return 'K.A.'
    if v >= 1e12: return f"{v/1e12:.2f} Bio."
    if v >= 1e9:  return f"{v/1e9:.2f} Mrd."
    if v >= 1e6:  return f"{v/1e6:.2f} Mio."
    return f"{v:,.0f}"

def erstelle_excel(df, top, sektor_stats, fehler, datei):
    print(f"\n[4/4] Erstelle Excel: {datei}")
    excel_df = df.copy()
    excel_df['MktCap_Text'] = excel_df['Marktkapitalisierung'].apply(formatiere_mktcap)
    top_ex   = top.copy()
    top_ex['MktCap_Text'] = top_ex['Marktkapitalisierung'].apply(formatiere_mktcap)

    with pd.ExcelWriter(datei, engine='xlsxwriter') as writer:
        wb  = writer.book
        hdr = wb.add_format({'bold': True, 'bg_color': '#1F4E79', 'font_color': 'white',
                             'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
        ttl = wb.add_format({'bold': True, 'font_size': 14, 'font_color': '#1F4E79'})
        grn = wb.add_format({'bg_color': '#C6EFCE', 'border': 1})

        cols1 = ['Rang','Ticker','Unternehmen','Sektor','Branche','Aktueller_Kurs','MktCap_Text',
                 'RSL','Perzentil','Aenderung_26T','Aenderung_1M','Aenderung_3M','Aenderung_6M',
                 'MA_50','MA_200','Proz_ueber_MA50','Proz_ueber_MA200',
                 'Proz_vom_Hoch','Proz_vom_Tief','Volumen_Ratio','Durchschn_Volumen',
                 'Beta','KGV','Dividendenrendite']
        hdrs1 = ['Rang','Ticker','Unternehmen','Sektor','Branche','Kurs ($)','Marktkapitalisierung',
                 'RSL 26T','Perzentil (%)','Änd. 26T (%)','Änd. 1M (%)','Änd. 3M (%)','Änd. 6M (%)',
                 'MA 50','MA 200','% über MA50','% über MA200',
                 '% vom Hoch','% vom Tief','Vol. Ratio','Ø Volumen',
                 'Beta','KGV','Div. Rendite (%)']
        b1 = excel_df[cols1].copy(); b1.columns = hdrs1
        b1.to_excel(writer, sheet_name='Vollstaendige_Rangliste', index=False)
        ws1 = writer.sheets['Vollstaendige_Rangliste']
        for i, h in enumerate(hdrs1): ws1.write(0, i, h, hdr)
        ws1.set_column('A:A', 6); ws1.set_column('B:B', 8); ws1.set_column('C:C', 26)
        ws1.set_column('D:E', 20); ws1.set_column('F:X', 12)
        top25 = int(len(df) * 0.25)
        ws1.conditional_format(1, 0, top25, len(hdrs1)-1,
                               {'type': 'formula', 'criteria': f'=$A2<={top25}', 'format': grn})
        ws1.freeze_panes(1, 0); ws1.autofilter(0, 0, len(b1), len(hdrs1)-1)

        cols2 = ['Rang','Ticker','Unternehmen','Sektor','Aktueller_Kurs','MktCap_Text',
                 'RSL','Perzentil','Rel_Staerke_NDX',
                 'Aenderung_26T','Aenderung_1M','Aenderung_3M','Aenderung_6M',
                 'Proz_ueber_MA50','Proz_ueber_MA200','Tage_seit_Hoch','Proz_vom_Hoch',
                 'Volumen_Ratio','KGV','Dividendenrendite']
        hdrs2 = ['Rang','Ticker','Unternehmen','Sektor','Kurs ($)','Marktkapitalisierung',
                 'RSL 26T','Perzentil (%)','Rel. Stärke vs NDX',
                 'Änd. 26T (%)','Änd. 1M (%)','Änd. 3M (%)','Änd. 6M (%)',
                 '% über MA50','% über MA200','Tage seit Hoch','% vom Hoch',
                 'Vol. Ratio','KGV','Div. Rendite (%)']
        b2 = top_ex[cols2].copy(); b2.columns = hdrs2
        b2.to_excel(writer, sheet_name='Top_25%_Stars', index=False)
        ws2 = writer.sheets['Top_25%_Stars']
        for i, h in enumerate(hdrs2): ws2.write(0, i, h, hdr)
        ws2.set_column('A:A', 6); ws2.set_column('B:B', 8); ws2.set_column('C:C', 26)
        ws2.set_column('D:T', 14); ws2.freeze_panes(1, 0)
        ws2.autofilter(0, 0, len(b2), len(hdrs2)-1)

        beste = df.loc[df.groupby('Sektor')['RSL'].idxmax()].set_index('Sektor')['Ticker'].to_dict()
        sdf   = sektor_stats.reset_index()
        sdf['Beste_Aktie'] = sdf['Sektor'].map(beste)
        sdf.columns = ['Sektor','Ø RSL','Median RSL','Anzahl','Ø 26T Änd. (%)','In Top 25%','Anteil Top 25% (%)','Beste Aktie']
        sdf.to_excel(writer, sheet_name='Sektoranalyse', index=False)
        ws3 = writer.sheets['Sektoranalyse']
        for i, h in enumerate(sdf.columns): ws3.write(0, i, h, hdr)
        ws3.set_column('A:A', 26); ws3.set_column('B:H', 16); ws3.freeze_panes(1, 0)

        ws4 = wb.add_worksheet('Zusammenfassung')
        ws4.write(0, 0, 'RSL NASDAQ-100 SCREENING — ZUSAMMENFASSUNG', ttl)
        meta = [('Analysedatum', datetime.now().strftime('%d.%m.%Y %H:%M')),
                ('Analysierte Aktien', len(df)), ('Fehlgeschlagen', len(fehler))]
        for i, (k, v) in enumerate(meta):
            ws4.write(2+i, 0, k, hdr); ws4.write(2+i, 1, v)
        ws4.write(7, 0, 'RSL-STATISTIKEN', ttl)
        rsl_stats = [('Durchschnitt', f"{df['RSL'].mean():.4f}"),
                     ('Median',       f"{df['RSL'].median():.4f}"),
                     ('Maximum',      f"{df['RSL'].max():.4f}  ({df.iloc[0]['Ticker']})"),
                     ('Minimum',      f"{df['RSL'].min():.4f}  ({df.iloc[-1]['Ticker']})"),
                     ('Bullisch (>1)',f"{(df['RSL']>1).sum()} Aktien"),
                     ('Bärisch (≤1)', f"{(df['RSL']<=1).sum()} Aktien")]
        for i, (k, v) in enumerate(rsl_stats):
            ws4.write(9+i, 0, k, hdr); ws4.write(9+i, 1, v)
        ws4.set_column('A:A', 22); ws4.set_column('B:M', 14)

        ws5 = wb.add_worksheet('Methodik')
        lines = [
            ('RSL NASDAQ-100 SCREENING — METHODIK', True),
            ('', False),
            ('Formel:  RSL = Aktueller Kurs / SMA(Kurs, 26 Handelstage)', False),
            ('Quelle:  Robert Levy, 1967', False),
            ('', False),
            ('INDEX', True),
            ('Nasdaq-100 (NDX) — die 100 größten nicht-finanziellen Nasdaq-Unternehmen', False),
            ('', False),
            ('DATEN', True),
            ('Ticker:  Wikipedia — Nasdaq-100', False),
            ('Kurse:   Yahoo Finance (yfinance)', False),
            ('Index:   Nasdaq-100 (^NDX)', False),
            ('', False),
            ('HAFTUNGSAUSSCHLUSS', True),
            ('Nur für Bildungs- und Forschungszwecke. Keine Anlageberatung.', False),
            (f'Erstellt: {datetime.now().strftime("%d.%m.%Y %H:%M:%S")}', False),
        ]
        for i, (txt, bold) in enumerate(lines):
            ws5.write(i, 0, txt, ttl if (bold and txt) else None)
        ws5.set_column('A:A', 80)

    size_kb = os.path.getsize(datei) / 1024
    print(f"  Gespeichert: {datei}  ({size_kb:.0f} KB)")

# ── Main ──────────────────────────────────────────────────────────────────────
nasdaq_df = hole_nasdaq100_ticker()

end_datum   = datetime.now()
start_datum = end_datum - timedelta(days=RUECKBLICK_TAGE)

print("\n[2/4] Lade Nasdaq-100 Indexdaten...")
ndx_kurse = hole_ndx_kurse(start_datum, end_datum)
print(f"  Nasdaq-100: {len(ndx_kurse) if ndx_kurse is not None else 0} Datenpunkte geladen.")

ergebnis_df, fehlgeschlagene = verarbeite_alle(nasdaq_df, start_datum, end_datum, ndx_kurse)

top_schwelle  = int(len(ergebnis_df) * TOP_PROZENT)
top_performer = ergebnis_df.head(top_schwelle).copy()

sektor_stats = ergebnis_df.groupby('Sektor').agg(
    RSL_mean=('RSL','mean'), RSL_median=('RSL','median'),
    RSL_count=('RSL','count'), Aend_26T_mean=('Aenderung_26T','mean')
).round(4)
sektor_stats.columns = ['Durchschn_RSL','Median_RSL','Anzahl','Durchschn_26T_Aend']
sektor_stats = sektor_stats.sort_values('Durchschn_RSL', ascending=False)
sektor_top25 = top_performer.groupby('Sektor').size().reindex(sektor_stats.index, fill_value=0)
sektor_stats['In_Top_25%']   = sektor_top25
sektor_stats['Anteil_Top25'] = (sektor_stats['In_Top_25%'] / sektor_stats['Anzahl'] * 100).round(1)

print("\n" + "=" * 70)
print("ERGEBNIS")
print("=" * 70)
print(f"Analysierte Aktien : {len(ergebnis_df)}")
print(f"Top-Performer (25%): {len(top_performer)}")
print(f"\nRSL-Statistiken:")
print(f"  Ø      : {ergebnis_df['RSL'].mean():.4f}")
print(f"  Median : {ergebnis_df['RSL'].median():.4f}")
print(f"  Max    : {ergebnis_df['RSL'].max():.4f}  ({ergebnis_df.iloc[0]['Ticker']})")
print(f"  Min    : {ergebnis_df['RSL'].min():.4f}  ({ergebnis_df.iloc[-1]['Ticker']})")
print(f"\nTop 10 nach RSL:")
for _, r in ergebnis_df.head(10).iterrows():
    print(f"  {r['Rang']:>3}. {r['Ticker']:<6}  RSL={r['RSL']}  26T={r['Aenderung_26T']:+.1f}%")

erstelle_excel(ergebnis_df, top_performer, sektor_stats, fehlgeschlagene, AUSGABE_DATEI)

# ── JSON export ───────────────────────────────────────────────────────────────
def erstelle_json(df, sektor_stats, fehler):
    import json, math

    def safe(v):
        if v is None: return None
        try:
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
            return v
        except Exception:
            return None

    now    = datetime.now()
    top25n = math.ceil(len(df) * TOP_PROZENT)
    beste  = df.loc[df.groupby('Sektor')['RSL'].idxmax()].set_index('Sektor')['Ticker'].to_dict()
    top25_by_sector = df.head(top25n).groupby('Sektor').size().to_dict()

    output = {
        "metadata": {
            "updated":          now.isoformat(timespec='seconds'),
            "updated_display":  now.strftime('%d.%m.%Y %H:%M'),
            "total_analyzed":   len(df),
            "failed":           len(fehler),
            "rsl_period_days":  RSL_PERIODE,
            "index":            "Nasdaq-100",
        },
        "stats": {
            "avg_rsl":       round(float(df['RSL'].mean()), 4),
            "median_rsl":    round(float(df['RSL'].median()), 4),
            "max_rsl":       round(float(df['RSL'].max()), 4),
            "max_ticker":    str(df.iloc[0]['Ticker']),
            "min_rsl":       round(float(df['RSL'].min()), 4),
            "min_ticker":    str(df.iloc[-1]['Ticker']),
            "bullish_count": int((df['RSL'] > 1).sum()),
            "bearish_count": int((df['RSL'] <= 1).sum()),
            "returns_avg": {
                "26t": safe(round(float(df['Aenderung_26T'].dropna().mean()), 2)),
                "1m":  safe(round(float(df['Aenderung_1M'].dropna().mean()),  2)),
                "3m":  safe(round(float(df['Aenderung_3M'].dropna().mean()),  2)),
                "6m":  safe(round(float(df['Aenderung_6M'].dropna().mean()),  2)),
            }
        },
        "rankings": [
            {
                "rank":           int(r['Rang']),
                "ticker":         str(r['Ticker']),
                "company":        str(r['Unternehmen']),
                "sector":         str(r['Sektor']),
                "rsl":            safe(r['RSL']),
                "percentile":     safe(r['Perzentil']),
                "price":          safe(r['Aktueller_Kurs']),
                "change_26t":     safe(r['Aenderung_26T']),
                "change_1m":      safe(r['Aenderung_1M']),
                "change_3m":      safe(r['Aenderung_3M']),
                "change_6m":      safe(r['Aenderung_6M']),
                "pct_over_ma50":  safe(r['Proz_ueber_MA50']),
                "pct_over_ma200": safe(r['Proz_ueber_MA200']),
                "pct_from_high":  safe(r['Proz_vom_Hoch']),
                "vol_ratio":      safe(r['Volumen_Ratio']),
                "rel_vs_ndx":     safe(r['Rel_Staerke_NDX']),
                "beta":           safe(r['Beta']),
                "pe_ratio":       safe(r['KGV']),
                "div_yield":      safe(r['Dividendenrendite']),
            }
            for _, r in df.iterrows()
        ],
        "sectors": [
            {
                "sector":         idx,
                "avg_rsl":        round(float(row['Durchschn_RSL']), 4),
                "median_rsl":     round(float(row['Median_RSL']), 4),
                "count":          int(row['Anzahl']),
                "avg_change_26t": safe(round(float(row['Durchschn_26T_Aend']), 2)),
                "in_top25":       int(top25_by_sector.get(idx, 0)),
                "top25_pct":      round(top25_by_sector.get(idx, 0) / int(row['Anzahl']) * 100, 1),
                "best_ticker":    beste.get(idx, ''),
            }
            for idx, row in sektor_stats.iterrows()
        ]
    }

    json_path = os.path.join('web', 'data', 'nasdaq_rankings.json')
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(json_path) / 1024
    print(f"  JSON gespeichert: {json_path}  ({size_kb:.0f} KB)")

erstelle_json(ergebnis_df, sektor_stats, fehlgeschlagene)
print(f"\nFertig! {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
