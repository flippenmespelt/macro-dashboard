import io
import re
import requests
import pandas as pd
import streamlit as st

EXCEL_URL = "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/real-time-data/data-files/xlsx/ROUTPUTQvQd.xlsx?sc_lang=en&hash=34FA1C6BF0007996E1885C8C32E3BEF9"

st.set_page_config(page_title="Macro Dashboard", layout="wide")


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def download_excel_bytes(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def parse_quarter_dates(date_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(date_series, errors="coerce")

    missing = dt.isna()
    if missing.any():
        s = date_series.astype(str).str.strip()
        extracted = s.str.extract(r"^(?P<year>\d{4})\s*[:\-/ ]?\s*Q(?P<q>[1-4])$", expand=True)
        qmask = missing & extracted["year"].notna()
        if qmask.any():
            periods = pd.PeriodIndex(
                extracted.loc[qmask, "year"] + "Q" + extracted.loc[qmask, "q"],
                freq="Q",
            )
            dt.loc[qmask] = periods.to_timestamp(how="end").normalize()

    return dt


def make_unique(names):
    seen = {}
    out = []
    for n in names:
        n = str(n)
        if n not in seen:
            seen[n] = 0
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
    return out


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def load_and_process_data() -> pd.DataFrame:
    content = download_excel_bytes(EXCEL_URL)
    xls = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    sheet = "ROUTPUT" if "ROUTPUT" in xls.sheet_names else xls.sheet_names[0]

    raw = pd.read_excel(xls, sheet_name=sheet, header=None, engine="openpyxl")
    raw = raw.dropna(how="all").reset_index(drop=True)

    # 1) Zeile mit "Date" finden
    col0 = raw.iloc[:, 0].astype(str).str.strip()
    date_rows = raw.index[col0.str.lower().eq("date")].tolist()
    if not date_rows:
        date_rows = raw.index[col0.str.lower().str.contains(r"\bdate\b", na=False)].tolist()
    if not date_rows:
        raise RuntimeError("Keine Zeile mit 'Date' gefunden.")

    header_anchor = date_rows[0]

    # 2) Vintage-Headerzeile finden: enthält mehrfach ROUTPUT##Q#
    pat = re.compile(r"ROUTPUT\d{2}Q[1-4]", re.IGNORECASE)
    vintage_row = None
    for r in range(header_anchor, min(header_anchor + 12, len(raw))):
        row_vals = raw.iloc[r].astype(str).fillna("").tolist()
        hits = sum(bool(pat.search(v)) for v in row_vals[1:])
        if hits >= 3:
            vintage_row = r
            break
    if vintage_row is None:
        vintage_row = header_anchor + 1  # fallback

    # 3) Spaltennamen setzen
    colnames = raw.iloc[vintage_row].tolist()
    colnames[0] = "Date"
    colnames = [str(c).strip() for c in colnames]
    colnames = make_unique(colnames)

    df = raw.iloc[vintage_row + 1 :].copy()
    df.columns = colnames

    df = df.dropna(how="all").dropna(axis=1, how="all")

    df["Date"] = parse_quarter_dates(df["Date"])
    df = df.dropna(subset=["Date"]).sort_values("Date")

    value_cols = [c for c in df.columns if c != "Date"]
    df[value_cols] = df[value_cols].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(axis=1, how="all")

    return df


def pick_vintage_values(df: pd.DataFrame, mode: str) -> pd.Series:
    value_cols = [c for c in df.columns if c != "Date"]
    data = df[value_cols].dropna(axis=1, how="all")

    if data.shape[1] == 0:
        return pd.Series([float("nan")] * len(df), index=df.index)

    def vintage_key(c):
        m = re.search(r"ROUTPUT(\d{2})Q([1-4])", str(c), re.IGNORECASE)
        if not m:
            return (10**9, 9)
        return (int(m.group(1)), int(m.group(2)))

    cols_sorted = sorted(list(data.columns), key=vintage_key)
    data = data[cols_sorted]

    if mode == "latest":
        return data.ffill(axis=1).iloc[:, -1]
    if mode == "first":
        return data.bfill(axis=1).iloc[:, 0]
    raise ValueError("mode muss 'latest' oder 'first' sein")


def calc_qoq_saar(level_series: pd.Series) -> pd.Series:
    return ((level_series / level_series.shift(1)) ** 4 - 1) * 100


def rolling_robust_z(series: pd.Series, window: int) -> pd.Series:
    """
    Google-Sheets-äquivalent:
    z_t = (x_t - median(window)) / (1.4826 * median(abs(x - median(window))))
    """

    def robust_z(x: pd.Series) -> float:
        x = x.dropna()
        if len(x) < window:
            return float("nan")
        med = x.median()
        mad = (x - med).abs().median()
        denom = 1.4826 * mad
        if denom == 0 or pd.isna(denom):
            return float("nan")
        return (x.iloc[-1] - med) / denom

    return series.rolling(window=window, min_periods=window).apply(robust_z, raw=False)


# ---------------- UI ----------------
st.title("Macro Dashboard – Philly Fed RTDSM (Vintage-sicher)")

with st.sidebar:
    st.markdown("### Einstellungen")
    choice = st.radio(
        "Vintage-Auswahl",
        ("Latest (aktuellster Wert je Quartal)", "First release (erste Schätzung je Quartal)"),
    )
    mode = "latest" if choice.startswith("Latest") else "first"

try:
    raw = load_and_process_data()
except Exception as e:
    st.error(f"Fehler beim Laden/Parsen der Excel-Datei: {e}")
    st.stop()

df = raw.copy()
df["value"] = pick_vintage_values(df, mode=mode)
df["qoq_saar"] = calc_qoq_saar(df["value"])

WINDOW_Q = 20 * 4  # 20 Jahre rollend
df["robust_z_20y_qoq"] = rolling_robust_z(df["qoq_saar"], WINDOW_Q)

st.subheader("QoQ SAAR (annualisiert)")
st.line_chart(df.set_index("Date")["qoq_saar"])

st.subheader("Robuster Z-Score (QoQ SAAR, Median/MAD, rollend 20 Jahre)")
st.line_chart(df.set_index("Date")["robust_z_20y_qoq"])

st.subheader("Auszug")
st.dataframe(df[["Date", "value", "qoq_saar", "robust_z_20y_qoq"]], use_container_width=True)

with st.expander("Rohdaten inkl. Vintagespalten (Header-Check)"):
    st.dataframe(df, use_container_width=True)
