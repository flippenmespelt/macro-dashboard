import io
import re

import pandas as pd
import requests
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
        extracted = s.str.extract(
            r"^(?P<year>\d{4})\s*[:\-/ ]?\s*(?:Q\s*)?(?P<q>[1-4])$",
            expand=True,
        )
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


def quarter_label(dt: pd.Timestamp) -> str:
    p = pd.Period(dt, freq="Q")
    return f"{p.year}Q{p.quarter}"


def parse_vintage_label(col: str):
    """
    Liest Vintage-Labels robust aus Headern, z. B.:
      - ROUTPUT66Q2
      - 66Q2
      - 1966Q2
    Gibt (year, quarter) zurück.
    """
    m = re.search(
        r"(?:ROUTPUT)?\s*(\d{2,4})\s*[:\-/ ]?\s*(?:Q\s*)?([1-4])",
        str(col),
        re.IGNORECASE,
    )
    if not m:
        return None

    year_raw = int(m.group(1))
    quarter = int(m.group(2))

    if year_raw < 100:
        # Philly-Fed-Dateien enthalten oft 2-stellige Jahre (z. B. 47Q1, 00Q1, 24Q4).
        # Starre Grenzen (z. B. ab 66 => 19xx) brechen für ältere Historien wie 1947.
        # Deshalb wird das Jahrhundert relativ zum aktuellen Jahr aufgelöst:
        # - Werte größer als das aktuelle yy (+1 Puffer) gehören ins 20. Jh.
        # - sonst ins 21. Jh.
        current_yy = pd.Timestamp.now().year % 100
        pivot = current_yy + 1
        year = 1900 + year_raw if year_raw > pivot else 2000 + year_raw
    else:
        year = year_raw

    return (year, quarter)


def vintage_sort_key(col: str):
    """
    Sortiert 2-stellige PhillyFed-Vintage-Jahre chronologisch.

    Dynamische Jahrhundert-Logik wie in parse_vintage_label().
    """
    parsed = parse_vintage_label(col)
    if parsed is None:
        return (float("inf"), float("inf"))

    year, q = parsed
    return (year, q)


def vintage_period_to_column(columns: list[str]) -> dict[pd.Period, str]:
    """Mappt Quarter-Periode auf exakt vorhandenen Headernamen aus den Rohdaten."""
    out = {}
    for col in columns:
        parsed = parse_vintage_label(col)
        if parsed is None:
            continue
        year, quarter = parsed
        out[pd.Period(f"{year}Q{quarter}", freq="Q")] = col
    return out


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def load_vintage_matrix() -> pd.DataFrame:
    content = download_excel_bytes(EXCEL_URL)
    xls = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    sheet = "ROUTPUT" if "ROUTPUT" in xls.sheet_names else xls.sheet_names[0]

    raw = pd.read_excel(xls, sheet_name=sheet, header=None, engine="openpyxl")
    raw = raw.dropna(how="all").reset_index(drop=True)

    # Zeile mit "Date" finden
    col0 = raw.iloc[:, 0].astype(str).str.strip()
    date_rows = raw.index[col0.str.lower().eq("date")].tolist()
    if not date_rows:
        date_rows = raw.index[col0.str.lower().str.contains(r"\bdate\b", na=False)].tolist()
    if not date_rows:
        raise RuntimeError("Keine Zeile mit 'Date' gefunden.")

    anchor = date_rows[0]

    # Vintage-Headerzeile: enthält mehrfach ROUTPUT##Q#
    vintage_row = None
    for r in range(anchor, min(anchor + 12, len(raw))):
        row_vals = raw.iloc[r].astype(str).fillna("").tolist()
        hits = sum(parse_vintage_label(v) is not None for v in row_vals[1:])
        if hits >= 3:
            vintage_row = r
            break
    if vintage_row is None:
        vintage_row = anchor + 1

    colnames = raw.iloc[vintage_row].tolist()
    colnames[0] = "Date"
    colnames = [str(c).strip() for c in colnames]
    colnames = make_unique(colnames)

    df = raw.iloc[vintage_row + 1 :].copy()
    df.columns = colnames

    df = df.dropna(how="all").dropna(axis=1, how="all")
    df["Date"] = parse_quarter_dates(df["Date"])
    df = df.dropna(subset=["Date"]).sort_values("Date")
    if df.empty:
        sample = raw.iloc[vintage_row + 1 : vintage_row + 11, 0].astype(str).tolist()
        raise RuntimeError(
            "Alle Date-Werte konnten nicht als Quartale geparst werden. "
            f"Beispiele aus Date-Spalte: {sample}"
        )

    value_cols = [c for c in df.columns if c != "Date"]
    df[value_cols] = df[value_cols].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(axis=1, how="all")

    # Nur echte Vintage-Spalten behalten (ROUTPUT..)
    vintage_cols = [c for c in df.columns if c != "Date" and parse_vintage_label(c) is not None]
    if not vintage_cols:
        raise RuntimeError("Keine Vintage-Spalten im Format ROUTPUT##Q# erkannt. Header ggf. anders.")
    if len(vintage_cols) < 3:
        raise RuntimeError(
            "Zu wenige Vintage-Spalten erkannt (<3). "
            "Möglicherweise wurde die Header-Zeile nicht korrekt gefunden."
        )

    # Sortieren nach Vintage (yy, q)
    vintage_cols = sorted(vintage_cols, key=vintage_sort_key)
    df = df[["Date"] + vintage_cols]

    return df


def compute_diagonal_qoq_saar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Für Quartal t:
      x_t = df.loc[t, vintage(t)]
      x_{t-1} = df.loc[t-1, vintage(t)]
    QoQ SAAR = ((x_t/x_{t-1})^4 - 1) * 100
    """
    out = df[["Date"]].copy()
    out["Quarter"] = out["Date"].apply(quarter_label)

    vintage_by_period = vintage_period_to_column([c for c in df.columns if c != "Date"])
    out["Vintage_used"] = [vintage_by_period.get(pd.Period(dt, freq="Q")) for dt in out["Date"]]

    values_t = []
    values_tm1 = []

    # Robust gegen unterschiedliche Date-Timestamps (Quarter-Start vs Quarter-End)
    df_period = df.copy()
    df_period["Quarter_period"] = pd.PeriodIndex(df_period["Date"], freq="Q")
    df_period = df_period.set_index("Quarter_period")

    for dt in out["Date"]:
        q_period = pd.Period(dt, freq="Q")
        vcol = vintage_by_period.get(q_period)

        if not vcol:
            values_t.append(float("nan"))
            values_tm1.append(float("nan"))
            continue

        # aktuelles Quartal t
        x_t = df_period.at[q_period, vcol] if (q_period in df_period.index and vcol in df_period.columns) else float("nan")

        # Vorquartal t-1, gleiches Vintage
        prev_period = q_period - 1
        x_tm1 = (
            df_period.at[prev_period, vcol]
            if (prev_period in df_period.index and vcol in df_period.columns)
            else float("nan")
        )

        values_t.append(x_t)
        values_tm1.append(x_tm1)

    out["Current_value"] = values_t
    out["Previous_value"] = values_tm1

    out["qoq_saar"] = ((out["Current_value"] / out["Previous_value"]) ** 4 - 1) * 100

    return out


def rolling_robust_z(series: pd.Series, window: int):
    """
    z_t = (x_t - median(window)) / (1.4826 * median(abs(x - median(window))))
    Liefert zusätzlich median, mad, denom für Trace.
    """
    z = pd.Series(index=series.index, dtype="float64")
    med_s = pd.Series(index=series.index, dtype="float64")
    mad_s = pd.Series(index=series.index, dtype="float64")
    denom_s = pd.Series(index=series.index, dtype="float64")

    for i in range(len(series)):
        if i + 1 < window:
            continue
        w = series.iloc[i + 1 - window : i + 1].dropna()
        if len(w) < window:
            continue

        med = w.median()
        mad = (w - med).abs().median()
        denom = 1.4826 * mad

        med_s.iloc[i] = med
        mad_s.iloc[i] = mad
        denom_s.iloc[i] = denom

        if denom and pd.notna(denom) and denom != 0:
            z.iloc[i] = (series.iloc[i] - med) / denom

    return z, med_s, mad_s, denom_s


# ---------------- UI ----------------
st.title("Macro Dashboard – Philly Fed RTDSM (Diagonal Vintage)")

try:
    matrix = load_vintage_matrix()
except Exception as e:
    st.error(f"Fehler beim Laden/Parsen der Excel-Datei: {e}")
    st.stop()

calc = compute_diagonal_qoq_saar(matrix)

WINDOW_Q = 20 * 4  # 20 Jahre
z, z_med, z_mad, z_denom = rolling_robust_z(calc["qoq_saar"], WINDOW_Q)
calc["robust_z_20y_qoq"] = z
calc["z_median_20y"] = z_med
calc["z_mad_20y"] = z_mad
calc["z_denom_20y"] = z_denom

st.subheader("QoQ SAAR (berechnet als: x(t,v=t) vs x(t-1,v=t))")
st.line_chart(calc.set_index("Date")["qoq_saar"])

st.subheader("Robuster Z-Score (Median/MAD, rollend 20 Jahre) auf QoQ SAAR")
st.line_chart(calc.set_index("Date")["robust_z_20y_qoq"])

st.subheader("Berechnete Tabelle")
st.dataframe(
    calc[
        [
            "Date",
            "Vintage_used",
            "Previous_value",
            "Current_value",
            "qoq_saar",
            "robust_z_20y_qoq",
        ]
    ].rename(
        columns={
            "Date": "Datum",
            "Vintage_used": "Vintage (zum Zeitpunkt)",
            "Previous_value": "Letzter Wert (t-1, gleiches Vintage)",
            "Current_value": "Aktueller Wert (t, gleiches Vintage)",
            "qoq_saar": "QoQ SAAR (%)",
            "robust_z_20y_qoq": "Robuster Z-Score (20 Jahre)",
        }
    ),
    use_container_width=True,
)

with st.expander("🔎 Trace / Nachvollziehen wie Excel"):
    st.markdown(
        """
Wähle ein Quartal. Du siehst dann exakt:
- **welche Vintage-Spalte** verwendet wurde,
- **welche zwei Werte** in die QoQ-Formel gehen,
- sowie **Median/MAD/Nenner** des 20y-Fensters für den robusten Z-Score.
"""
    )

    # wähle ein Quartal, das Z-Score hat
    selectable = calc.dropna(subset=["robust_z_20y_qoq"]).copy()
    if selectable.empty:
        st.info("Noch keine Z-Score-Werte (zu wenig Historie für 20 Jahre Lookback).")
    else:
        options = list(selectable["Quarter"].values)
        q = st.selectbox("Quartal auswählen", options, index=len(options) - 1)

        row = selectable.loc[selectable["Quarter"] == q].iloc[0]
        st.markdown("### QoQ-Input")
        st.write(
            {
                "Quarter": row["Quarter"],
                "Vintage_used": row["Vintage_used"],
                "Current_value (t, vintage=t)": row["Current_value"],
                "Previous_value (t-1, same vintage)": row["Previous_value"],
                "QoQ SAAR": row["qoq_saar"],
            }
        )

        st.markdown("### Z-Score-Input (20y, robust wie Sheets)")
        st.write(
            {
                "Lookback (quarters)": WINDOW_Q,
                "Median(window)": row["z_median_20y"],
                "MAD(window)": row["z_mad_20y"],
                "Denom = 1.4826 * MAD": row["z_denom_20y"],
                "Robust Z": row["robust_z_20y_qoq"],
            }
        )

with st.expander("Rohdaten-Matrix (Vintage-Spalten)"):
    st.dataframe(matrix, use_container_width=True)
