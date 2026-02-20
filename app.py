import bisect
import io
import re
import datetime as dt

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

EXCEL_URL = "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/real-time-data/data-files/xlsx/ROUTPUTQvQd.xlsx?sc_lang=en&hash=34FA1C6BF0007996E1885C8C32E3BEF9"
BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"
FRED_SERIES_OBS_URL = "https://api.stlouisfed.org/fred/series/observations"

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


def compute_diagonal_delta(df: pd.DataFrame, lag_quarters: int = 1) -> pd.DataFrame:
    """
    Für Beobachtungsquartal t:
      - suche das früheste Vintage v >= t + lag_quarters, das für t einen Wert hat
      - x_t   = value(t, v)
      - x_t-1 = value(t-1, v)
      - delta = x_t - x_t-1
    """
    out = df[["Date"]].copy()
    out["Quarter"] = out["Date"].apply(quarter_label)

    df_period = df.copy()
    df_period["Obs_period"] = pd.PeriodIndex(df_period["Date"], freq="Q")
    df_period = df_period.set_index("Obs_period")

    vintage_cols = [c for c in df.columns if c != "Date" and parse_vintage_label(c) is not None]
    vintage_cols = sorted(vintage_cols, key=vintage_sort_key)

    vintage_by_period = vintage_period_to_column(vintage_cols)
    vintage_period_cols = sorted(vintage_by_period.items(), key=lambda kv: kv[0])
    vintage_periods = [p for p, _ in vintage_period_cols]

    first_vintage = vintage_periods[0] if vintage_periods else None

    used_cols = []
    values_t = []
    values_tm1 = []

    for dt in out["Date"]:
        t = pd.Period(dt, freq="Q")
        desired = t + lag_quarters

        if first_vintage is not None and desired < first_vintage:
            # Für sehr frühe Beobachtungen (z. B. 1947–1965) existieren keine
            # vorherigen Vintages. Dann durchgehend die erste verfügbare
            # Vintage-Spalte nutzen.
            i = 0
        else:
            i = bisect.bisect_left(vintage_periods, desired)

        vcol = None
        while i < len(vintage_period_cols):
            _, col = vintage_period_cols[i]
            val = df_period.at[t, col] if (t in df_period.index) else float("nan")
            if pd.notna(val):
                vcol = col
                break
            i += 1

        used_cols.append(vcol)

        if vcol is None or (t not in df_period.index):
            values_t.append(float("nan"))
            values_tm1.append(float("nan"))
            continue

        x_t = df_period.at[t, vcol]
        prev = t - 1
        x_tm1 = df_period.at[prev, vcol] if (prev in df_period.index) else float("nan")

        values_t.append(x_t)
        values_tm1.append(x_tm1)

    out["Vintage_used"] = used_cols
    out["Current_value"] = values_t
    out["Previous_value"] = values_tm1

    out["delta"] = out["Current_value"] - out["Previous_value"]
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


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def fetch_next_bea_gdp_advance_estimate() -> str | None:
    response = requests.get(BEA_SCHEDULE_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    year = None
    for line in lines[:200]:
        match = re.search(r"\bYear\s+(\d{4})\s+Release\b", line)
        if match:
            year = int(match.group(1))
            break

    if year is None:
        year = dt.datetime.now().year

    for i, line in enumerate(lines):
        if "GDP (Advance Estimate)" not in line:
            continue

        date_str = None
        time_str = None

        for j in range(i - 1, max(-1, i - 15), -1):
            if re.match(
                r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}$",
                lines[j],
            ):
                date_str = lines[j]
                break

        for j in range(i - 1, max(-1, i - 15), -1):
            if re.match(r"^\d{1,2}:\d{2}\s+(AM|PM)$", lines[j]):
                time_str = lines[j]
                break

        if date_str and time_str:
            return f"{date_str}, {year} – {time_str} ET"

    return None


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def fetch_fred_series_observations(series_id: str) -> pd.DataFrame:
    api_key = st.secrets["FRED_API_KEY"]
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    response = requests.get(FRED_SERIES_OBS_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    obs = pd.DataFrame(data.get("observations", []))
    if obs.empty:
        return pd.DataFrame(columns=["date", "value"])

    obs["date"] = pd.to_datetime(obs["date"], errors="coerce")
    obs["value"] = pd.to_numeric(obs["value"], errors="coerce")
    obs = obs.dropna(subset=["date"]).sort_values("date")
    return obs[["date", "value"]]


def add_yoy_absolute_change(obs: pd.DataFrame, periods: int = 12) -> pd.DataFrame:
    """Berechnet die absolute YoY-Veränderung für monatliche Reihen (z. B. FEDFUNDS)."""
    out = obs.copy()
    out["yoy_abs"] = out["value"] - out["value"].shift(periods)
    return out


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def fetch_fred_next_release_date_from_page(series_id: str) -> str | None:
    url = f"https://fred.stlouisfed.org/series/{series_id}"
    response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = " ".join(soup.get_text(" ").split())
    match = re.search(r"Next Release Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", text)

    return match.group(1) if match else None


# ---------------- UI ----------------
st.title("Macro Dashboard – Philly Fed RTDSM (Diagonal Vintage)")
st.caption("Quelle")
st.markdown(f"{EXCEL_URL}")

try:
    matrix = load_vintage_matrix()
except Exception as e:
    st.error(f"Fehler beim Laden/Parsen der Excel-Datei: {e}")
    st.stop()

calc = compute_diagonal_delta(matrix, lag_quarters=1)

WINDOW_Q = 20 * 4  # 20 Jahre
z, z_med, z_mad, z_denom = rolling_robust_z(calc["delta"], WINDOW_Q)
calc["robust_z_20y_delta"] = z
calc["z_median_20y"] = z_med
calc["z_mad_20y"] = z_mad
calc["z_denom_20y"] = z_denom

latest = calc.dropna(subset=["qoq_saar"]).sort_values("Date").tail(1)
if latest.empty:
    st.warning("Noch keine gültigen QoQ-SAAR-Werte vorhanden.")
else:
    try:
        next_release = fetch_next_bea_gdp_advance_estimate()
    except Exception:
        next_release = None

    try:
        ffr_obs = fetch_fred_series_observations("FEDFUNDS")
        ffr_obs = add_yoy_absolute_change(ffr_obs, periods=12)
        ffr_latest = ffr_obs.dropna(subset=["value"]).tail(1)
        ffr_next_release = fetch_fred_next_release_date_from_page("FEDFUNDS")
        ffr_error = None
    except Exception as exc:
        ffr_obs = pd.DataFrame(columns=["date", "value"])
        ffr_latest = pd.DataFrame()
        ffr_next_release = None
        ffr_error = exc

    try:
        m2_obs = fetch_fred_series_observations("M2SL")
        m2_obs = m2_obs.dropna(subset=["value"]).sort_values("date")
        m2_obs["yoy_pct"] = (m2_obs["value"] / m2_obs["value"].shift(12) - 1) * 100

        window_m = 20 * 12
        m2_z, m2_med, m2_mad, m2_denom = rolling_robust_z(m2_obs["yoy_pct"], window_m)
        m2_obs["robust_z_20y_yoy"] = m2_z
        m2_obs["z_median_20y_yoy"] = m2_med
        m2_obs["z_mad_20y_yoy"] = m2_mad
        m2_obs["z_denom_20y_yoy"] = m2_denom

        m2_latest = m2_obs.dropna(subset=["yoy_pct"]).tail(1)
        m2_next_release = fetch_fred_next_release_date_from_page("M2SL")
        m2_error = None
    except Exception as exc:
        m2_obs = pd.DataFrame(columns=["date", "value", "yoy_pct", "robust_z_20y_yoy"])
        m2_latest = pd.DataFrame()
        m2_next_release = None
        m2_error = exc

    summary_rows = [
        {
            "Serie": "GDP",
            "Letztes Datum": latest["Date"].iloc[0].date(),
            "Aktuell": float(latest["qoq_saar"].iloc[0]),
            "Z-Score": (
                float(latest["robust_z_20y_delta"].iloc[0])
                if pd.notna(latest["robust_z_20y_delta"].iloc[0])
                else float("nan")
            ),
            "Next Release": next_release if next_release else "siehe BEA Schedule",
            "Einheit": "% (SAAR)",
            "YoY absolut": float("nan"),
        }
    ]

    if not ffr_latest.empty:
        summary_rows.append(
            {
                "Serie": "FFR",
                "Letztes Datum": ffr_latest["date"].iloc[0].date(),
                "Aktuell": float(ffr_latest["value"].iloc[0]),
                "Z-Score": float("nan"),
                "Next Release": ffr_next_release if ffr_next_release else "siehe FRED Series Page",
                "Einheit": "% p.a.",
                "YoY absolut": (
                    float(ffr_latest["yoy_abs"].iloc[0])
                    if pd.notna(ffr_latest["yoy_abs"].iloc[0])
                    else float("nan")
                ),
            }
        )

    if not m2_latest.empty:
        summary_rows.append(
            {
                "Serie": "M2",
                "Letztes Datum": m2_latest["date"].iloc[0].date(),
                "Aktuell": float(m2_latest["yoy_pct"].iloc[0]),
                "Z-Score": (
                    float(m2_latest["robust_z_20y_yoy"].iloc[0])
                    if pd.notna(m2_latest["robust_z_20y_yoy"].iloc[0])
                    else float("nan")
                ),
                "Next Release": m2_next_release if m2_next_release else "siehe FRED Series Page",
                "Einheit": "% YoY",
                "YoY absolut": float("nan"),
            }
        )

    summary = pd.DataFrame(summary_rows)

    st.subheader("Kompakt-Dashboard")
    event = st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    st.caption(f"Quelle Next Release GDP: {BEA_SCHEDULE_URL}")
    st.caption("Quelle FFR & Next Release: https://fred.stlouisfed.org/series/FEDFUNDS")
    st.caption("Quelle M2 & Next Release: https://fred.stlouisfed.org/series/M2SL")

    selected = "GDP"
    if event and event.selection and event.selection.rows:
        selected = summary.iloc[event.selection.rows[0]]["Serie"]

    st.divider()
    if selected == "FFR":
        st.header("FFR Details")
        if ffr_error:
            st.warning(f"FFR konnte nicht geladen werden: {ffr_error}")
        elif ffr_obs.empty:
            st.warning("FFR-Zeitreihe enthält keine Werte.")
        else:
            st.subheader("FFR Verlauf (FEDFUNDS)")
            st.line_chart(ffr_obs.set_index("date")["value"])
            st.subheader("FFR YoY absolut (Differenz zu vor 12 Monaten)")
            st.line_chart(ffr_obs.set_index("date")["yoy_abs"])
            st.dataframe(ffr_obs, use_container_width=True)
    elif selected == "M2":
        st.header("M2 Details")
        if m2_error:
            st.warning(f"M2 konnte nicht geladen werden: {m2_error}")
        elif m2_obs.empty:
            st.warning("M2-Zeitreihe enthält keine Werte.")
        else:
            st.subheader("M2 YoY Change (%)")
            st.line_chart(m2_obs.set_index("date")["yoy_pct"])

            st.subheader("Robuster Z-Score (20 Jahre) auf YoY (%)")
            st.line_chart(m2_obs.set_index("date")["robust_z_20y_yoy"])

            st.dataframe(
                m2_obs[["date", "value", "yoy_pct", "robust_z_20y_yoy"]],
                use_container_width=True,
            )
    else:
        st.header("GDP Details")
        st.subheader("RGDP QoQ SAAR")
        st.line_chart(calc.set_index("Date")["qoq_saar"])

        st.subheader("RGDP QoQ SAAR Robuster Z-Score 20y")
        st.line_chart(calc.set_index("Date")["robust_z_20y_delta"])

        st.subheader("Berechnete Tabelle")
        st.dataframe(
            calc[
                [
                    "Date",
                    "Vintage_used",
                    "Previous_value",
                    "Current_value",
                    "delta",
                    "robust_z_20y_delta",
                ]
            ].rename(
                columns={
                    "Date": "Datum",
                    "Vintage_used": "Vintage (genutzt)",
                    "Previous_value": "Wert (t-1, gleiches Vintage)",
                    "Current_value": "Wert (t, gleiches Vintage)",
                    "delta": "Änderung (t - (t-1))",
                    "robust_z_20y_delta": "Robuster Z-Score (20 Jahre)",
                }
            ),
            use_container_width=True,
        )

with st.expander("🔎 Trace / Nachvollziehen wie Excel"):
    st.markdown(
        """
Wähle ein Quartal. Du siehst dann exakt:
- **welche Vintage-Spalte** verwendet wurde,
- **welche zwei Werte** in die delta-Formel gehen,
- sowie **Median/MAD/Nenner** des 20y-Fensters für den robusten Z-Score.
"""
    )

    # wähle ein Quartal, das Z-Score hat
    selectable = calc.dropna(subset=["robust_z_20y_delta"]).copy()
    if selectable.empty:
        st.info("Noch keine Z-Score-Werte (zu wenig Historie für 20 Jahre Lookback).")
    else:
        options = list(selectable["Quarter"].values)
        q = st.selectbox("Quartal auswählen", options, index=len(options) - 1)

        row = selectable.loc[selectable["Quarter"] == q].iloc[0]
        st.markdown("### Delta-Input")
        st.write(
            {
                "Quarter": row["Quarter"],
                "Vintage_used": row["Vintage_used"],
                "Current_value (t, vintage≈t+1)": row["Current_value"],
                "Previous_value (t-1, same vintage)": row["Previous_value"],
                "delta": row["delta"],
            }
        )

        st.markdown("### Z-Score-Input (20y, robust wie Sheets)")
        st.write(
            {
                "Lookback (quarters)": WINDOW_Q,
                "Median(window)": row["z_median_20y"],
                "MAD(window)": row["z_mad_20y"],
                "Denom = 1.4826 * MAD": row["z_denom_20y"],
                "Robust Z": row["robust_z_20y_delta"],
            }
        )

with st.expander("Rohdaten-Matrix (Vintage-Spalten)"):
    st.dataframe(matrix, use_container_width=True)
