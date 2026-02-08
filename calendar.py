import re
import pandas as pd
import streamlit as st
from supabase import create_client

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]              # 일반용
SUPABASE_SERVICE_KEY = st.secrets["SUPABASE_SERVICE_KEY"] 
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TEMPLATE_KEY = "2026-02"
SHEET_NAME = "Sheet1"

def norm_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def parse_shift_period(cell0: str):
    s = cell0.replace(" ", "")
    m = re.search(r"(T[12]).*(주간|야간)", s)
    if not m:
        return None
    shift = m.group(1)
    period = "day" if m.group(2) == "주간" else "night"
    return shift, period

def find_day_of_month_above(df, r, c):
    for rr in range(r, -1, -1):
        v = df.iat[rr, c]
        if pd.isna(v):
            continue
        if isinstance(v, (int, float)) and float(v).is_integer():
            iv = int(v)
            if 1 <= iv <= 31:
                return iv
        sv = norm_str(v)
        if sv.isdigit():
            iv = int(sv)
            if 1 <= iv <= 31:
                return iv
    return None

def find_dow_above(df, r, c):
    dows = {"월":"Mon","화":"Tue","수":"Wed","목":"Thu","금":"Fri","토":"Sat","일":"Sun"}
    for rr in range(r, -1, -1):
        sv = norm_str(df.iat[rr, c])
        if sv in dows:
            return dows[sv]
    return None

def seed_from_excel_bytes(excel_bytes: bytes):
    df = pd.read_excel(excel_bytes, sheet_name=SHEET_NAME, header=None)

    records = []
    nrows, ncols = df.shape

    for r in range(nrows):
        c0 = norm_str(df.iat[r, 0])
        c1 = norm_str(df.iat[r, 1])
        sp = parse_shift_period(c0)
        if not sp:
            continue
        if c1 != "근무자":
            continue

        shift, period = sp

        for c in range(2, ncols):
            g = norm_str(df.iat[r, c])
            if not g or g == "-":
                continue

            day = find_day_of_month_above(df, r, c)
            if day is None:
                continue

            dow = find_dow_above(df, r, c)

            records.append({
                "template_key": TEMPLATE_KEY,
                "day_of_month": int(day),
                "dow": dow,
                "shift": shift,
                "period": period,
                "group_code": g,
            })

    if not records:
        return 0

    tmp = pd.DataFrame(records).drop_duplicates(
        subset=["template_key","day_of_month","shift","period"],
        keep="last"
    )
    records = tmp.to_dict("records")

    supabase.table("shift_template").upsert(records).execute()
    return len(records)

st.title("2026년 2월 근무표")

with st.expander("템플릿 업로드/적재 (calendar.xlsx)"):
    uploaded = st.file_uploader("calendar.xlsx 업로드", type=["xlsx"])
    if uploaded is not None:
        if st.button("Supabase에 템플릿 적재(seed)"):
            n = seed_from_excel_bytes(uploaded)
            st.success(f"shift_template upsert 완료: {n} rows")
