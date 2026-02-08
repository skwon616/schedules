import re
import pandas as pd
from supabase import create_client

SUPABASE_URL = "https://rorgbvvlwdnpjfltxtvj.supabase.co"
SUPABASE_SERVICE_KEY = "sb_publishable_xbWeDQ_2Ja8u2yQRFWFprg_dfLHyw4h"  # seed는 service key 권장
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

TEMPLATE_KEY = "2026-02"
EXCEL_PATH = "calendar.xlsx"
SHEET_NAME = "Sheet1"

df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, header=None)

def norm_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def parse_shift_period(cell0: str):
    """
    cell0 예시: "T1,주간", "T2,야간", "T1 주간" 등
    """
    s = cell0.replace(" ", "")
    m = re.search(r"(T[12]).*(주간|야간)", s)
    if not m:
        return None
    shift = m.group(1)
    period = "day" if m.group(2) == "주간" else "night"
    return shift, period

def find_day_of_month_above(df, r, c):
    """
    (r,c) 위쪽으로 스캔하면서 1~31 숫자 찾기
    """
    for rr in range(r, -1, -1):
        v = df.iat[rr, c]
        if pd.isna(v):
            continue
        # 숫자로 바로 들어온 경우
        if isinstance(v, (int, float)) and float(v).is_integer():
            iv = int(v)
            if 1 <= iv <= 31:
                return iv
        # 문자열 숫자
        sv = norm_str(v)
        if sv.isdigit():
            iv = int(sv)
            if 1 <= iv <= 31:
                return iv
    return None

def find_dow_above(df, r, c):
    """
    (선택) 요일도 저장하고 싶으면 '월/화/수/목/금/토/일' 같은 값을 위에서 찾기
    못 찾으면 None
    """
    dows = {"월":"Mon","화":"Tue","수":"Wed","목":"Thu","금":"Fri","토":"Sat","일":"Sun"}
    for rr in range(r, -1, -1):
        sv = norm_str(df.iat[rr, c])
        if sv in dows:
            return dows[sv]
    return None

# 1) 근무자 행만 찾아서 group_code를 템플릿으로 저장
records = []
nrows, ncols = df.shape

for r in range(nrows):
    c0 = norm_str(df.iat[r, 0])  # 근무형태
    c1 = norm_str(df.iat[r, 1])  # 구분(근무자/결원/대근자)
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

# 2) 같은 key(template_key, day, shift, period) 중복 제거(마지막 값 우선)
#    pandas로 정리해서 upsert 안정화
if records:
    tmp = pd.DataFrame(records)
    tmp = tmp.drop_duplicates(subset=["template_key","day_of_month","shift","period"], keep="last")
    records = tmp.to_dict("records")

supabase.table("shift_template").upsert(records).execute()
print("seeded:", len(records))
