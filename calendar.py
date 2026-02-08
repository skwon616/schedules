# schedules/calendar.py
import re
import pandas as pd
import streamlit as st
from supabase import create_client

# =========================================================
# 0) 상수 / 그룹 정의
# =========================================================
APP_TITLE = "2026년 2월 근무표"
TEMPLATE_KEY = "2026-02"     # 🔥 고정
SHEET_NAME = "Sheet1"

GROUPS = {
    "A1": {"AA", "BB", "CC", "DD"},
    "A2": {"EE", "FF", "GG", "HH"},
    "B1": {"QQ", "RR", "SS", "TT"},
    "B2": {"UU", "VV", "WW", "XX"},
    "C1": {"YY", "ZZ", "ZA", "ZB"},
    "C2": {"ZC", "ZD", "ZE", "ZF"},
    "D1": {"II", "JJ", "KK", "LL"},
    "D2": {"MM", "NN", "OO", "PP"},
}

PAIR_BY_LETTER = {
    "A": ["A1", "A2"],
    "B": ["B1", "B2"],
    "C": ["C1", "C2"],
    "D": ["D1", "D2"],
}

# 대근 규칙
DAY_RULE = {"A": "B", "B": "C", "C": "D", "D": "A"}
NIGHT_RULE = {"A": "D", "B": "A", "C": "B", "D": "C"}


# =========================================================
# 1) Streamlit 기본 설정
# =========================================================
st.set_page_config(layout="wide", page_title=APP_TITLE)
st.title(APP_TITLE)


# =========================================================
# 2) Secrets / Supabase 클라이언트
# =========================================================
def get_secret(key: str) -> str:
    v = st.secrets.get(key)
    if v is None or str(v).strip() == "":
        st.error(f"Streamlit Secrets에 '{key}' 값이 없습니다.")
        st.stop()
    return str(v).strip()
    
SUPABASE_URL = "https://rorgbvvlwdnpjfltxtvj.supabase.co"
SUPABASE_KEY = "sb_publishable_xbWeDQ_2Ja8u2yQRFWFprg_dfLHyw4h"
SUPABASE_SERVICE_KEY = "sb_secret_HZXy1kBPapoPjMTSsTWqQQ_mAHc8SO_"

# 일반(앱) / 관리자(seed) 분리
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# =========================================================
# 3) 유틸: 룰 / 후보 생성
# =========================================================
def substitute_candidate_groups(group_code: str, period: str) -> list[str]:
    letter = group_code[0].upper()
    if period == "day":
        target_letter = DAY_RULE[letter]
    else:
        target_letter = NIGHT_RULE[letter]
    return PAIR_BY_LETTER[target_letter]

def members_of_groups(group_codes: list[str]) -> list[str]:
    s = set()
    for g in group_codes:
        s |= set(GROUPS.get(g, set()))
    return sorted(s)


# =========================================================
# 4) Supabase IO
# =========================================================
def load_template(template_key: str):
    resp = supabase.table("shift_template") \
        .select("*") \
        .eq("template_key", template_key) \
        .order("day_of_month") \
        .execute()
    return resp.data or []

def load_assignments(template_key: str):
    resp = supabase.table("shift_assignment") \
        .select("*") \
        .eq("template_key", template_key) \
        .execute()
    data = resp.data or []
    return {(r["day_of_month"], r["shift"], r["period"]): r for r in data}

def upsert_assignment(template_key: str, day: int, shift: str, period: str,
                      vacancy_name: str | None, substitute_name: str | None):
    payload = {
        "template_key": template_key,
        "day_of_month": int(day),
        "shift": shift,
        "period": period,
        "vacancy_name": vacancy_name,
        "substitute_name": substitute_name,
    }
    supabase.table("shift_assignment").upsert(payload).execute()


# =========================================================
# 5) Seed: 업로드된 엑셀을 월 전체로 파싱하여 shift_template upsert
#     - "근무자" 행만 사용
#     - 각 셀 기준 같은 열의 위쪽에서 1~31 일자를 찾아 매핑
# =========================================================
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

def seed_from_excel_bytes(uploaded_file) -> int:
    df = pd.read_excel(uploaded_file, sheet_name=SHEET_NAME, header=None)

    records = []
    nrows, ncols = df.shape

    for r in range(nrows):
        c0 = norm_str(df.iat[r, 0])  # 근무형태: T1,주간 등
        c1 = norm_str(df.iat[r, 1])  # 구분: 근무자/결원/대근자
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
        subset=["template_key", "day_of_month", "shift", "period"],
        keep="last"
    )
    records = tmp.to_dict("records")

    # ✅ seed는 관리자 키로 upsert
    supabase_admin.table("shift_template").upsert(records).execute()
    return len(records)


# =========================================================
# 6) 업로드/seed UI (항상 렌더 전에 실행 + 성공 시 rerun)
# =========================================================
with st.expander("템플릿 업로드/적재 (calendar.xlsx)", expanded=True):
    uploaded = st.file_uploader("calendar.xlsx 업로드", type=["xlsx"])
    if uploaded is not None:
        if st.button("Supabase에 템플릿 적재(seed)"):
            try:
                n = seed_from_excel_bytes(uploaded)
                st.success(f"shift_template upsert 완료: {n} rows")
                st.rerun()  # 🔥 seed 후 즉시 화면 갱신(다음 단계 렌더 보장)
            except Exception as e:
                st.error(f"seed 실패: {type(e).__name__} | {e}")
                st.stop()

# =========================================================
# 7) 템플릿 로드 → 없으면 중단 (드롭다운이 안 생기는 핵심 방지 로직)
# =========================================================
template_rows = load_template(TEMPLATE_KEY)
if not template_rows:
    st.warning(f"템플릿이 비어있습니다. (template_key={TEMPLATE_KEY}) 업로드 후 seed를 눌러주세요.")
    st.stop()

assign_map = load_assignments(TEMPLATE_KEY)

# 날짜 목록
days = sorted({r["day_of_month"] for r in template_rows})

# 템플릿 맵: (day, shift, period) -> group_code
tmpl_map = {(r["day_of_month"], r["shift"], r["period"]): r["group_code"] for r in template_rows}

# 그날 이미 등록된 대근자 set (조건 4)
subs_used_by_day: dict[int, set[str]] = {d: set() for d in days}
for (d, sh, p), a in assign_map.items():
    sub = a.get("substitute_name")
    if sub:
        subs_used_by_day.setdefault(d, set()).add(sub)


# =========================================================
# 8) 렌더 블록: (T1/T2) x (day/night)
#     행 3개: 근무자 / 결원 / 대근자
# =========================================================
def render_block(shift: str, period: str, block_title: str):
    st.subheader(block_title)

    # 헤더
    cols = st.columns([1] + [1]*len(days))
    cols[0].markdown("**구분 / 날짜**")
    for i, d in enumerate(days):
        cols[i+1].markdown(f"**{d}일**")

    # (1) 근무자: 그룹 조원 전체 표시 (조건 1)
    row1 = st.columns([1] + [1]*len(days))
    row1[0].markdown("**근무자**")
    for i, d in enumerate(days):
        g = tmpl_map.get((d, shift, period))
        if not g:
            row1[i+1].markdown("-")
            continue
        members = sorted(GROUPS.get(g, set()))
        row1[i+1].markdown("<br/>".join(members), unsafe_allow_html=True)

    # (2) 결원: 해당 그룹에서만 선택 (조건 2)
    row2 = st.columns([1] + [1]*len(days))
    row2[0].markdown("**결원**")
    for i, d in enumerate(days):
        g = tmpl_map.get((d, shift, period))
        key = (d, shift, period)
        a = assign_map.get(key, {})
        current_vac = a.get("vacancy_name")

        if not g:
            row2[i+1].markdown("-")
            continue

        options = [""] + sorted(GROUPS.get(g, set()))
        vac = row2[i+1].selectbox(
            label=f"vac_{shift}_{period}_{d}",
            options=options,
            index=options.index(current_vac) if current_vac in options else 0,
            label_visibility="collapsed"
        )

        vac_val = vac if vac != "" else None
        sub_val = a.get("substitute_name") if a else None

        if vac_val != current_vac:
            upsert_assignment(TEMPLATE_KEY, d, shift, period, vac_val, sub_val)
            assign_map[key] = {
                "template_key": TEMPLATE_KEY, "day_of_month": d, "shift": shift, "period": period,
                "vacancy_name": vac_val, "substitute_name": sub_val
            }
            st.rerun()

    # (3) 대근자: 룰 기반 + 그날 중복 제외 (조건 3, 4)
    row3 = st.columns([1] + [1]*len(days))
    row3[0].markdown("**대근자**")
    for i, d in enumerate(days):
        g = tmpl_map.get((d, shift, period))
        key = (d, shift, period)
        a = assign_map.get(key, {})
        current_sub = a.get("substitute_name")
        current_vac = a.get("vacancy_name")

        if not g:
            row3[i+1].markdown("-")
            continue

        cand_groups = substitute_candidate_groups(g, period)
        candidates = members_of_groups(cand_groups)

        used = set(subs_used_by_day.get(d, set()))
        # 현재 셀의 값은 유지(편집 UX)
        if current_sub:
            used.discard(current_sub)

        filtered = [x for x in candidates if x not in used]

        options = [""] + filtered
        sub = row3[i+1].selectbox(
            label=f"sub_{shift}_{period}_{d}",
            options=options,
            index=options.index(current_sub) if current_sub in options else 0,
            label_visibility="collapsed"
        )

        sub_val = sub if sub != "" else None
        vac_val = current_vac if current_vac else None

        if sub_val != current_sub:
            # 메모리상 중복셋 갱신 후 저장
            if current_sub:
                subs_used_by_day[d].discard(current_sub)
            if sub_val:
                subs_used_by_day[d].add(sub_val)

            upsert_assignment(TEMPLATE_KEY, d, shift, period, vac_val, sub_val)
            assign_map[key] = {
                "template_key": TEMPLATE_KEY, "day_of_month": d, "shift": shift, "period": period,
                "vacancy_name": vac_val, "substitute_name": sub_val
            }
            st.rerun()


# =========================================================
# 9) 화면 출력
# =========================================================
render_block("T1", "day",   "T1 · 주간")
render_block("T2", "day",   "T2 · 주간")
render_block("T1", "night", "T1 · 야간")
render_block("T2", "night", "T2 · 야간")
