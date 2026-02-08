# app.py
import re
from datetime import date
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# =========================================================
# Page + Style (오른쪽처럼 "깔끔한 카드/그리드" 느낌)
# =========================================================
st.set_page_config(page_title="교대 결원/대근 입력", layout="wide")

st.markdown(
    """
    <style>
      .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
      .stSelectbox label {display:none;} /* 라벨 숨겨서 '드롭다운만' 보이게 */
      .card {
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 14px;
        padding: 14px 14px 10px 14px;
        background: rgba(255,255,255,0.03);
      }
      .muted {opacity: 0.7; font-size: 0.9rem;}
      .title {font-size: 1.05rem; font-weight: 700; margin-bottom: 6px;}
      .pill {
        display:inline-block; padding: 3px 10px; border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.15); font-size: 0.85rem; margin-right: 6px;
      }
      .rowgap {margin-top: 10px;}
    </style>
    """,
    unsafe_allow_html=True
)

st.title("교대 결원/대근 입력")

# =========================================================
# 0) Supabase
# =========================================================
SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY", "")
if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Streamlit secrets에 SUPABASE_URL / SUPABASE_ANON_KEY를 설정하세요.")
    st.stop()

sb: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
TABLE = "schedule_cells"

# =========================================================
# 1) 그룹 정의 (사용자 제공)
# =========================================================
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
GROUP_KEY_PATTERN = re.compile(r"\b([A-D][12])\b")

def parse_select_from(text: str):
    """'Select from A1' / 'Select from B1 or B2' 형태에서 후보 추출"""
    if not isinstance(text, str):
        return [], []
    if "Select from" not in text:
        return [], []
    keys = GROUP_KEY_PATTERN.findall(text)
    keys = list(dict.fromkeys(keys))
    cand = set()
    for k in keys:
        cand |= GROUPS.get(k, set())
    return sorted(cand), keys

# =========================================================
# 2) DB helpers
# =========================================================
def fetch_db() -> pd.DataFrame:
    res = sb.table(TABLE).select("team,shift_type,category,row_no,work_date,cell_value").execute()
    rows = res.data or []
    if not rows:
        return pd.DataFrame(columns=["team","shift_type","category","row_no","work_date","cell_value"])
    df = pd.DataFrame(rows)
    df["row_no"] = df["row_no"].astype(int)
    df["work_date"] = pd.to_datetime(df["work_date"]).dt.date
    df["cell_value"] = df["cell_value"].fillna("").astype(str)
    df["team"] = df["team"].fillna("").astype(str)
    df["shift_type"] = df["shift_type"].fillna("").astype(str)
    df["category"] = df["category"].fillna("").astype(str)
    return df

def db_upsert(team: str, shift_type: str, category: str, row_no: int, work_date: date, value: str):
    sb.table(TABLE).upsert({
        "team": team,
        "shift_type": shift_type,
        "category": category,
        "row_no": int(row_no),
        "work_date": work_date.isoformat(),
        "cell_value": str(value)
    }, on_conflict="team,shift_type,category,row_no,work_date").execute()

# =========================================================
# 3) 최초 1회 seed (schedule.xlsx 구조 기준)
# =========================================================
def seed_from_excel(excel_df: pd.DataFrame):
    # schedule.xlsx 컬럼: 관제소, 근무형태, 구분, 날짜들...
    if excel_df.shape[1] < 4:
        raise ValueError("엑셀은 최소 4개 컬럼(관제소,근무형태,구분,날짜...)이 필요합니다.")

    c_team = excel_df.columns[0]      # 관제소
    c_shift = excel_df.columns[1]     # 근무형태
    c_cat = excel_df.columns[2]       # 구분

    teams = excel_df[c_team].ffill().astype(str)
    shifts = excel_df[c_shift].astype(str)
    cats = excel_df[c_cat].astype(str)

    # 날짜 컬럼 추출 (4번째 컬럼부터)
    date_cols = []
    for c in excel_df.columns[3:]:
        if isinstance(c, pd.Timestamp):
            date_cols.append(c)
        else:
            try:
                _ = pd.to_datetime(c)
                date_cols.append(c)
            except:
                pass
    if not date_cols:
        raise ValueError("날짜 컬럼을 찾지 못했습니다. (4번째 컬럼부터 헤더가 날짜여야 함)")

    payload = []
    for i in range(len(excel_df)):
        team = str(teams.iat[i]).strip()
        shift_type = str(shifts.iat[i]).strip()     # 주간/야간
        category = str(cats.iat[i]).strip()         # 근무자/결원/대근자

        for dc in date_cols:
            wd = pd.to_datetime(dc).date() if not isinstance(dc, pd.Timestamp) else dc.date()
            v = excel_df.at[i, dc]
            v = "" if pd.isna(v) else str(v)

            payload.append({
                "team": team,
                "shift_type": shift_type,
                "category": category,
                "row_no": int(i),     # 엑셀 행 index를 row_no로
                "work_date": wd.isoformat(),
                "cell_value": v
            })

    # 같은 키 중복 제거(안전)
    dedup = {}
    for r in payload:
        k = (r["team"], r["shift_type"], r["category"], r["row_no"], r["work_date"])
        dedup[k] = r
    payload = list(dedup.values())

    # 배치 upsert
    BATCH = 800
    for k in range(0, len(payload), BATCH):
        sb.table(TABLE).upsert(
            payload[k:k+BATCH],
            on_conflict="team,shift_type,category,row_no,work_date"
        ).execute()

# =========================================================
# 4) Load DB or Seed
# =========================================================
db_df = fetch_db()
if db_df.empty:
    st.warning("Supabase DB가 비어있습니다. 최초 1회 schedule.xlsx 업로드로 초기화하세요.")
    up = st.file_uploader("초기 적재용 엑셀 업로드(.xlsx)", type=["xlsx"])
    if up:
        try:
            excel_df = pd.read_excel(up)
            seed_from_excel(excel_df)
            st.success("✅ 초기 적재 완료. 페이지를 새로고침하면 편집 화면이 뜹니다.")
        except Exception as e:
            st.error(f"초기 적재 실패: {e}")
    st.stop()

# =========================================================
# 5) 화면용 데이터 구성
# =========================================================
# 선택 가능한 날짜/근무형태
all_dates = sorted(db_df["work_date"].dropna().unique().tolist())
all_shifts = sorted(db_df["shift_type"].dropna().unique().tolist())  # 주간/야간
all_teams = sorted(db_df["team"].dropna().unique().tolist())         # T1/T2

# 상단: 날짜, 근무형태
top1, top2, top3 = st.columns([2, 1, 1])
with top1:
    d_label = st.selectbox("날짜", [d.strftime("%Y-%m-%d") for d in all_dates], index=0)
    target_date = date.fromisoformat(d_label)
with top2:
    target_shift = st.selectbox("근무형태", all_shifts, index=0)
with top3:
    # (필요 없으면 제거 가능) 관제소 필터가 아니라 전체(T1,T2) 보여줄 거라 그냥 정보용
    st.markdown('<span class="muted">DB 자동 저장</span>', unsafe_allow_html=True)

st.markdown("<div class='rowgap'></div>", unsafe_allow_html=True)

# =========================================================
# 6) 핵심 UI: 팀별 카드(T1/T2) + 결원/대근 드롭다운
# =========================================================
def get_workers(team: str, shift_type: str, wd: date):
    # 근무자 4명: category == '근무자'인 행들
    sub = db_df[
        (db_df["team"] == team) &
        (db_df["shift_type"] == shift_type) &
        (db_df["category"] == "근무자") &
        (db_df["work_date"] == wd)
    ].sort_values("row_no")
    names = [x for x in sub["cell_value"].tolist() if x and x.strip()]
    return names[:4]

def get_template(team: str, shift_type: str, category: str, wd: date):
    # 결원/대근자 템플릿: 첫 행(row_no 최소) 셀 값을 사용
    sub = db_df[
        (db_df["team"] == team) &
        (db_df["shift_type"] == shift_type) &
        (db_df["category"] == category) &
        (db_df["work_date"] == wd)
    ].sort_values("row_no")
    if sub.empty:
        return "", None
    return sub.iloc[0]["cell_value"], int(sub.iloc[0]["row_no"])

def get_saved_choice(team: str, shift_type: str, category: str, wd: date):
    # 템플릿이 'Select from ...'였다가 이미 이름으로 바뀌었을 수 있음
    v, rn = get_template(team, shift_type, category, wd)
    # 만약 v가 템플릿이면 "아직 미선택"으로 간주
    if isinstance(v, str) and "Select from" in v:
        return "", rn, v
    # 템플릿이 아닌 실제 이름이면 그걸 선택값으로 보여줌
    return v, rn, v

def compute_repl_candidates(template_text: str, workers: list, leave_name: str):
    cands, _ = parse_select_from(template_text)
    # 기본 제외 규칙: 당일 근무자/휴가자 제외
    ex = set([x for x in workers if x])
    if leave_name:
        ex.add(leave_name)
    cands = [c for c in cands if c not in ex]
    return cands

cols = st.columns(2)
for idx, team in enumerate(all_teams[:2]):  # T1, T2 기준 (더 많으면 반복 확장)
    with cols[idx]:
        workers = get_workers(team, target_shift, target_date)
        leave_saved, leave_rowno, leave_raw = get_saved_choice(team, target_shift, "결원", target_date)
        repl_saved, repl_rowno, repl_raw = get_saved_choice(team, target_shift, "대근자", target_date)

        # 결원 드롭다운 옵션은 "근무자 4명"에서 선택하도록
        leave_options = [""] + workers
        if leave_saved and leave_saved not in leave_options:
            leave_options = ["", leave_saved] + workers

        # 대근자 후보는 템플릿 파싱으로
        repl_template = repl_raw  # (대근자 category의 첫 행 값)
        repl_candidates = compute_repl_candidates(repl_template, workers, leave_saved)
        repl_options = [""] + repl_candidates
        if repl_saved and repl_saved not in repl_options:
            repl_options = ["", repl_saved] + repl_candidates

        st.markdown(f"<div class='card'><div class='title'>{team} | {target_shift}</div>", unsafe_allow_html=True)

        # 근무자 표시 (pill)
        if workers:
            pills = " ".join([f"<span class='pill'>{w}</span>" for w in workers])
            st.markdown(pills, unsafe_allow_html=True)
        else:
            st.markdown("<span class='muted'>근무자 데이터가 없습니다.</span>", unsafe_allow_html=True)

        st.markdown("<div class='rowgap'></div>", unsafe_allow_html=True)

        # 드롭다운 2개 (라벨 숨김 처리됨)
        leave_key = f"leave__{team}__{target_shift}__{target_date.isoformat()}"
        repl_key  = f"repl__{team}__{target_shift}__{target_date.isoformat()}"

        leave_choice = st.selectbox(
            "결원",
            options=leave_options,
            index=leave_options.index(leave_saved) if leave_saved in leave_options else 0,
            key=leave_key
        )

        # leave 선택이 바뀌면 대근 후보도 다시 계산
        repl_candidates = compute_repl_candidates(repl_template, workers, leave_choice)
        repl_options = [""] + repl_candidates
        if repl_saved and repl_saved not in repl_options:
            repl_options = ["", repl_saved] + repl_candidates

        repl_choice = st.selectbox(
            "대근자",
            options=repl_options,
            index=repl_options.index(repl_saved) if repl_saved in repl_options else 0,
            key=repl_key
        )

        save = st.button(f"{team} 저장", type="primary", use_container_width=True, key=f"save__{team}")
        if save:
            # 결원/대근자 category의 "첫 행(row_no 최소)"에 값 저장
            if leave_rowno is None or repl_rowno is None:
                st.error("DB에 결원/대근자 템플릿 행이 없습니다. seed 데이터를 확인하세요.")
            else:
                # 선택값이 비었으면 템플릿 원문으로 되돌리고 싶다면 아래 로직 유지
                # (비우면 "Select from ..."로 복구)
                leave_to_save = leave_choice if leave_choice else leave_raw
                repl_to_save = repl_choice if repl_choice else repl_raw

                db_upsert(team, target_shift, "결원", leave_rowno, target_date, leave_to_save)
                db_upsert(team, target_shift, "대근자", repl_rowno, target_date, repl_to_save)

                st.success("✅ 저장 완료 (DB 반영)")
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

# =========================================================
# 7) (선택) 하단 미리보기: 디버그용
# =========================================================
with st.expander("미리보기(디버그)", expanded=False):
    view = db_df[
        (db_df["work_date"] == target_date) &
        (db_df["shift_type"] == target_shift)
    ].sort_values(["team","category","row_no"])
    st.dataframe(view, use_container_width=True, height=350)
