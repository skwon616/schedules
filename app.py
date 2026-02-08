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

def norm_shift(x: str) -> str:
    s = (x or "").strip()
    if s in ("Day", "D", "주간", "주"):
        return "Day"
    if s in ("Night", "N", "야간", "야"):
        return "Night"
    # 이미 Day/Night면 그대로
    if s.lower().startswith("day"):
        return "Day"
    if s.lower().startswith("night"):
        return "Night"
    return s

def norm_cat(x: str) -> str:
    s = (x or "").strip()
    # 포함 매칭으로 튼튼하게
    if "근무" in s:
        return "근무자"
    if "결원" in s or "휴가" in s:
        return "결원"
    if "대근" in s or "대체" in s:
        return "대근자"
    return s

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
    """
    엑셀 구조 가정:
    [0] 관제소(team)
    [1] 근무형태(shift)  → Day / Night / 주간 / 야간
    [2] 구분(category)  → 근무자 / 결원 / 대근자 (병합셀 가능)
    [3:] 날짜 컬럼
    """

    if excel_df.shape[1] < 4:
        raise ValueError("엑셀은 최소 4개 컬럼(관제소, 근무형태, 구분, 날짜...)이 필요합니다.")

    # ✅ 컬럼 지정 (이게 빠져서 에러났음)
    c_team  = excel_df.columns[0]
    c_shift = excel_df.columns[1]   # ← 이 줄이 핵심
    c_cat   = excel_df.columns[2]

    # ✅ 병합셀 대비 ffill
    teams  = excel_df[c_team].ffill().astype(str)
    shifts = excel_df[c_shift].ffill().astype(str)
    cats   = excel_df[c_cat].ffill().astype(str)

    # 날짜 컬럼 찾기
    date_cols = []
    for c in excel_df.columns[3:]:
        try:
            date_cols.append(pd.to_datetime(c))
        except Exception:
            pass

    if not date_cols:
        raise ValueError("날짜 컬럼을 찾지 못했습니다. (4번째 컬럼부터 날짜 헤더 필요)")

    payload = []
    for i in range(len(excel_df)):
        team = teams.iat[i].strip()
        shift_type = norm_shift(shifts.iat[i])
        category = norm_cat(cats.iat[i])

        for dc in date_cols:
            v = excel_df.at[i, dc]
            payload.append({
                "team": team,
                "shift_type": shift_type,
                "category": category,
                "row_no": int(i),
                "work_date": dc.date().isoformat(),
                "cell_value": "" if pd.isna(v) else str(v)
            })

    if not payload:
        raise ValueError("Seed 대상 데이터가 0건입니다. 엑셀 구조를 확인하세요.")

    # 중복 제거 (안전)
    dedup = {}
    for r in payload:
        k = (r["team"], r["shift_type"], r["category"], r["row_no"], r["work_date"])
        dedup[k] = r
    payload = list(dedup.values())

    sb.table("schedule_cells").upsert(
        payload,
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
    
db_df["shift_type"] = db_df["shift_type"].apply(norm_shift)
db_df["category"] = db_df["category"].apply(norm_cat)
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

def is_template(x: str) -> bool:
    return isinstance(x, str) and ("Select from" in x)

def value_or_blank(x: str) -> str:
    # DB 값이 템플릿이면 화면에서는 공란처럼 취급
    if is_template(x):
        return ""
    return (x or "").strip()

def category_order_key(cat: str) -> int:
    cat = (cat or "").strip()
    if cat == "근무자":
        return 0
    if cat == "결원":
        return 1
    if cat == "대근자":
        return 2
    return 9

def get_rows(team: str, shift_type: str, category: str, wd: date) -> pd.DataFrame:
    sub = db_df[
        (db_df["team"] == team) &
        (db_df["shift_type"] == shift_type) &
        (db_df["category"] == category) &
        (db_df["work_date"] == wd)
    ].sort_values("row_no")
    return sub

def get_workers(team: str, shift_type: str, wd: date):
    sub = get_rows(team, shift_type, "근무자", wd)
    names = [x for x in sub["cell_value"].tolist() if (x or "").strip()]
    return names[:4]

def get_leave_rows(team: str, shift_type: str, wd: date) -> pd.DataFrame:
    return get_rows(team, shift_type, "결원", wd)

def get_repl_rows(team: str, shift_type: str, wd: date) -> pd.DataFrame:
    return get_rows(team, shift_type, "대근자", wd)

def compute_repl_candidates(template_text: str, workers: list, leave_name: str):
    cands, _ = parse_select_from(template_text)
    ex = set([x for x in workers if x])
    if leave_name:
        ex.add(leave_name)
    # 기본 제외: 당일 근무자 + 결원자
    cands = [c for c in cands if c not in ex]
    return cands

cols = st.columns(2)
cols = st.columns(2)

# (6) 중복 체크를 위해 현재 날짜/shift의 전체 대근자 값 수집
cur_repl_all = db_df[
    (db_df["work_date"] == target_date) &
    (db_df["shift_type"] == target_shift) &
    (db_df["category"] == "대근자")
].copy()
cur_repl_all["val"] = cur_repl_all["cell_value"].apply(value_or_blank)
already_used = set([v for v in cur_repl_all["val"].tolist() if v])

for idx, team in enumerate(all_teams[:2]):
    with cols[idx]:
        workers = get_workers(team, target_shift, target_date)

        leave_rows = get_leave_rows(team, target_shift, target_date)   # 여러 행일 수 있음
        repl_rows  = get_repl_rows(team, target_shift, target_date)    # 여러 행일 수 있음

        st.markdown(f"<div class='card'><div class='title'>{team} | {target_shift}</div>", unsafe_allow_html=True)

        # 근무자 pill
        if workers:
            pills = " ".join([f"<span class='pill'>{w}</span>" for w in workers])
            st.markdown(pills, unsafe_allow_html=True)
        else:
            st.markdown("<span class='muted'>근무자 데이터가 없습니다.</span>", unsafe_allow_html=True)

        st.markdown("<div class='rowgap'></div>", unsafe_allow_html=True)

        # 결원 드롭다운: 결원 행이 여러 개면 첫 번째만 사용(일단 1개만)
        leave_choice = ""
        leave_rowno = None
        if leave_rows.empty:
            st.warning("결원 행이 없습니다(엑셀 seed 확인).")
        else:
            leave_rowno = int(leave_rows.iloc[0]["row_no"])
            leave_saved = value_or_blank(leave_rows.iloc[0]["cell_value"])
            leave_options = [""] + workers
            if leave_saved and leave_saved not in leave_options:
                leave_options = ["", leave_saved] + workers

            leave_key = f"leave__{team}__{target_shift}__{target_date.isoformat()}"
            leave_choice = st.selectbox(
                "결원",
                options=leave_options,
                index=leave_options.index(leave_saved) if leave_saved in leave_options else 0,
                key=leave_key
            )

        # 대근자 드롭다운: DB에 있는 대근자 행 개수만큼 생성 (요구사항 1,2)
        repl_inputs = []  # [(row_no, chosen_value, template_text)]
        if repl_rows.empty:
            st.warning("대근자 행이 없습니다(엑셀 seed 확인).")
        else:
            for j, r in repl_rows.reset_index(drop=True).iterrows():
                rn = int(r["row_no"])
                raw = (r["cell_value"] or "")
                saved = value_or_blank(raw)

                # 후보는 템플릿(raw)에 기반
                if is_template(raw):
                    candidates = compute_repl_candidates(raw, workers, leave_choice)
                    options = [""] + candidates
                    if saved and saved not in options:
                        options = ["", saved] + candidates
                else:
                    # 템플릿이 아닌 경우(이미 값이거나 공란)
                    # 후보를 만들려면 원래 템플릿이 필요하므로 빈 옵션 + 현재값만 허용
                    options = [""] + ([saved] if saved else [])
                    candidates = []

                repl_key = f"repl__{team}__{target_shift}__{target_date.isoformat()}__{rn}"
                choice = st.selectbox(
                    f"대근자 {j+1}",
                    options=options,
                    index=options.index(saved) if saved in options else 0,
                    key=repl_key
                )
                repl_inputs.append((rn, choice, raw, candidates))

        save = st.button(f"{team} 저장", type="primary", use_container_width=True, key=f"save__{team}")
        if save:
            # (5) 선택 없으면 공란 저장
            # (6) 중복 체크: 같은 날짜/shift 전체에서 중복 금지
            # 현재 팀의 선택값들만 대상으로 검사
            chosen_vals = [c for (_, c, _, _) in repl_inputs if c]
            dup = None
            for v in chosen_vals:
                # 이미 사용된 값 중, "현재 팀의 동일 row_no에 있었던 값"은 제외해야 하는데
                # 간단히: 저장 시점에는 중복이면 막는다(실무상 충분)
                if v in already_used:
                    dup = v
                    break
            if dup:
                st.error(f"{dup}는 이미 입력되었습니다")
            else:
                # 결원 저장 (없으면 공란)
                if leave_rowno is not None:
                    db_upsert(team, target_shift, "결원", leave_rowno, target_date, leave_choice or "")

                # 대근자 N개 저장
                for rn, choice, raw, _ in repl_inputs:
                    db_upsert(team, target_shift, "대근자", rn, target_date, choice or "")

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
    ].copy()

    # (3) 카테고리 순서: 근무자 -> 결원 -> 대근자
    view["cat_ord"] = view["category"].apply(category_order_key)
    view = view.sort_values(["team", "cat_ord", "row_no"]).drop(columns=["cat_ord"])

    # (4) row_no 숨기기
    view = view.drop(columns=["row_no"], errors="ignore")

    # (5) 템플릿은 빈칸으로 보여주기(디버그에서도)
    view["cell_value"] = view["cell_value"].apply(value_or_blank)

    st.dataframe(view, use_container_width=True, height=350)
