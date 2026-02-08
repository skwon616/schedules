# app.py
import re
from datetime import date
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# =========================================================
# UI Style
# =========================================================
st.set_page_config(page_title="교대 결원/대근 입력", layout="wide")

st.markdown(
    """
    <style>
      .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
      .stSelectbox label {display:none;}
      .card {
        border: 1px solid rgba(0,0,0,0.10);
        border-radius: 14px;
        padding: 14px 14px 10px 14px;
        background: rgba(0,0,0,0.02);
      }
      .muted {opacity: 0.7; font-size: 0.9rem;}
      .title {font-size: 1.05rem; font-weight: 700; margin-bottom: 6px;}
      .pill {
        display:inline-block; padding: 3px 10px; border-radius: 999px;
        border: 1px solid rgba(0,0,0,0.15); font-size: 0.85rem; margin-right: 6px;
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
# 1) 그룹 정의
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
# 2) 정규화 함수 (seed 안정화)
# =========================================================
def norm_shift(x: str) -> str:
    s = (x or "").strip()
    if s in ("Day", "D", "주간", "주"):
        return "Day"
    if s in ("Night", "N", "야간", "야"):
        return "Night"
    if s.lower().startswith("day"):
        return "Day"
    if s.lower().startswith("night"):
        return "Night"
    return s

def norm_cat(x: str) -> str:
    s = (x or "").strip()
    if "근무" in s:
        return "근무자"
    if "결원" in s or "휴가" in s:
        return "결원"
    if "대근" in s or "대체" in s:
        return "대근자"
    return s

def is_template_text(s: str) -> bool:
    return isinstance(s, str) and s.strip().startswith("Select from")

def value_or_blank(s: str) -> str:
    # 화면/프리뷰에서 템플릿은 공란 취급
    if is_template_text(s):
        return ""
    return (s or "").strip()

def category_order_key(cat: str) -> int:
    cat = (cat or "").strip()
    if cat == "근무자": return 0
    if cat == "결원": return 1
    if cat == "대근자": return 2
    return 9

# =========================================================
# 3) DB helpers
# =========================================================
def fetch_db() -> pd.DataFrame:
    res = sb.table(TABLE).select(
        "team,shift_type,category,row_no,work_date,cell_value,template_value"
    ).execute()
    rows = res.data or []
    if not rows:
        return pd.DataFrame(columns=[
            "team","shift_type","category","row_no","work_date","cell_value","template_value"
        ])
    df = pd.DataFrame(rows)
    df["row_no"] = df["row_no"].astype(int)
    df["work_date"] = pd.to_datetime(df["work_date"]).dt.date
    df["team"] = df["team"].fillna("").astype(str).str.strip()
    df["shift_type"] = df["shift_type"].fillna("").astype(str).apply(norm_shift)
    df["category"] = df["category"].fillna("").astype(str).apply(norm_cat)
    df["cell_value"] = df["cell_value"].fillna("").astype(str)
    df["template_value"] = df["template_value"].fillna("").astype(str)
    return df

def db_upsert(team: str, shift_type: str, category: str, row_no: int, work_date: date,
             cell_value: str, template_value: str = None):
    payload = {
        "team": team,
        "shift_type": shift_type,
        "category": category,
        "row_no": int(row_no),
        "work_date": work_date.isoformat(),
        "cell_value": str(cell_value)
    }
    # 템플릿은 한 번이라도 알게 되면 유지(빈 값으로 덮지 않음)
    if template_value is not None and str(template_value).strip():
        payload["template_value"] = str(template_value)

    sb.table(TABLE).upsert(
        payload,
        on_conflict="team,shift_type,category,row_no,work_date"
    ).execute()

# =========================================================
# 4) Seed (template_value에 템플릿 보관, cell_value는 공란)
# =========================================================
def seed_from_excel(excel_df: pd.DataFrame):
    """
    엑셀: [0]team [1]shift [2]category [3:]dates...
    병합셀 대비: team/shift/category ffill
    템플릿(Select from...)은 template_value에 저장하고, cell_value는 공란 처리
    """
    if excel_df.shape[1] < 4:
        raise ValueError("엑셀은 최소 4개 컬럼(team, shift, category, 날짜...)이 필요합니다.")

    c_team  = excel_df.columns[0]
    c_shift = excel_df.columns[1]
    c_cat   = excel_df.columns[2]

    teams  = excel_df[c_team].ffill().astype(str)
    shifts = excel_df[c_shift].ffill().astype(str)
    cats   = excel_df[c_cat].ffill().astype(str)

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
        team = str(teams.iat[i]).strip().split()[0]  # "T1 ..." 같은 경우 대비
        shift_type = norm_shift(shifts.iat[i])
        category = norm_cat(cats.iat[i])

        for dc in date_cols:
            raw = excel_df.at[i, dc]
            raw = "" if pd.isna(raw) else str(raw)

            tv = raw if is_template_text(raw) else ""
            cv = "" if is_template_text(raw) else raw  # 템플릿은 화면에 안 보이도록 공란

            payload.append({
                "team": team,
                "shift_type": shift_type,
                "category": category,
                "row_no": int(i),
                "work_date": dc.date().isoformat(),
                "cell_value": cv,
                "template_value": tv
            })

    # dedup
    dedup = {}
    for r in payload:
        k = (r["team"], r["shift_type"], r["category"], r["row_no"], r["work_date"])
        dedup[k] = r
    payload = list(dedup.values())

    # batch upsert
    BATCH = 800
    for k in range(0, len(payload), BATCH):
        sb.table(TABLE).upsert(
            payload[k:k+BATCH],
            on_conflict="team,shift_type,category,row_no,work_date"
        ).execute()

# =========================================================
# 5) Load or Seed
# =========================================================
db_df = fetch_db()
if db_df.empty:
    st.warning("Supabase DB가 비어있습니다. 최초 1회 schedule.xlsx 업로드로 초기화하세요.")
    up = st.file_uploader("초기 적재용 엑셀 업로드(.xlsx)", type=["xlsx"])
    if up:
        try:
            excel_df = pd.read_excel(up)
            seed_from_excel(excel_df)
            st.success("✅ 초기 적재 완료. 새로고침하면 편집 화면이 뜹니다.")
        except Exception as e:
            st.error(f"초기 적재 실패: {e}")
    st.stop()

# =========================================================
# 6) Top selectors
# =========================================================
all_dates = sorted(db_df["work_date"].dropna().unique().tolist())
all_shifts = sorted(db_df["shift_type"].dropna().unique().tolist())
all_teams = sorted(db_df["team"].dropna().unique().tolist())

top1, top2, top3 = st.columns([2, 1, 1])
with top1:
    d_label = st.selectbox("날짜", [d.strftime("%Y-%m-%d") for d in all_dates], index=0)
    target_date = date.fromisoformat(d_label)
with top2:
    target_shift = st.selectbox("근무형태", all_shifts, index=0)
with top3:
    st.markdown('<span class="muted">DB 자동 저장</span>', unsafe_allow_html=True)

st.markdown("<div class='rowgap'></div>", unsafe_allow_html=True)

# =========================================================
# 7) Row getters
# =========================================================
def get_rows(team: str, shift_type: str, category: str, wd: date) -> pd.DataFrame:
    return db_df[
        (db_df["team"] == team) &
        (db_df["shift_type"] == shift_type) &
        (db_df["category"] == category) &
        (db_df["work_date"] == wd)
    ].sort_values("row_no")

def get_workers(team: str, shift_type: str, wd: date):
    sub = get_rows(team, shift_type, "근무자", wd)
    names = [x for x in sub["cell_value"].tolist() if (x or "").strip()]
    return names[:4]

def compute_repl_candidates(template_text: str, workers: list, leave_name: str):
    cands, _ = parse_select_from(template_text)
    ex = set([x for x in workers if x])
    if leave_name:
        ex.add(leave_name)
    return [c for c in cands if c not in ex]

# =========================================================
# 8) Cards UI
# =========================================================
cols = st.columns(2)

# 현재 날짜/shift에서 이미 입력된 대근자 (중복 방지)
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

        leave_rows = get_rows(team, target_shift, "결원", target_date)
        repl_rows = get_rows(team, target_shift, "대근자", target_date)

        st.markdown(f"<div class='card'><div class='title'>{team} | {target_shift}</div>", unsafe_allow_html=True)

        # 근무자 표시
        if workers:
            pills = " ".join([f"<span class='pill'>{w}</span>" for w in workers])
            st.markdown(pills, unsafe_allow_html=True)
        else:
            st.markdown("<span class='muted'>근무자 데이터가 없습니다.</span>", unsafe_allow_html=True)

        st.markdown("<div class='rowgap'></div>", unsafe_allow_html=True)

        # 결원 (첫 행만 사용)
        leave_choice = ""
        leave_rowno = None
        if leave_rows.empty:
            st.warning("결원 행이 없습니다(엑셀 seed 확인).")
        else:
            leave_rowno = int(leave_rows.iloc[0]["row_no"])
            leave_saved = value_or_blank(leave_rows.iloc[0]["cell_value"])  # 템플릿은 공란
            leave_options = [""] + workers
            if leave_saved and leave_saved not in leave_options:
                leave_options = ["", leave_saved] + workers

            leave_key = f"leave__{team}__{target_shift}__{target_date.isoformat()}"
            leave_choice = st.selectbox("결원", leave_options,
                                        index=leave_options.index(leave_saved) if leave_saved in leave_options else 0,
                                        key=leave_key)

        # 대근자: row 개수만큼 드롭다운 생성
        repl_inputs = []  # (row_no, chosen, template_text)
        if repl_rows.empty:
            st.warning("대근자 행이 없습니다(엑셀 seed 확인).")
        else:
            rr = repl_rows.reset_index(drop=True)
            for j in range(len(rr)):
                r = rr.iloc[j]
                rn = int(r["row_no"])

                # ✅ 후보 생성은 template_value 우선
                template_text = (r["template_value"] or "").strip()
                saved_val = value_or_blank(r["cell_value"])

                if not template_text:
                    # 템플릿이 없으면 후보 생성 불가
                    options = [""] + ([saved_val] if saved_val else [])
                else:
                    candidates = compute_repl_candidates(template_text, workers, leave_choice)
                    options = [""] + candidates
                    if saved_val and saved_val not in options:
                        options = ["", saved_val] + candidates

                repl_key = f"repl__{team}__{target_shift}__{target_date.isoformat()}__{rn}"
                choice = st.selectbox(f"대근자 {j+1}", options,
                                      index=options.index(saved_val) if saved_val in options else 0,
                                      key=repl_key)

                repl_inputs.append((rn, choice, template_text))

        save = st.button(f"{team} 저장", type="primary", use_container_width=True, key=f"save__{team}")
        if save:
            # 중복 체크 (대근자 선택값)
            chosen_vals = [c for (_, c, _) in repl_inputs if c]
            dup = None
            for v in chosen_vals:
                if v in already_used:
                    dup = v
                    break
            if dup:
                st.error(f"{dup}는 이미 입력되었습니다")
            else:
                # 결원 저장: 선택 없으면 공란(템플릿은 건드리지 않음)
                if leave_rowno is not None:
                    db_upsert(team, target_shift, "결원", leave_rowno, target_date, leave_choice or "")

                # 대근자 저장: 선택 없으면 공란, 템플릿 유지
                for rn, choice, template_text in repl_inputs:
                    db_upsert(team, target_shift, "대근자", rn, target_date,
                              cell_value=choice or "",
                              template_value=template_text)

                st.success("✅ 저장 완료 (DB 반영)")
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)

# =========================================================
# 9) Preview (정렬 + row_no 숨김 + 템플릿 숨김)
# =========================================================
with st.expander("미리보기(디버그)", expanded=False):
    view = db_df[
        (db_df["work_date"] == target_date) &
        (db_df["shift_type"] == target_shift)
    ].copy()

    view["cat_ord"] = view["category"].apply(category_order_key)
    view = view.sort_values(["team", "cat_ord", "row_no"]).drop(columns=["cat_ord"])

    # row_no 숨김
    view = view.drop(columns=["row_no"], errors="ignore")

    # 템플릿 숨김(공란 처리)
    view["cell_value"] = view["cell_value"].apply(value_or_blank)

    # template_value도 디버그에선 보고 싶으면 주석 해제
    # st.dataframe(view, use_container_width=True, height=350)

    # 디버그에는 template_value까지 함께 보여주자(문제 원인 파악용)
    st.dataframe(view, use_container_width=True, height=350)
