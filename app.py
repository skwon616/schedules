# app.py
import re
import pandas as pd
import streamlit as st
from datetime import date
from supabase import create_client, Client

st.set_page_config(page_title="교대 대체자 입력 (Supabase 자동 저장)", layout="wide")
st.title("교대 근무 대체자 입력 (Supabase 자동 저장)")

# =========================
# 0) Supabase 연결
# =========================
SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY", "")
if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Streamlit secrets에 SUPABASE_URL / SUPABASE_ANON_KEY를 설정하세요.")
    st.stop()

sb: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# =========================
# 1) 그룹 정의 (사용자 제공)
# =========================
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
        if k in GROUPS:
            cand |= GROUPS[k]
    return sorted(cand), keys

# =========================
# 2) DB <-> DataFrame 변환
# =========================
def fetch_db():
    # DB에서 전부 가져오기 (규모 커지면 날짜 범위 필터 권장)
    res = sb.table("schedule_cells").select("team,shift_type,work_date,cell_value").execute()
    rows = res.data or []
    if not rows:
        return pd.DataFrame(columns=["team", "shift_type", "work_date", "cell_value"])
    df = pd.DataFrame(rows)
    df["work_date"] = pd.to_datetime(df["work_date"]).dt.date
    return df

def seed_from_excel(excel_df: pd.DataFrame):
    """
    업로드한 엑셀(df) 구조 가정:
    - 0번째 컬럼: team
    - 1번째 컬럼: shift_type (Day/Night/Off/Replace 등)
    - 2번째~ : 날짜 컬럼
    각 셀 값을 schedule_cells로 upsert 적재
    """
    if excel_df.shape[1] < 3:
        raise ValueError("엑셀은 최소 3개 컬럼(team, shift_type, 날짜...)이 필요합니다.")

    team_col = excel_df.columns[0]
    shift_col = excel_df.columns[1]

    # 병합셀 대응: team ffill
    teams = excel_df[team_col].ffill().astype(str)
    shifts = excel_df[shift_col].astype(str)

    date_cols = []
    for c in excel_df.columns[2:]:
        if isinstance(c, pd.Timestamp):
            date_cols.append(c)
        else:
            # 문자열 날짜도 허용
            try:
                _ = pd.to_datetime(c)
                date_cols.append(c)
            except:
                pass

    if not date_cols:
        raise ValueError("날짜 컬럼(3번째 컬럼부터)을 찾지 못했습니다.")

    payload = []
    for i in range(len(excel_df)):
        team = str(teams.iat[i]).strip()
        shift_type = str(shifts.iat[i]).strip()
        if shift_type not in ("Day", "Night"):  # 필요하면 Off/Replace 포함 가능
            continue
        for dc in date_cols:
            # 날짜 키
            work_date = pd.to_datetime(dc).date() if not isinstance(dc, pd.Timestamp) else dc.date()
            v = excel_df.at[i, dc]
            if pd.isna(v):
                v = ""
            payload.append({
                "team": team,
                "shift_type": shift_type,
                "work_date": work_date.isoformat(),
                "cell_value": str(v)
            })

    # upsert (unique index: team, shift_type, work_date)
    # supabase-py upsert는 on_conflict에 컬럼명 지정
    # 주의: payload가 너무 크면 배치로 나눠야 함(예: 500~1000개 단위)
    BATCH = 800
    for k in range(0, len(payload), BATCH):
        sb.table("schedule_cells").upsert(
            payload[k:k+BATCH],
            on_conflict="team,shift_type,work_date"
        ).execute()

def to_pivot(db_df: pd.DataFrame):
    """
    schedule_cells -> 화면용 pivot
    index: (team, shift_type)
    columns: work_date
    values: cell_value
    """
    if db_df.empty:
        return pd.DataFrame()
    pv = db_df.pivot_table(
        index=["team", "shift_type"],
        columns="work_date",
        values="cell_value",
        aggfunc="first",
        fill_value=""
    )
    # 정렬
    pv = pv.sort_index()
    pv = pv.reindex(sorted(pv.columns), axis=1)
    return pv

def db_update_cell(team: str, shift_type: str, work_date: date, new_value: str):
    sb.table("schedule_cells").upsert({
        "team": team,
        "shift_type": shift_type,
        "work_date": work_date.isoformat(),
        "cell_value": new_value
    }, on_conflict="team,shift_type,work_date").execute()

# =========================
# 3) DB 로드 / 비어있으면 Seed 안내
# =========================
db_df = fetch_db()

if db_df.empty:
    st.warning("Supabase DB가 비어있습니다. 최초 1회 엑셀 업로드로 DB를 초기화하세요.")
    up = st.file_uploader("초기 적재용 엑셀 업로드(.xlsx)", type=["xlsx"])
    if up:
        excel_df = pd.read_excel(up)
        try:
            seed_from_excel(excel_df)
            st.success("✅ DB 초기 적재 완료. 페이지를 새로고침하면 DB 기반 편집이 시작됩니다.")
        except Exception as e:
            st.error(f"초기 적재 실패: {e}")
    st.stop()

pv = to_pivot(db_df)
date_cols = list(pv.columns)
row_keys = list(pv.index)  # (team, shift_type)

# =========================
# 4) UI
# =========================
colL, colR = st.columns([1, 1])

with colL:
    st.subheader("대상 선택 → 후보 드롭다운 → DB 자동 저장")

    # 날짜 선택
    date_options = [d.strftime("%Y-%m-%d") for d in date_cols]
    date_label = st.selectbox("날짜", date_options, index=0)
    work_date = date.fromisoformat(date_label)

    # 행 선택(team, shift)
    row_labels = [f"{t} | {s}" for (t, s) in row_keys]
    row_label = st.selectbox("팀/교대", row_labels, index=0)
    idx = row_labels.index(row_label)
    team, shift_type = row_keys[idx]

    cell_value = pv.loc[(team, shift_type), work_date]
    st.write("현재 셀 값:")
    st.code(cell_value if cell_value else "(빈 값)")

    candidates, keys = parse_select_from(cell_value)
    st.write("파싱된 그룹키:", keys if keys else "-")
    st.write("후보 수:", len(candidates))

    chosen = ""
    if candidates:
        chosen = st.selectbox("대체 후보 선택", options=[""] + candidates, index=0)

    if st.button("선택값 저장(즉시 DB 반영)", type="primary", use_container_width=True):
        if not candidates:
            st.error("이 셀은 'Select from ...' 형태가 아니어서 후보가 없습니다.")
        elif not chosen:
            st.error("대체 후보를 선택하세요.")
        else:
            db_update_cell(team, shift_type, work_date, chosen)
            st.success(f"✅ 저장 완료: {team}/{shift_type}/{date_label} → {chosen}")
            st.rerun()

with colR:
    st.subheader("미리보기(DB에서 불러온 스케줄)")

    # 화면에 보기 좋은 테이블 형태로 변환
    view = pv.copy()
    view.index = [f"{t} | {s}" for (t, s) in view.index]
    view.columns = [d.strftime("%Y-%m-%d") for d in view.columns]
    st.dataframe(view, use_container_width=True, height=650)

st.caption("DB 기반 자동 저장: 업로드는 최초 1회(초기 적재용)만 필요하며, 이후 수정은 Supabase에 바로 저장됩니다.")
