# app.py
# 실행: streamlit run app.py
# 필요 패키지: streamlit pandas openpyxl

import re
import io
import pandas as pd
import streamlit as st

st.set_page_config(page_title="교대 대체자 입력(Excel 기반)", layout="wide")

st.title("교대 근무 'Select from A1/A2/B1...' → 드롭다운 입력 → 엑셀 저장")

# =========================
# 1) 그룹 정의 (사용자 제공 로직)
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

def parse_select_from(cell_value: str):
    """
    "Select from A1", "Select from B1 or B2", "Select from A1 or A2" 등에서
    그룹 키를 추출하여 후보 리스트(정렬) 반환
    """
    if not isinstance(cell_value, str):
        return [], []
    if "Select from" not in cell_value:
        return [], []

    keys = GROUP_KEY_PATTERN.findall(cell_value)
    keys = list(dict.fromkeys(keys))  # 중복 제거(순서 유지)
    candidates = set()
    unknown = []
    for k in keys:
        if k in GROUPS:
            candidates |= GROUPS[k]
        else:
            unknown.append(k)

    return sorted(candidates), keys

def is_date_col(col):
    # 엑셀에서 날짜 컬럼은 Timestamp/datetime으로 들어올 가능성이 높음
    return isinstance(col, (pd.Timestamp,))

def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x)

# =========================
# 2) 파일 업로드 & 로드
# =========================
uploaded = st.file_uploader("엑셀 업로드 (.xlsx)", type=["xlsx"])
if not uploaded:
    st.info("엑셀을 업로드하면, 'Select from ...' 셀만 골라서 드롭다운 입력 UI를 제공합니다.")
    st.stop()

# 엑셀 로드
try:
    xls = pd.ExcelFile(uploaded)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(uploaded, sheet_name=sheet)
except Exception as e:
    st.error(f"엑셀 로드 실패: {e}")
    st.stop()

# 세션에 저장(수정 상태 유지)
if "df" not in st.session_state:
    st.session_state.df = df.copy()
    st.session_state.sheet = sheet

df = st.session_state.df

# =========================
# 3) 엑셀 구조 가정/보정
# - 첫 두 컬럼: 팀(예: T1/T2), 구분(Date: Day/Night/Off/Replace)
# - 나머지 컬럼: 날짜(2026-02-01 ...)
# =========================
if df.shape[1] < 3:
    st.error("엑셀 형식이 예상과 다릅니다. (최소 3개 컬럼 필요: 팀/구분/날짜...)")
    st.stop()

team_col = df.columns[0]
type_col = df.columns[1]

# 팀 컬럼은 병합 셀처럼 NaN이 있을 수 있으니 ffill로 라벨링
team_filled = df[team_col].copy()
team_filled = team_filled.ffill()

# 날짜 컬럼 후보
date_cols = [c for c in df.columns[2:] if is_date_col(c)]
if not date_cols:
    # 혹시 문자열 날짜라면 파싱 시도
    maybe_date_cols = []
    for c in df.columns[2:]:
        try:
            pd.to_datetime(c)
            maybe_date_cols.append(c)
        except:
            pass
    date_cols = maybe_date_cols

if not date_cols:
    st.error("날짜 컬럼을 찾지 못했습니다. 3번째 컬럼부터 날짜 컬럼이 있어야 합니다.")
    st.stop()

# 표시용 날짜 옵션 만들기
date_options = []
col_to_label = {}
label_to_col = {}
for c in date_cols:
    if isinstance(c, pd.Timestamp):
        label = c.strftime("%Y-%m-%d")
    else:
        # 문자열이면 그대로 쓰되 YYYY-MM-DD 형태로 보이게
        try:
            label = pd.to_datetime(c).strftime("%Y-%m-%d")
        except:
            label = str(c)
    date_options.append(label)
    col_to_label[c] = label
    label_to_col[label] = c

# =========================
# 4) "Select from ..." 셀 인덱스 구축
# =========================
selectable_rows = []
for idx in range(len(df)):
    # Day/Night만 보이게 하고 싶으면 아래 조건을 조정
    row_type = safe_str(df.at[idx, type_col]).strip()
    # Off/Replace도 포함 가능하지만 보통 Day/Night 입력이 핵심이라 기본은 제외
    if row_type not in ("Day", "Night"):
        continue

    # 날짜 컬럼 중 하나라도 Select from가 있으면 row 후보로
    has_any = False
    for c in date_cols:
        v = df.at[idx, c]
        if isinstance(v, str) and "Select from" in v:
            has_any = True
            break
    if has_any:
        selectable_rows.append(idx)

if not selectable_rows:
    st.warning("Day/Night 행에서 'Select from ...' 셀을 찾지 못했습니다. 엑셀을 확인하세요.")
    st.stop()

# 행 라벨 만들기
row_labels = []
label_to_row = {}
for i in selectable_rows:
    team = safe_str(team_filled.iat[i]).strip()
    rtype = safe_str(df.at[i, type_col]).strip()
    label = f"Row {i:02d} | {team} | {rtype}"
    row_labels.append(label)
    label_to_row[label] = i

# =========================
# 5) UI
# =========================
colL, colR = st.columns([1, 1])

with colL:
    st.subheader("입력 대상 선택")
    date_label = st.selectbox("날짜(컬럼)", options=date_options, index=0)
    row_label = st.selectbox("행(Row)", options=row_labels, index=0)

    target_col = label_to_col[date_label]
    target_row = label_to_row[row_label]

    cell_value = df.at[target_row, target_col]
    cell_value_str = safe_str(cell_value)

    st.write("선택된 셀 내용:")
    st.code(cell_value_str if cell_value_str else "(빈 값)")

    candidates, keys = parse_select_from(cell_value_str)

    if "Select from" in cell_value_str and not candidates:
        st.warning("문구는 'Select from'인데 그룹키(A1~D2)를 파싱하지 못했습니다. 문구/규칙을 확인하세요.")

    st.write("파싱된 그룹키:", keys if keys else "-")
    st.write("후보 수:", len(candidates))

    # 드롭다운 후보
    if candidates:
        chosen = st.selectbox("대체 후보 선택(드롭다운)", options=[""] + candidates, index=0)
    else:
        chosen = ""

    btn_apply = st.button("선택값을 셀에 입력(반영)", type="primary", use_container_width=True)

    if btn_apply:
        if not chosen:
            st.error("대체 후보를 선택하세요.")
        else:
            # 셀에 입력
            df.at[target_row, target_col] = chosen
            st.session_state.df = df
            st.success(f"✅ 반영 완료: {date_label} / {row_label} → {chosen}")

    st.divider()
    st.subheader("엑셀 다운로드")
    filename = st.text_input("저장 파일명", value="updated_schedule.xlsx")

    # 엑셀로 내보내기
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=st.session_state.sheet, index=False)
    st.download_button(
        label="수정본 엑셀 다운로드",
        data=output.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

with colR:
    st.subheader("미리보기(선택한 날짜 컬럼)")
    # 선택한 날짜 컬럼만 보기 좋게 표시 (Day/Night만)
    preview = df.loc[:, [team_col, type_col, target_col]].copy()
    preview[team_col] = team_filled
    # Day/Night만
    preview = preview[preview[type_col].isin(["Day", "Night"])].reset_index(drop=True)
    # 빈값 정리
    preview[target_col] = preview[target_col].apply(safe_str)

    st.dataframe(preview, use_container_width=True, height=650)

st.caption("로직: 셀에 'Select from A1' 또는 'Select from B1 or B2'가 있으면, 해당 그룹 구성원을 후보로 드롭다운 제공 후 선택값을 셀에 그대로 입력합니다.")
