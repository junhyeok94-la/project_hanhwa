import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import urllib.parse
import time

# ---
# ⚠️ Pandas Styler 설정: 경고 해결
# DataFrame의 셀 수가 많아질 경우 Styler가 렌더링할 수 있도록 최대 셀 개수 설정
pd.set_option("styler.render.max_elements", 1_000_000)
# ---

st.set_page_config(layout="wide")
st.title("DB 정합성 모니터링")

# --------------------
# 1. secrets.toml에서 Redshift 접속 정보 로드
# --------------------
try:
    redshift_secrets = st.secrets["redshift"]
except KeyError:
    st.error("`.streamlit/secrets.toml` 파일에 'redshift' 섹션이 없습니다. 접속 정보를 올바르게 설정해주세요.")
    st.stop()

# --------------------
# 2. 데이터 조회 함수
# --------------------
@st.cache_data(ttl=3600, show_spinner=False)
def get_table_info(conn_config, schemas):
    """
    주어진 스키마 목록의 모든 테이블과 컬럼 정보를 information_schema.columns에서 가져옵니다.
    """
    with st.spinner(f"데이터베이스 연결 및 {schemas} 스키마 정보 로딩 중..."):
        try:
            conn_red = psycopg2.connect(
                dbname=conn_config["database"],
                host=conn_config["host"],
                port=conn_config["port"],
                user=conn_config["user"],
                password=conn_config["password"]
            )
            
            schema_list_str = ", ".join([f"'{s}'" for s in schemas])
            
            query = f"""
                SELECT
                    c.table_schema as tbl_schema,
                    c.table_name,
                    c.column_name,
                    pgd.description AS column_comment,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_precision_radix
                FROM
                    information_schema.columns c
                LEFT JOIN
                    pg_catalog.pg_class t ON t.relname = c.table_name AND t.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema)
                LEFT JOIN
                    pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attname = c.column_name
                LEFT JOIN
                    pg_catalog.pg_description pgd ON pgd.objoid = t.oid AND pgd.objsubid = a.attnum
                WHERE
                    c.table_schema IN ({schema_list_str})
                    AND c.table_name NOT LIKE '%2025%'
                    AND c.table_name NOT LIKE 'drop%'
                    AND c.table_name NOT LIKE 'previous%'
                ORDER BY
                    c.table_schema, c.table_name, c.ordinal_position;
            """
            
            df = pd.read_sql(query, conn_red)
            conn_red.close()
            return df
        
        except Exception as e:
            st.error(f"데이터베이스 연결 또는 쿼리 오류: {e}")
            return pd.DataFrame()

# --------------------
# 3. 정합성 체크 및 시각화 함수
# --------------------
def check_consistency(src_tables_df, tgt_tables_df, src_schema_name, tgt_schema_name):
    # 'base%'로 시작하는 스키마명을 처리하기 위해 데이터를 전처리
    if src_schema_name == "운영 BASE":
        src_tables_df['table_schema'] = src_tables_df['tbl_schema'].apply(
            lambda x: x.replace('base_', '')
        )
    else:
        src_tables_df['table_schema'] = src_tables_df['tbl_schema']
    
    if tgt_schema_name == "운영 TOBE":
        tgt_tables_df['table_schema'] = tgt_tables_df['tbl_schema']
    
    # 💡 정합성 체크 설명 추가
    st.markdown("""
    이 검사는 **SRC와 TGT 스키마 간의 메타데이터 불일치**를 확인합니다.
    - **SRC 와 TGT 기준**
      - `SRC`: 개발 TOBE redshift의 'sdhub_own','dhub_own','gsbi_own' 스키마
      - `TGT`: 운영 TOBE redshift의 'sdhub_own','dhub_own','gsbi_own' 스키마
    - **정합성 불일치 체크 기준:**
      - `'[TGT]'에 컬럼 없음`: SRC에만 존재하는 컬럼
      - `'[SRC]'에 컬럼 없음`: TGT에만 존재하는 컬럼
      - `데이터 타입 불일치`: SRC와 TGT의 데이터 타입이 다른 경우
      - `컬럼 길이 불일치`: VARCHAR, CHAR 타입의 길이가 다른 경우
      - `숫자 타입 정밀도 불일치`: NUMERIC 타입의 정밀도가 다른 경우
    """)

    merged_df = pd.merge(
        src_tables_df,
        tgt_tables_df,
        on=['table_schema', 'table_name', 'column_name'],
        how='outer',
        suffixes=('_src', '_tgt'),
        indicator=True
    )
    
    # 💡 character_maximum_length를 정수형으로 변환 (소수점 제거)
    merged_df['character_maximum_length_src'] = pd.to_numeric(merged_df['character_maximum_length_src'], errors='coerce').astype('Int64')
    merged_df['character_maximum_length_tgt'] = pd.to_numeric(merged_df['character_maximum_length_tgt'], errors='coerce').astype('Int64')

    # 💡 Status_detail 컬럼을 위한 함수
    def get_status_detail(row):
        if row['_merge'] == 'left_only':
            return f"'{tgt_schema_name}'에 컬럼 없음"
        if row['_merge'] == 'right_only':
            return f"'{src_schema_name}'에 컬럼 없음"

        if row['data_type_src'] != row['data_type_tgt']:
            return "데이터 타입 불일치"

        if row['data_type_src'] in ['character varying', 'varchar', 'char']:
            if not pd.isna(row['character_maximum_length_src']) and not pd.isna(row['character_maximum_length_tgt']):
                if row['character_maximum_length_src'] != row['character_maximum_length_tgt']:
                    return "컬럼 길이 불일치"
            elif not pd.isna(row['character_maximum_length_src']) or not pd.isna(row['character_maximum_length_tgt']):
                return "컬럼 길이 불일치"

        if row['data_type_src'] == 'numeric':
            if not pd.isna(row['numeric_precision_src']) and not pd.isna(row['numeric_precision_tgt']):
                if row['numeric_precision_src'] != row['numeric_precision_tgt'] or \
                   row['numeric_precision_radix_src'] != row['numeric_precision_radix_tgt']:
                    return "숫자 타입 정밀도 불일치"
            elif not pd.isna(row['numeric_precision_src']) or not pd.isna(row['numeric_precision_tgt']):
                return "숫자 타입 정밀도 불일치"
        return "OK"

    # 💡 Mismatch 여부를 판단하는 간단한 Status 컬럼
    def get_status(detail_status):
        return "OK" if detail_status == "OK" else "Mismatch"

    merged_df['Status_detail'] = merged_df.apply(get_status_detail, axis=1)
    merged_df['Status'] = merged_df['Status_detail'].apply(get_status)

    total_count = len(merged_df)
    error_count = len(merged_df[merged_df['Status'] != 'OK'])

    # 💡 테이블 단위 정합성 지표 계산
    table_status_df = merged_df.groupby(['table_schema', 'table_name']).agg(
        total_columns=('column_name', 'size'),
        mismatch_columns=('Status', lambda x: (x == 'Mismatch').sum())
    ).reset_index()

    table_status_df['matching_rate'] = (1 - table_status_df['mismatch_columns'] / table_status_df['total_columns']).fillna(1) * 100
    
    table_status_df['table_status'] = table_status_df.apply(
        lambda row: 'OK' if row['mismatch_columns'] == 0 else 'Mismatch', axis=1
    )

    # 💡 테이블, 컬럼 단위 지표 시각화
    st.markdown("#### 정합성 요약")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("총 테이블 수", len(table_status_df))
    col2.metric("정합성 불일치 테이블 수", len(table_status_df[table_status_df['table_status'] == 'Mismatch']))
    col3.metric("정합성 일치율(테이블 기준)", f"{(1 - len(table_status_df[table_status_df['table_status'] == 'Mismatch'])/len(table_status_df))*100:.2f}%" if len(table_status_df) > 0 else "0%")
    
    col4, col5, col6 = st.columns(3)
    col4.metric("총 컬럼 수", total_count)
    col5.metric("정합성 불일치 컬럼 수", error_count)
    col6.metric("정합성 일치율(컬럼 기준)", f"{(1 - error_count/total_count)*100:.2f}%" if total_count > 0 else "0%")

    st.markdown("---")
    
    st.dataframe(table_status_df.sort_values(by=['table_status', 'mismatch_columns'], ascending=[False, False]), 
                 use_container_width=True, height=300)

    st.markdown("---")
    st.markdown("#### 정합성 체크 상세 지표")
    
    search_query = st.text_input(
        "테이블명 검색",
        help="찾으려는 테이블명을 입력하세요. (예: sales_table)",
        key=f"table_search_{src_schema_name}_{tgt_schema_name}"
    )

    status_options = merged_df['Status'].unique().tolist()
    selected_status = st.multiselect(
        "상태 필터",
        status_options,
        default=status_options,
        key=f"status_filter_{src_schema_name}_{tgt_schema_name}"
    )

    filtered_df = merged_df[
        merged_df['Status'].isin(selected_status) &
        (merged_df['table_name'].str.contains(search_query, case=False, na=False))
    ].sort_values(by=['Status', 'table_name', 'column_name'], ascending=[False, True, True])

    rows_per_page = 100
    total_rows = len(filtered_df)
    total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0)
    
    st.info(f"총 `{total_rows}`개의 결과가 검색되었습니다.")
    
    if total_pages > 1:
        page_number = st.number_input("페이지 번호", 1, total_pages, 1, key=f"page_{src_schema_name}_{tgt_schema_name}")
    else:
        page_number = 1

    start_row = (page_number - 1) * rows_per_page
    end_row = start_row + rows_per_page
    paginated_df = filtered_df.iloc[start_row:end_row]

    def highlight_status(row):
        if row['Status'] == 'OK':
            return ['background-color: #e0ffe0'] * len(row)
        else:
            return ['background-color: #ffcccc'] * len(row)
    
    # 💡 'Status_detail' 컬럼을 추가
    st.dataframe(
        paginated_df[[
            'table_schema', 'table_name', 'column_name', 'Status', 'Status_detail',
            'column_comment_src', 'column_comment_tgt',
            'data_type_src', 'data_type_tgt',
            'character_maximum_length_src', 'character_maximum_length_tgt',
            'numeric_precision_src', 'numeric_precision_tgt',
            'numeric_precision_radix_src', 'numeric_precision_radix_tgt'
        ]].style.apply(highlight_status, axis=1),
        use_container_width=True,
        hide_index=True,
        height=500
    )
    st.markdown("---")


# --------------------
# 4. TGT 검사 전용 함수
# --------------------
def check_tgt_comments(df):
    st.subheader("TGT 스키마 - 코멘트 누락 컬럼 검사")
    
    st.markdown("""
    이 검사는 **운영 TOBE 스키마의 컬럼 중 코멘트가 누락된 목록**을 확인합니다.
    - **점검 기준:** `column_comment`가 `NULL`인 경우
    - **코멘트 누락 원인:**
      - 데이터 모델링 시 코멘트가 작성되지 않음
      - ETL 작업 중 메타데이터가 누락됨
    """)
    
    no_comment_df = df[pd.isna(df['column_comment'])].sort_values(by=['tbl_schema', 'table_name', 'column_name'])
    
    total_count = len(df)
    no_comment_count = len(no_comment_df)
    
    st.metric("코멘트 누락 컬럼 수", no_comment_count)
    st.info(f"총 `{no_comment_count}`개의 코멘트 누락 컬럼이 검색되었습니다.")
    
    st.dataframe(no_comment_df[[
        'tbl_schema', 'table_name', 'column_name', 'column_comment'
    ]], use_container_width=True, hide_index=True, height=500)


# --------------------
# 5. 정합성 체크 실행
# --------------------
with st.spinner('테이블 정보를 로딩 중입니다...'):
    start_time = time.time()
    
    # DB 접속 정보
    redshift_dev = {
        "host": st.secrets["redshift"]["dev_host"], "port": st.secrets["redshift"]["dev_port"],
        "user": st.secrets["redshift"]["dev_user"], "password": st.secrets["redshift"]["dev_password"],
        "database": st.secrets["redshift"]["dev_database"]
    }
    redshift_prod = {
        "host": st.secrets["redshift"]["prd_host"], "port": st.secrets["redshift"]["prd_port"],
        "user": st.secrets["redshift"]["prd_user"], "password": st.secrets["redshift"]["prd_password"],
        "database": st.secrets["redshift"]["prd_database"]
    }
    
    dev_tables_df = get_table_info(redshift_dev, ['gsbi_own', 'sdhub_own', 'dhub_own'])
    prod_tobe_tables_df = get_table_info(redshift_prod, ['gsbi_own', 'sdhub_own', 'dhub_own'])
    # prod_base_tables_df = get_table_info(redshift_prod, ['base_gsbi_own', 'base_sdhub_own', 'base_dhub_own'])

    dev_tables_df_filtered = dev_tables_df[dev_tables_df['tbl_schema'].isin(['gsbi_own', 'sdhub_own', 'dhub_own'])].copy()
    prod_tobe_tables_df_filtered = prod_tobe_tables_df[prod_tobe_tables_df['tbl_schema'].isin(['gsbi_own', 'sdhub_own', 'dhub_own'])].copy()
    # prod_base_tables_df_filtered = prod_base_tables_df[prod_base_tables_df['tbl_schema'].isin(['base_gsbi_own', 'base_sdhub_own', 'base_dhub_own'])].copy()

    tab1, tab2 = st.tabs(["정합성 체크 (SRC/TGT)", "TGT 검사"])

    with tab1:
        check_consistency(dev_tables_df_filtered, prod_tobe_tables_df_filtered, "개발 TOBE", "운영 TOBE")
        # st.markdown("---")
        # check_consistency(prod_base_tables_df_filtered, prod_tobe_tables_df_filtered, "운영 BASE", "운영 TOBE")

    with tab2:
        check_tgt_comments(prod_tobe_tables_df_filtered)

    end_time = time.time()
    st.sidebar.success(f"데이터 로딩 및 정합성 체크 완료\n(총 소요 시간: {end_time - start_time:.2f}초)")
