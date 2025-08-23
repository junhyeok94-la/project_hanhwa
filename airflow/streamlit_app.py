import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
import os
from sqlalchemy import create_engine
import plotly.graph_objects as go
import plotly.express as px

# 데이터베이스 연결
@st.cache_resource
def get_postgres_connection():
    # Streamlit Cloud Secret에서 데이터베이스 연결 정보 가져오기
    # os.environ.get으로 환경 변수 접근
    # 로컬 테스트 시에는 직접 연결 정보 명시
    db_user = os.getenv("POSTGRES_USER", "junhyeok")
    db_password = os.environ.get("DB_PASSWORD", "Gozld4wjf!")
    db_host = os.getenv("POSTGRES_HOST", "hanhwa-ocean-sample-1.cidpj5fbqkiw.ap-northeast-2.rds.amazonaws.com")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "postgres")

    conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(conn_string)
    return engine

# aggreagted_bi_data 테이블에서 데이터 로드 (캐싱 적용)
@st.cache_data
def get_aggregated_data():
    try:
        engine = get_postgres_connection()
        query = "SELECT * FROM bi.aggregated_bi_data;"
        df = pd.read_sql(query, engine)
        
        # 'contract_date' 컬럼이 문자열이면 datetime으로 변환
        if not pd.api.types.is_datetime64_any_dtype(df['contract_date']):
            df['contract_date'] = pd.to_datetime(df['contract_date'])

        # 'revenue' 컬럼이 문자열이면 숫자형으로 변환
        if not pd.api.types.is_numeric_dtype(df['revenue']):
            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"데이터베이스 연결 또는 aggregated_bi_data 로드 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

# movement_logs_stg 테이블에서 데이터 로드 (캐싱 적용)
@st.cache_data
def get_movement_logs():
    try:
        engine = get_postgres_connection()
        query = "SELECT * FROM stg.movement_logs_stg ORDER BY log_datetime ASC;"
        df = pd.read_sql(query, engine)
        
        # 'log_datetime' 컬럼이 문자열이면 datetime으로 변환
        if not pd.api.types.is_datetime64_any_dtype(df['log_datetime']):
            df['log_datetime'] = pd.to_datetime(df['log_datetime'])
        
        return df
    except Exception as e:
        st.error(f"데이터베이스 연결 또는 movement_logs_stg 로드 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

# Streamlit 앱 시작
st.set_page_config(
    page_title="한화오션 사업 현황 BI 대시보드",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 로고와 타이틀을 나란히 배치하기 위해 컬럼을 사용
logo_col, title_col, desc_col = st.columns([1, 6, 3]) 

# 로고 이미지 URL (임의의 이미지 사용. 실제 로고 이미지로 교체 가능)
logo_url = "Hanwha_Ocean_Logo.png"

with logo_col:
    st.image(logo_url, width=170) 

with title_col:
    st.title('한화오션 사업 현황 BI 대시보드')
    st.markdown("### 비즈니스 단위별 통합 데이터 시각화")
with desc_col:
    st.markdown("""
    - **주요 사업**
    - `'merchant_ships'` : 상선 사업
    - `'offshore_plants'` : 해양플랜트 사업
    - `'special_ships'` : 특수선 사업
    """)

# 데이터 로드
agg_df = get_aggregated_data()
log_df = get_movement_logs()

if agg_df.empty:
    st.warning("데이터를 불러오지 못했습니다. 데이터베이스 연결 상태를 확인해주세요.")
else:
    # 사이드바 필터링
    st.sidebar.header('필터 옵션')
    selected_business_unit = st.sidebar.multiselect(
        '사업 단위 선택',
        options=agg_df['business_unit'].unique(),
        default=agg_df['business_unit'].unique()
    )

    if not selected_business_unit:
        st.warning("하나 이상의 사업 단위를 선택해주세요.")
    else:
        filtered_agg_df = agg_df[agg_df['business_unit'].isin(selected_business_unit)]
        
        # 날짜 필터링
        start_date, end_date = st.sidebar.date_input(
            "날짜 범위 선택",
            value=[filtered_agg_df['contract_date'].min().date(), filtered_agg_df['contract_date'].max().date()],
            min_value=filtered_agg_df['contract_date'].min().date(),
            max_value=filtered_agg_df['contract_date'].max().date()
        )
        
        filtered_agg_df = filtered_agg_df[(filtered_agg_df['contract_date'].dt.date >= start_date) & (filtered_agg_df['contract_date'].dt.date <= end_date)]

        st.markdown("---")

        # 1. 누적 계약 건수 & 총 매출액 KPI 지표 (사업부문별로도 표시)
        st.subheader('누적 계약 건수 및 총 매출액')

        total_contracts = filtered_agg_df.shape[0]
        total_revenue = filtered_agg_df['revenue'].sum()
        col1, col2 = st.columns(2)
        with col1:
            st.metric("총 계약 건수", f"{total_contracts:,}건")
        with col2:
            st.metric("총 매출액", f"₩ {total_revenue / 1000000000:.2f}억")
        
        st.markdown("##### 사업부문별 주요 지표")
        kpi_cols = st.columns(len(selected_business_unit))
        for i, bu in enumerate(selected_business_unit):
            bu_df = filtered_agg_df[filtered_agg_df['business_unit'] == bu]
            bu_contracts = bu_df.shape[0]
            bu_revenue = bu_df['revenue'].sum()
            with kpi_cols[i]:
                st.markdown(f"**{bu.replace('_', ' ').title()}**")
                st.metric("계약 건수", f"{bu_contracts:,}건")
                st.metric("매출액", f"₩ {bu_revenue / 1000000000:.2f}억")

        st.markdown("---")

        # 2. 비즈니스 단위별 건수 및 매출액 (라인 차트)
        st.subheader('비즈니스 단위별 추이')

        time_unit = st.radio("시간 단위 선택", ('일별', '월별'), horizontal=True)

        if time_unit == '일별':
            grouped_df = filtered_agg_df.groupby([filtered_agg_df['contract_date'].dt.date, 'business_unit']).agg(
                contracts=('project_id', 'count'),
                total_revenue=('revenue', 'sum')
            ).reset_index()
            grouped_df.rename(columns={'contract_date': '날짜'}, inplace=True)
        else:
            grouped_df = filtered_agg_df.groupby([filtered_agg_df['contract_date'].dt.to_period('M'), 'business_unit']).agg(
                contracts=('project_id', 'count'),
                total_revenue=('revenue', 'sum')
            ).reset_index()
            grouped_df['날짜'] = grouped_df['contract_date'].astype(str)
            
        grouped_df['total_revenue_billion'] = grouped_df['total_revenue'] / 100000000

        col3, col4 = st.columns(2)
        with col3:
            st.markdown("##### 계약 건수 추이")
            fig_contracts = px.line(grouped_df, x='날짜', y='contracts', color='business_unit', markers=True)
            st.plotly_chart(fig_contracts, use_container_width=True)
        with col4:
            st.markdown("##### 총 매출액 추이 (단위: 억)")
            fig_revenue = px.line(grouped_df, x='날짜', y='total_revenue_billion', color='business_unit', markers=True)
            st.plotly_chart(fig_revenue, use_container_width=True)

        st.markdown("---")

        # 3. 계약 국가별 현황 (상위 10개국)
        st.subheader('계약 국가별 현황 (상위 10개국)')
        country_summary = filtered_agg_df.groupby('country').agg(
            contracts=('project_id', 'count'),
            total_revenue=('revenue', 'sum')
        ).sort_values(by='contracts', ascending=False).head(10).reset_index()

        col5, col6 = st.columns(2)
        with col5:
            st.markdown("##### 계약 건수")
            fig_country_contracts = go.Figure(data=[go.Pie(labels=country_summary['country'], values=country_summary['contracts'])])
            st.plotly_chart(fig_country_contracts, use_container_width=True)
        with col6:
            st.markdown("##### 총 매출액 (단위: 억)")
            country_summary['total_revenue_billion'] = country_summary['total_revenue'] / 100000000
            fig_country_revenue = go.Figure(data=[go.Pie(labels=country_summary['country'], values=country_summary['total_revenue_billion'])])
            st.plotly_chart(fig_country_revenue, use_container_width=True)

        st.markdown("---")

        # 4. 지도 시각화 (정적 위치 + 동적 이동 경로)
        st.subheader('사업별 위치 및 이동 경로')
        st.markdown("##### 모든 프로젝트의 계약 위치와 특정 프로젝트의 이동 경로를 시각화합니다.")

        # 사이드바에서 이동 경로를 볼 프로젝트 선택
        project_ids_with_logs = log_df['project_id'].unique()
        selected_project = st.sidebar.selectbox(
            '이동 경로를 볼 프로젝트 선택',
            options=['선택 안함'] + list(project_ids_with_logs)
        )

        location_df = filtered_agg_df.dropna(subset=['latitude', 'longitude']).copy()
        
        if not location_df.empty:
            m = folium.Map(location=[location_df['latitude'].mean(), location_df['longitude'].mean()], zoom_start=2)
            
            # 모든 프로젝트의 정적 위치를 MarkerCluster로 표시
            marker_cluster = MarkerCluster().add_to(m)
            for index, row in location_df.iterrows():
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=f"사업명: {row['project_id']}<br>제품: {row['product_type']}<br>매출: {row['revenue']:,}",
                    tooltip=row['product_type']
                ).add_to(marker_cluster)

            # 선택된 프로젝트의 이동 경로를 라인으로 그리기
            if selected_project != '선택 안함':
                st.markdown(f"**선택된 프로젝트: {selected_project}의 이동 경로**")
                project_path_df = log_df[log_df['project_id'] == selected_project].sort_values(by='log_datetime')
                if not project_path_df.empty:
                    path_points = project_path_df[['latitude', 'longitude']].values.tolist()
                    folium.PolyLine(path_points, color='red', weight=2.5, opacity=1).add_to(m)
                    
                    # 시작점과 끝점 마커 추가
                    folium.CircleMarker(
                        location=path_points[0],
                        radius=5,
                        color='green',
                        fill=True,
                        fill_color='green',
                        tooltip="경로 시작"
                    ).add_to(m)
                    folium.CircleMarker(
                        location=path_points[-1],
                        radius=5,
                        color='blue',
                        fill=True,
                        fill_color='blue',
                        tooltip="경로 끝"
                    ).add_to(m)

            st_folium(m, width=700, height=500)
        else:
            st.warning("선택된 필터에 해당하는 위치 데이터가 없습니다.")

        st.markdown("---")

        # 5. 건조 진행도 (Stage Progress) - 상세 테이블로 변경
        st.subheader('건조 진행도 상세 현황')
        
        progress_df = filtered_agg_df[['project_id', 'business_unit', 'product_type', 'stage_progress', 'delivery_date']].copy()
        progress_df = progress_df.dropna(subset=['stage_progress'])

        if not progress_df.empty:
            st.dataframe(progress_df.sort_values(by='stage_progress', ascending=False), use_container_width=True)
        else:
            st.warning("선택된 필터에 해당하는 건조 진행도 데이터가 없습니다.")