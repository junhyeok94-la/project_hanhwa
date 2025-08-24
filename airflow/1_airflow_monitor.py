import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # Plotly 그래프 객체를 직접 다루기 위해 import
from itertools import product, cycle # 모든 조합을 만들기 위해 import, cycle 추가

# 페이지 설정
st.set_page_config(page_title="통합 모니터링", layout="wide")
st.title("Airflow 통합 모니터링 대시보드")

# --------------------
# 화면 상단에 필터링 조건 배치
# --------------------
# secrets.toml에서 DB 접속 정보 로드
airflow_secrets = st.secrets["airflow"]
pem_path="rds-combined-ca-bundle.pem"

# 환경 선택 라디오 버튼 (화면 상단)
selected_env = st.radio("환경을 선택하세요:", ["개발 Airflow", "운영 Airflow"])

# 환경 선택에 따라 접속 정보 결정
if selected_env == "개발 Airflow":
    selected_db_config = {
        "host": airflow_secrets["dev_host"],
        "port": airflow_secrets["dev_port"],
        "user": airflow_secrets["dev_user"],
        "password": airflow_secrets["dev_password"],
        "db": airflow_secrets["dev_db"]
    }
else:
    selected_db_config = {
        "host": airflow_secrets["prd_host"],
        "port": airflow_secrets["prd_port"],
        "user": airflow_secrets["prd_user"],
        "password": airflow_secrets["prd_password"],
        "db": airflow_secrets["prd_db"]
    }

db_uri = (
    f"mysql+pymysql://{selected_db_config['user']}:{selected_db_config['password']}@{selected_db_config['host']}:{selected_db_config['port']}/{selected_db_config['db']}"
    f"?ssl_ca={pem_path}")

# ---------------------------
# Task 데이터 불러오기
# ---------------------------
@st.cache_data(ttl=300)
def load_data(db_uri, start_date, end_date):
    try:
        engine = create_engine(db_uri)
        query = f"""
            SELECT
                ti.dag_id,
                ti.task_id,
                ti.state,
                ti.start_date,
                ti.end_date,
                ti.queued_dttm,
                ti.pool,
                ti.operator,
                d.max_active_tasks,
                ti.priority_weight
            FROM task_instance ti
            LEFT JOIN dag d ON ti.dag_id = d.dag_id
            WHERE ti.start_date BETWEEN '{start_date}' AND '{end_date}'
            AND ti.end_date IS NOT NULL
        """
        df_raw = pd.read_sql(query, con=engine)
        df_raw['start_date'] = pd.to_datetime(df_raw['start_date'])
        df_raw['end_date'] = pd.to_datetime(df_raw['end_date'])
        df_raw['queued_dttm'] = pd.to_datetime(df_raw['queued_dttm'])
        df_raw['execution_time_sec'] = (df_raw['end_date'] - df_raw['start_date']).dt.total_seconds()
        df_raw['wait_time_sec'] = (df_raw['start_date'] - df_raw['queued_dttm']).dt.total_seconds()
        return df_raw
    except Exception as e:
        st.error(f"데이터베이스 연결 오류: {e}")
        return pd.DataFrame()
    
st.header("조회 조건")
# 날짜 + 시간 필터
st.subheader("기간 설정")
col_date1, col_time1 = st.columns(2)
with col_date1:
    start_date = st.date_input("조회 시작일", datetime.date.today())
with col_time1:
    start_time = st.time_input("조회 시작시간", datetime.time(0, 0))

col_date2, col_time2 = st.columns(2)
with col_date2:
    end_date = st.date_input("조회 종료일", datetime.date.today())
with col_time2:
    end_time = st.time_input("조회 종료시간", datetime.time(1, 0))

start = datetime.datetime.combine(start_date, start_time)
end = datetime.datetime.combine(end_date, end_time)

if start > end:
    st.error("시작일은 종료일보다 이전이어야 합니다.")
else:
    df_raw = load_data(db_uri, start, end)
    if df_raw.empty:
        st.info("선택한 환경의 데이터를 불러올 수 없습니다.")
    # dag_id의 prefix가 "_"인 컨트롤 DAG 필터링
    df_raw = df_raw[~df_raw['dag_id'].str.startswith('_')]
    
    # DAG 필터링 조회조건
    st.subheader("DAG 필터링")
    dag_options = df_raw['dag_id'].unique().tolist()
    select_all = st.checkbox("DAG 전체 선택", value=True)
    selected_dags = st.multiselect("DAG 필터", dag_options, default=dag_options if select_all else [])

    df_raw = df_raw[df_raw['dag_id'].isin(selected_dags)]
    # 모든 DAG ID를 정렬하여 색상 매핑 일관성 유지
    all_dag_ids_sorted = sorted(df_raw['dag_id'].unique().tolist())
    
    # DAG ID에 대한 일관된 색상 맵 생성
    # Plotly의 기본 색상 팔레트 사용 (px.colors.qualitative.Plotly)
    # 색상 수가 부족할 경우 cycle을 사용하여 반복
    base_colors = px.colors.qualitative.Plotly
    cycled_colors = cycle(base_colors)
    dag_color_map = {dag_id: next(cycled_colors) for dag_id in all_dag_ids_sorted}
    # DAG ID에 대한 짧은 고유 ID 맵 생성
    dag_short_id_map = {dag_id: f"D{i+1}" for i, dag_id in enumerate(all_dag_ids_sorted)}

# <-----------------------------------------------------------------------------------------------------------> #
    # --- 1번 그룹: DAG 그룹 ---
    st.header("1. DAG 그룹")
    
    st.subheader("DAG별 Task 실행 시간 Gantt Chart")
    df_gantt = df_raw.copy()
    df_gantt['start'] = df_gantt['start_date']
    df_gantt['finish'] = df_gantt['end_date']
    operator_color_map = {
        'EmptyOperator': '#636EFA',
        'ExternalTaskSensor': '#EF553B',
        'EmrAddStepsOperator': '#00CC96',
        'TimeSensorAsync': '#AB63FA',
        'PythonSensor': '#FFA15A',
        'PythonOperator': '#19D3F3',
        'LatestOnlyOperator': '#FF6692',
        'TriggerDagRunOperator': '#B6E880',
        '_PythonDecoratedOperator': '#FF97FF',
        'SQLExecuteQueryOperator': '#FECB52',
        'DbtRunLocalOperator': '#636EFA'
    }
    df_gantt['color'] = df_gantt['operator'].map(operator_color_map).fillna('#CCCCCC')
    # DAG별 Gantt 차트 높이 동적 설정
    num_unique_dags_gantt = len(df_gantt['dag_id'].unique())
    # 각 DAG당 25px, 최소 300px
    gantt_dag_height = max(300, num_unique_dags_gantt * 25 + 100) 
    fig_dag_gantt = px.timeline(
        df_gantt,
        x_start='start',
        x_end='finish',
        y='dag_id',
        color='operator',
        color_discrete_map=operator_color_map,
        hover_data=['dag_id', 'task_id', 'start_date', 'end_date', 'pool', 'operator', 'max_active_tasks']
    )
    fig_dag_gantt.update_layout(
        title="DAG별 Task 실행 시간",
        xaxis_title="시간",
        yaxis_title="DAG",
        xaxis_tickformat="%Y-%m-%d %H:%M:%S",
        showlegend=True,
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True, tickfont=dict(size=10)),
        height=gantt_dag_height # 동적으로 계산된 높이 적용
    )
    fig_dag_gantt.update_yaxes(autorange="reversed")
    
    st.plotly_chart(fig_dag_gantt, use_container_width=True)

    # Area Chart (시간대별 task 실행 누적)
    st.subheader("전체 Task 실행 수 누적 영역 차트 (초 단위)") # 제목 변경
    
    # df_raw가 비어있는 경우 처리
    if df_raw.empty:
        st.info("선택된 기간에 실행된 Task 데이터가 없습니다.")
    else:
        all_dag_ids = df_raw['dag_id'].unique()
        min_time = df_raw['start_date'].min().replace(microsecond=0)
        max_time = df_raw['end_date'].max().replace(microsecond=0)
    
        if min_time > max_time:
            st.warning("선택된 기간 내에 유효한 Task 시작/종료 시간이 없습니다.")
        else:
            all_seconds = pd.date_range(start=min_time, end=max_time, freq='s')
            full_grid_data = list(product(all_seconds, all_dag_ids))
            df_full_grid = pd.DataFrame(full_grid_data, columns=['second', 'dag_id'])
    
            task_active_seconds = []
            for _, row in df_raw.iterrows():
                current = row['start_date'].replace(microsecond=0)
                end_second = row['end_date'].replace(microsecond=0)
                while current <= end_second:
                    task_active_seconds.append({'second': current, 'dag_id': row['dag_id']})
                    current += datetime.timedelta(seconds=1)
            
            if not task_active_seconds:
                st.info("선택된 기간에 활성 Task 데이터가 없습니다.")
            else:
                df_active_counts = pd.DataFrame(task_active_seconds).groupby(['second', 'dag_id']).size().reset_index(name='task_count')
                area_df = pd.merge(df_full_grid, df_active_counts, on=['second', 'dag_id'], how='left').fillna(0)
                
                # 툴팁에 사용할 DAG별 Task 수 정보 생성
                area_df['dag_task_info'] = area_df.apply(
                    lambda row: f"DAG ID: {row['dag_id']}<br>Task 수 (이 DAG): {row['task_count']:.0f}" if row['task_count'] > 0 else "",
                    axis=1
                )
                
                # Plotly Express Area Chart 생성
                fig_area = px.area(
                    area_df,
                    x='second',
                    y='task_count',
                    color='dag_id',
                    title="시간별 DAG Task 실행 수 (누적)",
                    labels={'second': '시간', 'task_count': 'Task 수'},
                    hover_data={
                        'second': '|%Y-%m-%d %H:%M:%S',
                        'dag_id': False,
                        'task_count': False,
                        'dag_task_info': True # 커스텀 정보 표시
                    },
                    custom_data=['dag_task_info'],
                    category_orders={"dag_id": all_dag_ids_sorted},
                    color_discrete_map=dag_color_map
                )
    
                # 툴팁이 영역 위로 마우스 오버 시 나타나도록 설정
                fig_area.update_layout(
                    xaxis_title="시간", 
                    yaxis_title="Task 수", 
                    xaxis_tickformat="%Y-%m-%d %H:%M:%S",
                    #hovermode="x unified" # 툴팁 모드를 "x unified"로 설정
                    hovermode="x" 
                )
    
                # 각 트레이스(영역)의 속성 업데이트
                for trace in fig_area.data:
                    dag_id_for_trace = trace.name # 각 트레이스의 이름은 dag_id가 됩니다.
                    
                    # fillcolor를 명시적으로 설정하여 색상 일관성 유지
                    if dag_id_for_trace in dag_color_map:
                        trace.fillcolor = dag_color_map[dag_id_for_trace]
                    # `hovertemplate`는 `customdata`가 빈 문자열이면 해당 줄을 건너뜁니다.
                    # 이를 이용해 Task 수가 0인 경우는 툴팁에 표시되지 않도록 합니다.
                    trace.hovertemplate = (
                        # "시간: %{x|%Y-%m-%d %H:%M:%S}<br>" +
                        "<b>%{customdata[0]}</b><br>" +
                        "<extra></extra>"
                    )
                    trace.line.width = 0
                    trace.hoverinfo = 'all'
                    
                st.plotly_chart(fig_area, use_container_width=True)
    # --- 3번 그룹: 전체 Grid 조회 ---
    st.header("3. 전체 Grid 조회")
    st.subheader("Task Grid")
    with st.expander("Task 필터링 (dag_id, pool, operator)"):
        dag_filter = st.text_input("dag_id 포함 텍스트")
        pool_filter = st.text_input("pool 포함 텍스트")
        operator_filter = st.text_input("operator 포함 텍스트")
    filtered_tasks = df_raw.copy()
    if dag_filter:
        filtered_tasks = filtered_tasks[filtered_tasks['dag_id'].str.contains(dag_filter, case=False)]
    if pool_filter:
        filtered_tasks = filtered_tasks[filtered_tasks['pool'].str.contains(pool_filter, na=False, case=False)]
    if operator_filter:
        filtered_tasks = filtered_tasks[filtered_tasks['operator'].str.contains(operator_filter, case=False)]
    st.dataframe(filtered_tasks.sort_values("execution_time_sec", ascending=False)[[
        "dag_id", "task_id", "state", "execution_time_sec", "start_date", "end_date", "pool", "operator"
    ]])
