from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Asia/Seoul")
now = datetime.now(local_tz)
end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(days=1)

# DAG 파일 이름을 기준으로 dag_id를 동적으로 설정
DAG_FILE_NAME = os.path.basename(__file__).replace('.py', '')

with DAG(
    dag_id=DAG_FILE_NAME,
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=['hanwha_ocean', 'bi'],
    doc_md="""
    ## BI (Business Intelligence) 데이터 파이프라인
    - 이 DAG는 `mart` 스키마의 여러 테이블을 통합하여 최종 `bi` 스키마에 적재합니다.
    - Streamlit 앱에서 직접 조회하는 최종 시각화용 데이터입니다.
    """,
    # 날짜 필터링이 필요 없으므로 params를 삭제합니다.
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # mart 스키마의 여러 테이블을 통합하여 bi 스키마에 적재
    aggregate_to_bi = PostgresOperator(
        task_id='aggregate_to_bi',
        # Airflow Connection ID 'datalake_1' 사용
        postgres_conn_id='datalake_1',
        sql="""
        -- 매번 DAG가 실행될 때마다 테이블을 새로 생성
        DROP TABLE IF EXISTS bi.aggregated_bi_data;

        CREATE TABLE bi.aggregated_bi_data AS
        -- 상선 데이터 통합
        SELECT
            project_id,
            contract_date,
            business_unit,
            product_type,
            country,
            revenue,
            build_duration_days,
            delivery_date,
            stage_progress,
            global_freight_index,
            vessel_utilization_rate,
            weather_risk_index,
            recommended_route_score,
            NULL::INTEGER AS well_depth_meters,
            NULL::TEXT AS drilling_rig_type,
            NULL::INTEGER AS drilling_duration_days,
            NULL::INTEGER AS geopolitical_risk_index,
            NULL::BIGINT AS mro_contract_revenue,
            NULL::INTEGER AS patrol_days_per_maintenance_cycle,
            NULL::TEXT AS model_type,
            NULL::INTEGER AS fleet_size,
            latitude,
            longitude,
            product_description,
            continent,
            geo_region,
            '{{ ds }}'::date AS etl_dtm
        FROM mart.merchant_ships_clean

        UNION ALL

        -- 해양플랜트 데이터 통합
        SELECT
            project_id,
            contract_date,
            business_unit,
            product_type,
            country,
            revenue,
            build_duration_days,
            delivery_date,
            stage_progress,
            NULL::FLOAT AS global_freight_index,
            NULL::FLOAT AS vessel_utilization_rate,
            NULL::INTEGER AS weather_risk_index,
            NULL::INTEGER AS recommended_route_score,
            well_depth_meters,
            drilling_rig_type,
            drilling_duration_days,
            NULL::INTEGER AS geopolitical_risk_index,
            NULL::BIGINT AS mro_contract_revenue,
            NULL::INTEGER AS patrol_days_per_maintenance_cycle,
            NULL::TEXT AS model_type,
            NULL::INTEGER AS fleet_size,
            latitude,
            longitude,
            product_description,
            continent,
            geo_region,
            '{{ ds }}'::date AS etl_dtm
        FROM mart.offshore_plants_clean

        UNION ALL

        -- 특수선 데이터 통합
        SELECT
            project_id,
            contract_date,
            business_unit,
            product_type,
            country,
            revenue,
            build_duration_days,
            delivery_date,
            stage_progress,
            NULL::FLOAT AS global_freight_index,
            NULL::FLOAT AS vessel_utilization_rate,
            NULL::INTEGER AS weather_risk_index,
            NULL::INTEGER AS recommended_route_score,
            NULL::INTEGER AS well_depth_meters,
            NULL::TEXT AS drilling_rig_type,
            NULL::INTEGER AS drilling_duration_days,
            geopolitical_risk_index,
            mro_contract_revenue,
            patrol_days_per_maintenance_cycle,
            model_type,
            fleet_size,
            latitude,
            longitude,
            product_description,
            continent,
            geo_region,
            '{{ ds }}'::date AS etl_dtm
        FROM mart.special_ships_clean;
        """
    )

    start >> aggregate_to_bi >> end