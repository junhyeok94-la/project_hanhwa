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
    tags=['hanwha_ocean', 'mart'],
    doc_md="""
    ## MART (Data Mart) 데이터 파이프라인
    - 이 DAG는 `stg` 스키마의 데이터를 정제하고, `mart` 스키마에 적재합니다.
    - 비즈니스 도메인별로 개별 테이블을 생성합니다.
    - 마스터 데이터와 조인하여 상세 정보를 추가합니다.
    """,
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # 상선 데이터 정제 및 mart 스키마에 적재
    transform_merchant_ships = PostgresOperator(
        task_id='transform_merchant_ships',
        postgres_conn_id='datalake_1',
        sql="""
        DROP TABLE IF EXISTS mart.merchant_ships_clean;

        CREATE TABLE mart.merchant_ships_clean AS
        SELECT
            ms.project_id,
            ms.contract_date,
            ms.business_unit,
            ms.product_type,
            ms.country,
            CAST(ms.revenue AS BIGINT) AS revenue,
            CAST(ms.build_duration_days AS INTEGER) AS build_duration_days,
            ms.delivery_date,
            ms.stage_progress,
            ms.global_freight_index,
            ms.vessel_utilization_rate,
            ms.weather_risk_index,
            ms.recommended_route_score,
            pm.product_description,
            cm.continent,
            cm.geo_region,
            ms.latitude,
            ms.longitude,
            '{{ ds }}'::date AS etl_dtm
        FROM stg.merchant_ships_stg AS ms
        LEFT JOIN stg.product_master_stg AS pm ON ms.product_type = pm.product_type
        LEFT JOIN stg.country_master_stg AS cm ON ms.country = cm.country;
        """,
    )

    # 해양플랜트 데이터 정제 및 mart 스키마에 적재
    transform_offshore_plants = PostgresOperator(
        task_id='transform_offshore_plants',
        postgres_conn_id='datalake_1',
        sql="""
        DROP TABLE IF EXISTS mart.offshore_plants_clean;

        CREATE TABLE mart.offshore_plants_clean AS
        SELECT
            op.project_id,
            op.contract_date,
            op.business_unit,
            op.product_type,
            op.country,
            CAST(op.revenue AS BIGINT) AS revenue,
            CAST(op.build_duration_days AS INTEGER) AS build_duration_days,
            op.delivery_date,
            op.stage_progress,
            op.well_depth_meters,
            op.drilling_rig_type,
            op.drilling_duration_days,
            pm.product_description,
            cm.continent,
            cm.geo_region,
            op.latitude,
            op.longitude,
            '{{ ds }}'::date AS etl_dtm
        FROM stg.offshore_plants_stg AS op
        LEFT JOIN stg.product_master_stg AS pm ON op.product_type = pm.product_type
        LEFT JOIN stg.country_master_stg AS cm ON op.country = cm.country;
        """,
    )

    # 특수선 데이터 정제 및 mart 스키마에 적재
    transform_special_ships = PostgresOperator(
        task_id='transform_special_ships',
        postgres_conn_id='datalake_1',
        sql="""
        DROP TABLE IF EXISTS mart.special_ships_clean;

        CREATE TABLE mart.special_ships_clean AS
        SELECT
            ss.project_id,
            ss.contract_date,
            ss.business_unit,
            ss.product_type,
            ss.country,
            CAST(ss.revenue AS BIGINT) AS revenue,
            CAST(ss.build_duration_days AS INTEGER) AS build_duration_days,
            ss.delivery_date,
            ss.stage_progress,
            ss.geopolitical_risk_index,
            ss.mro_contract_revenue,
            ss.patrol_days_per_maintenance_cycle,
            ss.model_type,
            ss.fleet_size,
            pm.product_description,
            cm.continent,
            cm.geo_region,
            ss.latitude,
            ss.longitude,
            '{{ ds }}'::date AS etl_dtm
        FROM stg.special_ships_stg AS ss
        LEFT JOIN stg.product_master_stg AS pm ON ss.product_type = pm.product_type
        LEFT JOIN stg.country_master_stg AS cm ON ss.country = cm.country;
        """,
    )

    start >> [
        transform_merchant_ships,
        transform_offshore_plants,
        transform_special_ships
    ] >> end