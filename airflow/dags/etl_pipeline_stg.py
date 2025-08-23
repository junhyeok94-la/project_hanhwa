from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Asia/Seoul")
now = datetime.now(local_tz)
end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(days=1)

def create_and_load_data_to_stg(**kwargs):
    """
    create_data.py를 실행하여 데이터를 생성하고 STG 스키마에 로드합니다.
    kwargs를 통해 DAG의 파라미터(날짜)를 가져와 데이터 생성에 사용합니다.
    """
    try:
        from create_data import create_dummy_data

        # DAG의 파라미터에서 날짜 정보 추출
        start_date_str = kwargs['params']['start_date_param']
        end_date_str = kwargs['params']['end_date_param']
        
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
        
        # 날짜 파라미터를 create_dummy_data 함수에 전달
        dummy_data = create_dummy_data(start_date, end_date)
        
        # Airflow Connection ID 'datalake_1' 사용
        hook = PostgresHook(postgres_conn_id='datalake_1')
        db_connection_str = hook.get_uri()

        engine = create_engine(db_connection_str)

        # 'business_unit' 칼럼의 고유 값들을 기준으로 여러 개의 테이블에 데이터 적재
        main_df = dummy_data['main_data']
        for business_unit in main_df['business_unit'].unique():
            filtered_df = main_df[main_df['business_unit'] == business_unit].copy()
            
            # 테이블 이름 동적 설정
            table_name = f"{business_unit.replace(' ', '_')}_stg".lower()
            
            # STG 스키마에 테이블 생성 및 데이터 적재
            filtered_df.to_sql(
                table_name, 
                engine, 
                schema='stg', 
                if_exists='replace', 
                index=False
            )
            print(f"Data loaded to STG schema for {business_unit} in table '{table_name}' successfully.")

        # 마스터 테이블 데이터 적재
        product_master_df = dummy_data['product_master']
        product_master_df.to_sql(
            'product_master_stg',
            engine,
            schema='stg',
            if_exists='replace',
            index=False
        )
        print("Product master data loaded to 'stg.product_master_stg' successfully.")

        country_master_df = dummy_data['country_master']
        country_master_df.to_sql(
            'country_master_stg',
            engine,
            schema='stg',
            if_exists='replace',
            index=False
        )
        print("Country master data loaded to 'stg.country_master_stg' successfully.")

        # 새로 추가된 로그 데이터 적재
        movement_logs_df = dummy_data['movement_logs']
        movement_logs_df.to_sql(
            'movement_logs_stg',
            engine,
            schema='stg',
            if_exists='replace',
            index=False
        )
        print("Movement logs data loaded to 'stg.movement_logs_stg' successfully.")
        
        engine.dispose()

    except Exception as e:
        print(f"Error during data creation and loading: {e}")
        raise

# DAG 파일 이름을 기준으로 dag_id를 동적으로 설정
DAG_FILE_NAME = os.path.basename(__file__).replace('.py', '')

with DAG(
    dag_id=DAG_FILE_NAME,
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    tags=['hanwha_ocean', 'stg'],
    doc_md="""
    ## STG (Staging) 데이터 파이프라인
    - 이 DAG는 원본 데이터를 생성하고, `stg` 스키마에 적재합니다.
    - 데이터 웨어하우스의 첫 번째 단계이며, 비즈니스 단위별로 개별 테이블을 생성합니다.
    - 마스터 데이터를 포함합니다.
    """,
    params={
        'start_date_param': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'end_date_param': end_time.strftime('%Y-%m-%d %H:%M:%S')
    },
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    create_and_load_task = PythonOperator(
        task_id='create_and_load_data',
        python_callable=create_and_load_data_to_stg,
        provide_context=True
    )

    start >> create_and_load_task >> end
    