import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def create_dummy_data(start_date: datetime, end_date: datetime) -> dict:
    """
    주어진 기간(start_date ~ end_date) 내에서 Dummy 데이터를 생성하여 DataFrame으로 반환합니다.
    데이터 스키마를 etl_pipeline_stg, mart, bi와 일치시킵니다.
    """
    try:
        main_data = []
        product_master_data = []
        country_master_data = []
        
        # 날짜 범위의 총 일수 계산
        days_diff = (end_date - start_date).days
        
        # 코드성 마스터 테이블 데이터
        product_map = {
            'merchant_ships': ['LNG운반선', '컨테이너선', 'VLCC'],
            'offshore_plants': ['LNG-FPSO', '해상풍력설비', '부유식 생산설비'],
            'special_ships': ['잠수함', '구축함', '해양경비함']
        }
        country_map = {
            'merchant_ships': ['대한민국', '그리스', '영국', '일본', '싱가포르'],
            'offshore_plants': ['미국', '호주', '노르웨이', '캐나다'],
            'special_ships': ['대한민국', '호주', '필리핀', '인도']
        }
        
        # 마스터 테이블 생성
        for bu, products in product_map.items():
            for product in products:
                product_master_data.append({
                    'product_type': product,
                    'business_unit': bu,
                    'product_description': f"{product}에 대한 상세 설명"
                })
                
        for bu, countries in country_map.items():
            for country in countries:
                country_master_data.append({
                    'country': country,
                    'continent': '아시아', # 더미 대륙 정보 추가
                    'geo_region': '동아시아' # 더미 지역 정보 추가
                })
        
        # 1000개의 더미 데이터 생성
        for i in range(1000):
            project_id = f'PROJ-{1000+i}'
            # 'contract_date'를 주어진 날짜 범위 내에서 랜덤하게 선택
            contract_date = start_date + timedelta(days=random.randint(0, days_diff))
            
            # 상선(merchant_ships), 해양플랜트(offshore_plants), 특수선(special_ships)
            business_unit = random.choice(list(product_map.keys()))
            
            product_type = random.choice(product_map[business_unit])
            country = random.choice(country_map[business_unit])
            
            # 공통 컬럼
            revenue = random.randint(100, 2000) * 1000000
            build_duration_days = random.randint(300, 1500)
            delivery_date = contract_date + timedelta(days=build_duration_days)
            stage_progress = random.uniform(0, 1)
            latitude = random.uniform(-90, 90)
            longitude = random.uniform(-180, 180)

            record = {
                'project_id': project_id,
                'contract_date': contract_date,
                'business_unit': business_unit,
                'product_type': product_type,
                'country': country,
                'revenue': revenue,
                'build_duration_days': build_duration_days,
                'delivery_date': delivery_date,
                'stage_progress': stage_progress,
                'latitude': latitude,
                'longitude': longitude,
                'global_freight_index': np.nan,
                'vessel_utilization_rate': np.nan,
                'weather_risk_index': np.nan,
                'recommended_route_score': np.nan,
                'well_depth_meters': np.nan,
                'drilling_rig_type': np.nan,
                'drilling_duration_days': np.nan,
                'geopolitical_risk_index': np.nan,
                'mro_contract_revenue': np.nan,
                'patrol_days_per_maintenance_cycle': np.nan,
                'model_type': np.nan,
                'fleet_size': np.nan
            }

            if business_unit == 'merchant_ships':
                record.update({
                    'global_freight_index': random.uniform(1000, 5000),
                    'vessel_utilization_rate': random.uniform(0.7, 0.99),
                    'weather_risk_index': random.randint(1, 10),
                    'recommended_route_score': random.randint(1, 100)
                })
            
            elif business_unit == 'offshore_plants':
                record.update({
                    'well_depth_meters': random.randint(100, 3000),
                    'drilling_rig_type': random.choice(['잭업리그', '반잠수식']),
                    'drilling_duration_days': random.randint(50, 200)
                })
            
            elif business_unit == 'special_ships':
                record.update({
                    'geopolitical_risk_index': random.randint(1, 10),
                    'mro_contract_revenue': random.randint(10, 50) * 1000000,
                    'patrol_days_per_maintenance_cycle': random.randint(30, 90),
                    'model_type': random.choice(['Type-I', 'Type-II', 'Type-III']),
                    'fleet_size': random.randint(1, 100)
                })

            main_data.append(record)

        main_df = pd.DataFrame(main_data)
        product_master_df = pd.DataFrame(product_master_data).drop_duplicates()
        country_master_df = pd.DataFrame(country_master_data).drop_duplicates()
        
        # 데이터프레임의 칼럼 순서 재정렬
        column_order = [
            'project_id', 'contract_date', 'business_unit', 'product_type', 'country', 'revenue',
            'build_duration_days', 'delivery_date', 'stage_progress',
            'global_freight_index', 'vessel_utilization_rate', 'weather_risk_index', 'recommended_route_score',
            'well_depth_meters', 'drilling_rig_type', 'drilling_duration_days',
            'geopolitical_risk_index', 'mro_contract_revenue', 'patrol_days_per_maintenance_cycle', 'model_type', 'fleet_size',
            'latitude', 'longitude'
        ]
        
        main_df = main_df[column_order]


        # 새로운 로그성 데이터 리스트 추가
        movement_logs_data = []

        # 각 프로젝트 ID별로 이동 경로 더미 데이터 생성
        for project_id in main_df['project_id'].unique():
            # 임의의 이동 횟수 (10 ~ 100회)
            num_logs = random.randint(10, 100)
            
            # 임의의 시작점
            current_lat = random.uniform(-90, 90)
            current_lon = random.uniform(-180, 180)
            
            # 시작 시간
            current_time = start_date

            for i in range(num_logs):
                # 다음 위치는 이전 위치에서 약간씩 이동
                current_lat += random.uniform(-5, 5)
                current_lon += random.uniform(-5, 5)

                # 시간 증가
                current_time += timedelta(minutes=random.randint(10, 60))

                movement_logs_data.append({
                    'log_id': f'LOG-{project_id}-{i}',
                    'project_id': project_id,
                    'log_datetime': current_time,
                    'latitude': current_lat,
                    'longitude': current_lon,
                    'speed_knots': random.uniform(5, 30),
                    'status': random.choice(['운항 중', '정박', '대기 중'])
                })
        
        movement_logs_df = pd.DataFrame(movement_logs_data)

        return {
            'main_data': main_df,
            'product_master': product_master_df,
            'country_master': country_master_df,
            'movement_logs': movement_logs_df  # 새로운 데이터프레임 추가
        }
        
    except Exception as e:
        print(f"Error creating dummy data: {e}")
        raise
