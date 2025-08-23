# **조선업 실적 데이터 분석 및 시각화 프로젝트**

## **프로젝트 목적**

본 프로젝트는 가상의 조선업 데이터셋을 생성하여 사업 실적 데이터를 효율적으로 수집, 가공, 분석하고, 이를 직관적으로 시각화하여 비즈니스 의사결정을 지원하는 End-to-End 데이터 파이프라인 구축을 목표로 합니다.

* **데이터 거버넌스 확보**: 원천 데이터의 무결성을 유지하면서 비즈니스 분석 목적에 맞게 정제된 데이터를 제공합니다.  
* **자동화된 파이프라인**: Apache Airflow를 활용하여 데이터 처리 과정을 자동화하고, 모니터링 및 재처리를 용이하게 합니다.  
* **통합 분석 환경 구축**: 다양한 비즈니스 단위(상선, 해양플랜트, 특수선)의 데이터를 통합하여 일관된 시각화 대시보드를 구축합니다.  
* **비즈니스 인사이트 도출**: 건조 진행 현황, 매출 분석, 계약 국가별 실적 등 핵심 지표를 실시간으로 확인하여 전략 수립에 활용합니다.

## **시스템 아키텍처**

본 프로젝트는 AWS 클라우드 환경에 Docker Compose를 활용하여 구축되었습니다.

* **Orchestration (Apache Airflow)**:  
  * **airflow-scheduler**: DAG(Directed Acyclic Graph)를 스케줄링하고, 태스크를 Celery 큐에 보냅니다.  
  * **airflow-worker**: CeleryExecutor를 통해 큐에 있는 태스크를 병렬로 처리합니다.  
  * **airflow-webserver**: Airflow 웹 UI를 제공하여 DAG를 모니터링하고 관리합니다.  
* **Database (PostgreSQL, RDS(PostgreSQL))**:  
  * **Postgres**: Airflow의 메타데이터 및 ETL 파이프라인의 최종 데이터 웨어하우스 역할을 수행합니다.  
* **Message Queue (Redis)**:  
  * **Redis**: Airflow의 CeleryExecutor를 위한 메시지 브로커로, 태스크의 분산 처리를 가능하게 합니다.  
* **Data Visualization (Streamlit)**:  
  * **streamlit-app**: bi 스키마에 저장된 데이터를 조회하여 직관적인 대시보드를 렌더링합니다.  
* **Web Server (Nginx)**:  
  * **nginx**: Streamlit 앱의 프록시 서버 역할을 하며, 외부 트래픽을 처리하고 웹소켓 연결을 지원합니다.  
* **Cloud Infrastructure (AWS)**:  
  * **EC2**: Docker 컨테이너를 호스팅하는 가상 머신입니다.  
  * **RDS**: Airflow 메타데이터의 경우, EC2 내의 PostgreSQL 컨테이너를 사용했지만, 데이터레이크는 관리형 데이터베이스 서비스인 RDS를 활용하여 안정성을 확보할 수 있습니다.  
  * **EBS/S3**: 데이터 및 로그 저장을 위한 스토리지로, 본 프로젝트에서는 Docker Volume을 통해 가상 스토리지를 사용합니다.

## **데이터 모델링 및 ETL 파이프라인 설계**

데이터 파이프라인은 Staging(STG) → Data Mart(MART) → Business Intelligence(BI)의 3단계로 구성된 **데이터 웨어하우스 아키텍처**를 따릅니다.

### **1\. STG (Staging)**

* **목적**: 원본 데이터를 가공 없이 임시로 적재하는 영역입니다.  
* **etl\_pipeline\_stg.py**:  
  * create\_data.py에서 생성된 더미 데이터를 Pandas DataFrame으로 읽어옵니다.  
  * business\_unit별(상선, 해양플랜트, 특수선)로 데이터를 분할하여 STG 스키마에 개별 테이블로 저장합니다.  
  * 향후 마스터 데이터와 조인하기 위한 **product\_master\_stg** 및 **country\_master\_stg** 테이블도 함께 적재합니다.

### **2\. MART (Data Mart)**

* **목적**: 비즈니스 도메인별로 데이터를 정제하고 비즈니스 로직을 적용하는 영역입니다.  
* **etl\_pipeline\_mart.py**:  
  * STG 스키마의 각 테이블(merchant\_ships\_stg, offshore\_plants\_stg, special\_ships\_stg)을 읽습니다.  
  * product\_master\_stg 및 country\_master\_stg 테이블과 LEFT JOIN하여 제품 설명(product\_description), 대륙(continent), 지역(geo\_region)과 같은 상세 정보 필드를 추가합니다.  
  * 데이터 타입을 CAST하여 일관성을 확보하고, 최종적으로 mart 스키마에 \*\_clean 테이블로 저장합니다.  
  * **개선점**: 원시 데이터와 마스터 데이터 간의 조인을 통해 데이터 품질을 향상시키고, 최종 사용자가 복잡한 조인 없이도 의미 있는 분석을 할 수 있도록 **스타 스키마(Star Schema)** 형태의 데이터 모델을 구축했습니다.

### **3\. BI (Business Intelligence)**

* **목적**: 대시보드 및 리포팅 용도로 최적화된 통합 테이블을 제공합니다.  
* **etl\_pipeline\_bi.py**:  
  * MART 스키마의 모든 정제된 테이블을 UNION ALL 연산으로 하나로 통합합니다.  
  * 각 테이블에 특화된 필드들은 NULL 값으로 채워 컬럼을 통일하고, 최종적으로 bi 스키마의 aggregated\_bi\_data 테이블에 적재합니다.  
  * **개선점**: streamlit-app.py가 여러 테이블에 복잡한 쿼리를 날리는 대신, 단일 통합 테이블을 조회하도록 설계하여 **대시보드의 쿼리 성능을 극대화**했습니다.

## **시각화 결과 및 기술적 개선사항**

### **시각화 대시보드 (streamlit\_app.py)**

* streamlit-app.py는 aggregated\_bi\_data 테이블에 직접 연결하여 실시간 데이터를 불러옵니다.  
* **매출 현황**: 사업 단위별 매출 및 계약 연도별 추이를 Plotly 차트로 시각화하여 사업 부문별 성과를 한눈에 파악합니다.  
* **건조 진행도**: 현재 진행 중인 프로젝트의 진척도를 상세 테이블로 제공하여 리소스 배분을 최적화합니다.  
* **해양플랜트 위치**: 계약 국가 및 위/경도 데이터를 Folium 맵에 시각화하여 전 세계 사업 현황을 지리적으로 분석합니다.  
* **개선점**: @st.cache\_data와 @st.cache\_resource를 활용하여 데이터베이스 연결 및 데이터 로딩을 캐싱함으로써 **대시보드의 로딩 속도를 개선**했습니다.

### **기술적 개선사항**

* **Docker 컨테이너 오케스트레이션**: docker-compose를 사용하여 Airflow 클러스터, PostgreSQL, Redis, Streamlit, Nginx 등 복잡한 다중 컨테이너 환경을 간편하게 구축했습니다.  
* **성능 개선 (Celery)**: EC2 자원이 작아 초기 Airflow Worker가 높은 기본 동시성 설정(concurrency=16)으로 인해 시스템 리소스(특히 메모리)를 과도하게 소모하여 성능 저하 문제가 발생했습니다. **AIRFLOW\_\_CELERY\_\_WORKER\_CONCURRENCY 환경 변수를 2로 조정**하여 리소스 사용량을 안정화하고 태스크 처리량을 개선했습니다.   
  후에는 워커의 프로세스별로 리소스 제한 옵션을 적용하여 프로세스 별 메모리 사용량 제한 값을 넘어설 경우 process recycle 하도록 적용할 예정입니다.
* **안정성 확보 (Health Check)**: 각 서비스(PostgreSQL, Redis, Airflow)에 healthcheck를 추가하여 컨테이너의 상태를 지속적으로 모니터링하고, 서비스 간의 올바른 시작 순서를 보장했습니다. 특히 airflow-init 서비스가 PostgreSQL의 준비 상태를 확인한 후에만 데이터베이스 초기화를 진행하도록 하여 컨테이너 시작 실패 문제를 근본적으로 해결했습니다.

## **실행 방법**

1. **대시보드 접속**: 웹 브라우저에서 아래 URL로 접속하여 Streamlit 대시보드를 확인합니다. (Nginx 설정에 따라 포트가 다를 수 있습니다.)  
   * http://3.36.22.226:8501  
2. **Airflow 접속**: 웹 브라우저에서 아래 URL로 접속하여 Airflow 웹 UI에서 DAG를 관리합니다. (id: airflow / pw: airflow )
   * http://3.36.22.226:8080
