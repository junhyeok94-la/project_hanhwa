# **README.md**

## **🚢 한화오션 데이터/AI 플랫폼 개발자 포트폴리오**

이 프로젝트는 한화오션의 가상 비즈니스 데이터를 활용하여 데이터 파이프라인 구축부터 시각화 대시보드 개발까지 전 과정을 담은 포트폴리오입니다. 데이터/AI 플랫폼 개발자로서의 역량을 보여주기 위해 **ETL 파이프라인(Airflow)**, **데이터베이스(PostgreSQL)**, 그리고 \*\*인터랙티브 웹 대시보드(Streamlit)\*\*를 통합하여 설계했습니다.

### **🛠️ 설치 라이브러리**

이 프로젝트를 실행하기 위해 필요한 라이브러리 목록입니다. 아래 명령어를 통해 모든 의존성을 설치할 수 있습니다.

pip install pandas==2.3.1  
pip install numpy==2.3.2  
pip install streamlit==1.48.0  
pip install plotly==5.25.0  
pip install sqlalchemy==2.0.29  
pip install psycopg2-binary==2.9.9

### **🐍 파이썬 버전**

프로젝트는 **Python 3.10** 버전에서 개발 및 테스트되었습니다. 최적의 호환성을 위해 Python 3.9 이상 버전을 권장합니다.

### **🎯 목표**

* 한화오션 데이터/AI 플랫폼 개발자 이력서 제출을 위한 포트폴리오 용도  
* 데이터 생성, ETL 파이프라인, 데이터 시각화 대시보드 구축 경험을 통합적으로 보여주기

### **🏗️ 아키텍처**

* **서버**: AWS VPC 내에 단일 EC2 인스턴스 사용  
* **DB**: **PostgreSQL** (AWS RDS 사용 권장)  
* **추가 구성**:  
  * **Nginx**: 웹 서버 역할을 하며, Streamlit 앱으로의 **리버스 프록시** 설정  
  * **보안 그룹(Security Group)**: Nginx 접속을 위해 인바운드 규칙에 포트 **80** 및 **443**(HTTPS) 추가

### **⚙️ 데이터 생성 및 ETL (ETL 파이프라인)**

* **create\_data.py**: 가상의 조선업 데이터를 생성하여 shipbuilding\_data\_deep.csv 파일로 저장  
* **etl\_dag.py**: **Airflow**를 사용하여 create\_data.py를 정기적으로 실행하고, 생성된 데이터를 **PostgreSQL** 데이터베이스로 로드하는 파이프라인을 정의합니다.  
* **실행 환경**: AWS EC2 인스턴스에 설치된 Airflow 환경에서 실행됩니다. Airflow 스케줄러가 정해진 시간에 etl\_dag.py를 실행하여 데이터베이스를 최신 상태로 유지합니다.

### **📊 웹 애플리케이션 (대시보드)**

* **streamlit\_app.py**: **PostgreSQL** 데이터베이스에서 데이터를 읽어와 **Streamlit** 웹 프레임워크를 통해 시각화 대시보드를 구축합니다.  
* **실행 환경**: 이 스크립트 역시 같은 AWS EC2 인스턴스에서 streamlit run streamlit\_app.py 명령어를 통해 실행됩니다.

### **📝 프로젝트 설명**

본 프로젝트는 조선업의 세 가지 핵심 사업 부문(상선, 해양플랜트, 특수선)에 대한 심층 분석을 제공합니다. 각 부문별 데이터는 현실적인 시장 동향 및 기술적 요소를 반영하여 가상으로 생성되었으며, 이는 비즈니스 인사이트 도출의 기반이 됩니다.

* **상선 (컨테이너선)**: 글로벌 해운 운임 지수, 선박 활용률, 기상 위험 등 외부 데이터를 결합하여 운임 변동성이 매출에 미치는 영향을 분석하고, 위험 요소를 시각화합니다.  
* **해양플랜트 (드릴십)**: 시추 유정의 깊이, 소요 기간, 장비 유형 등을 분석하여 시추 효율성을 평가하고, 프로젝트의 위치를 지도에 시각화하여 지리적 특성을 파악합니다.  
* **특수선 (잠수함)**: 지정학적 리스크와 MRO(유지보수, 수리, 운영) 계약 매출 간의 상관관계를 분석하고, 국가별 잠수함 보유량과 프로젝트 수주량을 비교하여 전략적 사업 기회를 탐색합니다.

이 대시보드는 사용자가 특정 사업 부문을 선택하면 관련 KPI 및 상세 분석 차트가 동적으로 업데이트되는 인터랙티브한 경험을 제공합니다.

### **▶️ 실행 방법**

1. **AWS 환경 설정**:  
   * AWS VPC 내에 EC2 인스턴스를 생성하고, 인스턴스에 필요한 패키지(Python, Nginx 등)를 설치합니다.  
   * AWS RDS를 통해 PostgreSQL 데이터베이스를 생성하고, 연결 정보를 기록합니다.  
2. **프로젝트 파일 업로드**:  
   * 모든 프로젝트 파일(create\_data.py, etl\_dag.py, streamlit\_app.py)을 EC2 인스턴스에 업로드합니다.  
3. **데이터베이스 연결 정보 설정**:  
   * etl\_dag.py와 streamlit\_app.py 파일 내의 db\_connection\_str 변수를 실제 PostgreSQL 연결 문자열로 수정합니다.  
4. **Nginx 설정**:  
   * /etc/nginx/sites-available/ 경로에 새로운 설정 파일을 생성하고, Streamlit 앱을 위한 리버스 프록시를 설정합니다.

   server {  
         listen 80;  
         server\_name \_; \# 와일드카드 사용

         location / {  
             proxy\_pass http://localhost:8501;  
             proxy\_http\_version 1.1;  
             proxy\_set\_header Upgrade $http\_upgrade;  
             proxy\_set\_header Connection "upgrade";  
             proxy\_set\_header Host $host;  
         }  
     }

   * 설정 파일을 활성화하고 Nginx를 재시작합니다.  
5. **Airflow 설치 및 실행**:  
   * EC2 인스턴스에 Airflow를 설치하고, etl\_dag.py 파일을 Airflow DAGs 폴더에 넣습니다.  
   * Airflow 스케줄러를 실행하여 데이터 ETL 파이프라인을 가동합니다.  
6. **대시보드 실행**:  
   * streamlit\_app.py 파일이 있는 디렉토리에서 다음 명령어를 실행하여 웹 애플리케이션을 시작합니다.

   streamlit run streamlit\_app.py

   * 웹 브라우저에서 http://\[EC2 인스턴스 퍼블릭 IP\]로 접속하여 대시보드를 확인할 수 있습니다.
