FROM python:3.12.3-slim

WORKDIR /app

# 필요한 패키지 설치
COPY requirements.txt .

# pip를 최신 버전으로 업데이트하는 명령어 추가
RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

# Streamlit 앱 코드 및 로고 이미지 복사
COPY streamlit_app.py .
COPY Hanwha_Ocean_Logo.png .

# Streamlit 앱 실행 명령어
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
