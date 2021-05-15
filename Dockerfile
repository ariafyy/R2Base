FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7
WORKDIR /app

COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
RUN python -m spacy download en_core_web_md 

COPY . .

# make model directory and download model files
ENV LANG C.UTF-8

CMD ["uvicorn", "r2base.http.server:app", "--host", "0.0.0.0", "--port", "8000",  "--workers", "4"]
