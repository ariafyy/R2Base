FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7
WORKDIR /app

COPY ./requirements.txt /app
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt 

COPY . .

# make model directory and download model files
ENV LANG C.UTF-8

CMD ["uvicorn", "r2base.http.server:app", "--host", "0.0.0.0", "--port", "8000",  "--workers", "4"]