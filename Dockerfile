FROM python:3.8.2-buster

COPY requirements.txt /workspace/
WORKDIR /workspace
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY . /workspace

EXPOSE 80
CMD gunicorn -w 1 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:80 server.app:app
