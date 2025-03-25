FROM python:3.12-alpine
WORKDIR /code
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
EXPOSE 5000

RUN apk update && apk add opencv cmake
RUN apk add onnxruntime --repository=https://dl-cdn.alpinelinux.org/alpine/edge/testing

RUN python3 -m pip install -U pip

COPY requirements.txt requirements.txt
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY app .
CMD ["flask", "run", "--debug"]
