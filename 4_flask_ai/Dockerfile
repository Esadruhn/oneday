FROM python:3.12-slim
WORKDIR /code
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
EXPOSE 5000

RUN python3 -m pip install -U pip

COPY requirements.txt requirements.txt
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY app .
ENTRYPOINT ["tail", "-f", "/dev/null"]
#CMD ["flask", "--app", "app.py", "run"]
