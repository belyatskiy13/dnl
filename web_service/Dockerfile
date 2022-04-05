# syntax=docker/dockerfile:1

FROM python:3.9-slim-buster

WORKDIR /DNL/web_service

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

# ENTRYPOINT [ "python3", "-u", "web_scraper.py"]

CMD [ "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "5000"]