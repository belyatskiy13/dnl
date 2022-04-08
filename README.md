## Description

THe project is mysql database and an api to query the database.

## Project structure
    .
    ├── web_service                 # Web service. Python part of the project
    │   ├── logger.py               # A Logger class. Just for info messages
    │   ├── database.py             # A class to connect and manage database
    │   ├── web_scraper.py          # A module to srape date
    │   ├── api.py                  # An api to query database
    │   ├── mysql.ini               # Mysql database config
    │   ├── requirements.txt
    │   └── Dockerfile
    ├── docker-compose.yml
    ├── .gitignore
    └── README.md

## Usage

To run whole project:
``` bash
docker-compose up --build
```
