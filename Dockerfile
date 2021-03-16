FROM python:3.8

COPY weather.py /home/weather/weather.py

ENTRYPOINT ['weather.py']