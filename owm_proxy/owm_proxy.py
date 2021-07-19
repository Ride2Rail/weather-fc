from flask import Flask, request
import configparser as cp
import logging
import os
from datetime import datetime, timedelta
import random
import requests, json

from r2r_offer_utils.logging import setup_logger

service_name = os.path.splitext(os.path.basename(__file__))[0]
app = Flask(service_name)

# config
config = cp.ConfigParser()
config.read(f'{service_name}.conf')

# init logging
logger, ch = setup_logger()
logger.setLevel(logging.INFO)


@app.route('/compute', methods=['POST', 'GET'])
def handle_date():

    data_from_weather_fc = request.get_json()
    print(data_from_weather_fc, flush=True)

    current_time = datetime.fromisoformat(data_from_weather_fc['current_time'])
    leg_time = datetime.fromisoformat(data_from_weather_fc['leg_time'])
    leg_coordinates = [data_from_weather_fc['leg_coordinate_x'], 
                       data_from_weather_fc['leg_coordinate_y']]
    api_key = data_from_weather_fc['api_key']
    execution_mode = data_from_weather_fc['execution_mode']

    # decide to use hourly or daily data
    days_until_start_time = int((leg_time - current_time).total_seconds()//86400)
    hours_until_start_time = int((leg_time - current_time).total_seconds()//3600)
    logger.info(f'Current time: {current_time}')
    logger.info(f'Leg time: {leg_time}')
    logger.info(f'Days until start time: {days_until_start_time}')
    logger.info(f'Hours until start time: {hours_until_start_time}')

    logger.info(f'Execution mode: {execution_mode}')
    if execution_mode == 'TEST' and (days_until_start_time > 7 or days_until_start_time < -5):
        days_delta = random.randint(-4, 6)
        hours_delta = random.randint(-23, 23)
        leg_time = current_time + timedelta(days=days_delta, hours=hours_delta)
        days_until_start_time = int((leg_time - current_time).total_seconds()//86400)
        hours_until_start_time = int((leg_time - current_time).total_seconds()//3600)
        logger.info(f'*** The leg time has been changed for testing. ***')
        logger.info(f'Current time: {current_time}')
        logger.info(f'Leg time: {leg_time}')
        logger.info(f'Days until start time: {days_until_start_time}')
        logger.info(f'Hours until start time: {hours_until_start_time}')
    elif days_until_start_time > 7:
        logger.info("Date provided is not within the next 7 days (in the future). Weather data is not available.")
        raise Exception
    elif days_until_start_time < -5:
        logger.info("Date provided is not within the last 5 days (in the past). Weather data is not available.")
        raise Exception
    
    # fetch forecast for future day
    if hours_until_start_time > 0:
        # data from API
        url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&exclude=minutely" \
            "&units=metric" % (leg_coordinates[0], leg_coordinates[1], api_key)
    # fetch weather for past day
    else:
        url = "https://api.openweathermap.org/data/2.5/onecall/timemachine?lat=%s&lon=%s&dt=%s&appid=%s&exclude=minutely" \
                "&units=metric" % (leg_coordinates[0], leg_coordinates[1], int(leg_time.timestamp()), api_key)

    response_api = requests.get(url).text
    data = json.loads(response_api)

    if hours_until_start_time < 0:
        #if 'current' in data:
        data_trip = data['current']
        #else:
        #    logger.info("Date provided is not within the last 5 days (in the past). Weather data is not available.")
        #    raise Exception
    else:
    # this is the case, theoretically possible, in which a user sends a request for a time in the past
        #try:
        if hours_until_start_time < 48:
            data_trip = data['hourly'][hours_until_start_time]
        else:
            data_trip = data['daily'][days_until_start_time]
        #except IndexError as e:
        #    logger.info("Date provided is not within the next 7 days (in the future). Weather data is not available.")
        #    raise Exception
    
    if type(data_trip['feels_like']) is dict:
        if leg_time.hour >= 6 and leg_time.hour < 12:
            data_trip['feels_like'] = data_trip['feels_like']['morn']
        elif leg_time.hour >= 12 and leg_time.hour < 18:
            data_trip['feels_like'] = data_trip['feels_like']['day']
        elif leg_time.hour >= 18 and leg_time.hour < 24:
            data_trip['feels_like'] = data_trip['feels_like']['eve']
        else:
            data_trip['feels_like'] = data_trip['feels_like']['night']
    
    return data_trip


if __name__ == '__main__':
    
    FLASK_PORT = 5000
    os.environ["FLASK_ENV"] = "development"
    app.run(port=FLASK_PORT, debug=True)
