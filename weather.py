#!/usr/bin/env python3

import os
import configparser as cp

import redis
from flask import Flask, request

from r2r_offer_utils.logging import setup_logger
from mapping.cache_operations import extract_data_from_cache

from mapping.functions import *

from datetime import datetime
import geojson
import numpy as np
import json
import requests

service_name = os.path.splitext(os.path.basename(__file__))[0]
app = Flask(service_name)

# config
config = cp.ConfigParser()
config.read(f'{service_name}.conf')

# logging
logger, ch = setup_logger()

# cache
cache = redis.Redis(host=config.get('cache', 'host'),
                    port=config.get('cache', 'port'))

# API
api_key = config.get('openweatherAPI', 'key')


@app.route('/compute', methods=['POST'])
def extract():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    print(offer_data)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

    output_offer_level, output_tripleg_level = extract_data_from_cache(pa_cache=cache, pa_request_id=request_id,
                                                                       pa_offer_level_items=[],
                                                                       pa_tripleg_level_items=['start_time', 'end_time',
                                                                                               'leg_stops'])
    if 'offer_ids' in output_offer_level.keys():
        for offer in output_offer_level['offer_ids']:
            if 'triplegs' in output_tripleg_level[offer].keys():
                num_legs = len(output_tripleg_level[offer]['triplegs'])
                leg = output_tripleg_level[offer]['triplegs'][(num_legs - 1) // 2]
                print(leg)
                # for leg in output_tripleg_level[offer]['triplegs']:
                # time
                start_time = datetime.fromisoformat(output_tripleg_level[offer][leg]['start_time'])
                end_time = datetime.fromisoformat(output_tripleg_level[offer][leg]['end_time'])
                leg_time = start_time + (end_time - start_time) / 2
                print(leg_time.isoformat())
                # current_time = datetime.now()
                current_time = datetime.fromisoformat('2019-09-20T12:31:00')

                # location
                track = geojson.loads(output_tripleg_level[offer][leg]['leg_stops'])
                start_coordinates = np.array(track['coordinates'][0])
                end_coordinates = np.array(track['coordinates'][-1])
                # print(start_coordinates, end_coordinates)
                leg_coordinates = (end_coordinates + start_coordinates) / 2
                print(leg_coordinates[0], leg_coordinates[1])

                # data from API
                url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&exclude=minutely" \
                      "&units=metric" % (leg_coordinates[0], leg_coordinates[1], api_key)
                response_api = requests.get(url).text
                data = json.loads(response_api)

                # decide to use hourly or daily data
                days_until_start_time = (leg_time - current_time).days
                seconds_until_start_time = (leg_time - current_time).seconds
                hours_until_start_time = days_until_start_time * 24 + seconds_until_start_time // 3600
                if hours_until_start_time < 48:
                    data_trip = data['hourly'][hours_until_start_time]
                else:
                    data_trip = data['daily'][days_until_start_time]

                # categorization
                cat_temperature, main_temperature = map_temperature_category(data_trip['feels_like'])
                cat_clouds, desc_clouds = map_cloud_category(data_trip['clouds'])
                cat_precipitation, main_precipitation = map_precipitation_category(check_rain_snow(data_trip))
                cat_wind, desc_wind, num_wind = map_wind_category(data_trip['wind_speed'])

                # match_clouds = match_scenarios("clouds", cat_clouds)
                # match_precipitation = match_scenarios("precipitation", cat_precipitation)
                # match_wind = match_scenarios("wind", cat_wind)
                # match_temperature = match_scenarios("temperature", cat_temperature)

                # print(match_clouds)
                # print(match_precipitation)
                # print(match_wind)
                # print(match_temperature)

                categories = map_weather_scenarios(cat_clouds, cat_precipitation, cat_wind, cat_temperature)
                print(categories)

    # normalization.zscore(...)

    return response


if __name__ == '__main__':
    import argparse
    import logging
    from r2r_offer_utils.cli_utils import IntRange

    FLASK_PORT = 5000
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-host',
                        default=REDIS_HOST,
                        help=f'Redis hostname [default: {REDIS_HOST}].')
    parser.add_argument('--redis-port',
                        default=REDIS_PORT,
                        type=IntRange(1, 65536),
                        help=f'Redis port [default: {REDIS_PORT}].')
    parser.add_argument('--flask-port',
                        default=FLASK_PORT,
                        type=IntRange(1, 65536),
                        help=f'Flask port [default: {FLASK_PORT}].')

    # remove default logger
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    # create file handler which logs debug messages
    fh = logging.FileHandler(f"{service_name}.log", mode='a+')
    fh.setLevel(logging.DEBUG)

    # set logging level to debug
    ch.setLevel(logging.DEBUG)

    os.environ["FLASK_ENV"] = "development"

    cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    app.run(port=FLASK_PORT, debug=True)

    exit(0)
