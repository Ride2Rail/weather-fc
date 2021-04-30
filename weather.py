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

    # number of cities
    cities_day = dict()
    if 'offer_ids' in output_offer_level.keys():
        for offer_id in output_offer_level['offer_ids']:
            if 'triplegs' in output_tripleg_level[offer_id].keys():
                for leg_id in output_tripleg_level[offer_id]['triplegs']:
                    # city
                    track = geojson.loads(output_tripleg_level[offer_id][leg_id]['leg_stops'])
                    leg_start_coordinates = np.array(track['coordinates'][0])
                    city_name = get_city(leg_start_coordinates[0], leg_start_coordinates[1])
                    # date
                    leg_time = datetime.fromisoformat(output_tripleg_level[offer_id][leg_id]['start_time'])
                    date = str(leg_time.date())
                    dict_key = '{city},{date}'.format(city=city_name, date=date)
                    cities_day.setdefault(dict_key, [])
                    cities_day[dict_key].append([offer_id, leg_id])
    print(cities_day)

    prob_delay = dict()
    # current_time = datetime.now()
    current_time = datetime.fromisoformat('2019-09-20T12:31:00')
    for elements in cities_day.items():
        # get offer_id and leg_id of just the first element of each city and date
        offer_key = elements[1][0]
        offer_id = offer_key[0]
        leg_id = offer_key[1]

        # time
        leg_time = datetime.fromisoformat(output_tripleg_level[offer_id][leg_id]['start_time'])

        # location
        track = geojson.loads(output_tripleg_level[offer_id][leg_id]['leg_stops'])
        leg_coordinates = np.array(track['coordinates'][0])

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

        trip_scenarios = map_weather_scenarios(cat_clouds, cat_precipitation, cat_wind, cat_temperature)
        print(trip_scenarios)

        # probability of delay
        trip_extreme_conditions = extreme_condition(trip_scenarios)
        city_date_delay = probability_delay(trip_extreme_conditions)

        # probability of each offer
        for ids in elements[1]:
            offerid = ids[0]
            legid = ids[1]
            prob_delay.setdefault(offerid, {})
            prob_delay[offerid].setdefault(legid, city_date_delay)

    # get the maximum probability of delay of each offer
    for offer in prob_delay.items():
        print(offer[0], offer[1][max(offer[1], key=offer[1].get)])

    # print(prob_delay)
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
