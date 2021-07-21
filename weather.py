#!/usr/bin/env python3

import os
import configparser as cp

import redis
from flask import Flask, request

from r2r_offer_utils.logging import setup_logger
from r2r_offer_utils.cache_operations import read_data_from_cache_wrapper, store_simple_data_to_cache_wrapper
from r2r_offer_utils.normalization import zscore, minmaxscore

from mapping.functions import *

from datetime import datetime, timezone
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
                    port=config.get('cache', 'port'),
                    decode_responses=True)

# API
api_key = config.get('openweatherAPI', 'key')

score = config.get('running', 'scores')
execution_mode = config.get('running', 'mode')

@app.route('/compute', methods=['POST'])
def extract():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    # print(offer_data)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

    output_offer_level, output_tripleg_level = read_data_from_cache_wrapper(pa_cache=cache, pa_request_id=request_id,
                                                                            pa_offer_level_items=[],
                                                                            pa_tripleg_level_items=['start_time',
                                                                                                    'end_time',
                                                                                                    'leg_stops'])                                                                                         
    # save in a dict the offers. The keys are the different city-date pairs
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
                    leg_time_string = output_tripleg_level[offer_id][leg_id]["start_time"]
                    try:
                        leg_time = datetime.fromisoformat(leg_time_string)
                    except ValueError:
                        # this is to handle an error in the formatting of the time string in some TRIAS files
                        leg_time_string = leg_time_string[:leg_time_string.index('+')] + '0' + leg_time_string[leg_time_string.index('+'):]
                        leg_time = datetime.fromisoformat(leg_time_string)
                    
                    date = str(leg_time.date())
                    dict_key = '{city},{date}'.format(city=city_name, date=date)
                    cities_day.setdefault(dict_key, [])
                    cities_day[dict_key].append([offer_id, leg_id])

    # get the time zone from one of the leg_times, or else default it to UTC
    try:
        time_zone = leg_time.tzinfo
    except:
        time_zone = timezone.utc
    current_time = datetime.now(tz=time_zone)

    prob_delay = dict()
    for elements in cities_day.items():
        # get offer_id and leg_id of just the first element of each city and date
        offer_key = elements[1][0]
        offer_id = offer_key[0]
        leg_id = offer_key[1]

        # time
        leg_time_string = output_tripleg_level[offer_id][leg_id]["start_time"]
        try:
            leg_time = datetime.fromisoformat(leg_time_string)
        except ValueError:
            # this is to handle an error in the formatting of the time string in some TRIAS files
            leg_time_string = leg_time_string[:leg_time_string.index('+')] + '0' + leg_time_string[leg_time_string.index('+'):]
            leg_time = datetime.fromisoformat(leg_time_string)

        # location
        track = geojson.loads(output_tripleg_level[offer_id][leg_id]['leg_stops'])
        leg_coordinates = np.array(track['coordinates'][0])

        logger.info(f'Current time: {current_time}')
        logger.info(f'Leg time: {leg_time}')
        data_trip = requests.post(url = 'http://owm_proxy:5000/compute',
                                  json = {'current_time' : current_time.isoformat(),
                                          'leg_time' : leg_time.isoformat(),
                                          'leg_coordinate_x' : leg_coordinates[0],
                                          'leg_coordinate_y' : leg_coordinates[1],
                                          'api_key' : api_key,
                                          'execution_mode' : execution_mode},
                                  headers = {'Content-Type': 'application/json'}).json()
        logger.info(data_trip)

        # categorization
        cat_temperature, main_temperature = map_temperature_category(data_trip['feels_like'])
        cat_clouds, desc_clouds = map_cloud_category(data_trip['clouds'])
        cat_precipitation, main_precipitation = map_precipitation_category(check_rain_snow(data_trip))
        cat_wind, desc_wind, num_wind = map_wind_category(data_trip['wind_speed'])

        trip_scenarios = map_weather_scenarios(cat_clouds, cat_precipitation, cat_wind, cat_temperature)
        # print(trip_scenarios)

        # probability of delay
        trip_extreme_conditions = extreme_condition(trip_scenarios)
        city_date_delay = probability_delay(trip_extreme_conditions)

        # probability of each offer
        for ids in elements[1]:
            offerid = ids[0]
            legid = ids[1]
            prob_delay.setdefault(offerid, {})
            prob_delay[offerid].setdefault(legid, city_date_delay)

    # aggregation over legs: get the maximum probability of delay of each offer
    prob_delay_offer = dict()
    for offer in prob_delay.items():
        prob_delay_offer.setdefault(offer[0], offer[1][max(offer[1], key=offer[1].get)])
    # print(prob_delay_offer)

    # normalization
    if score == 'z_score':
        prob_delay_offer_normalized = zscore(prob_delay_offer, flipped=True)
    else:
        prob_delay_offer_normalized = minmaxscore(prob_delay_offer, flipped=True)
    # print(prob_delay_offer_normalized)

    try:
        store_simple_data_to_cache_wrapper(cache, request_id, prob_delay_offer_normalized, 'weather')
    except redis.exceptions.ConnectionError as exc:
        logger.debug("Writing outputs to cache by weather-fc feature collector failed.")

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
