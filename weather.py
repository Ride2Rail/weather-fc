#!/usr/bin/env python3

import os
import configparser as cp

import redis
from flask import Flask, request

from r2r_offer_utils.logging import setup_logger
from mapping.cache_operations import extract_data_from_cache

from datetime import datetime
import geojson
import numpy as np


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
                for leg in output_tripleg_level[offer]['triplegs']:
                    # time
                    start_time = datetime.fromisoformat(output_tripleg_level[offer][leg]['start_time'])
                    end_time = datetime.fromisoformat(output_tripleg_level[offer][leg]['end_time'])
                    leg_time = start_time + (end_time-start_time)/2
                    print(leg_time.isoformat())

                    # location
                    track = geojson.loads(output_tripleg_level[offer][leg]['leg_stops'])
                    start_coordinates = np.array(track['coordinates'][0])
                    end_coordinates = np.array(track['coordinates'][-1])
                    print(start_coordinates, end_coordinates)
                    leg_coordinates = (end_coordinates + start_coordinates) / 2
                    print(leg_coordinates)

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
