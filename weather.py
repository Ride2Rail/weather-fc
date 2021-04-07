#!/usr/bin/env python3

import os
import configparser as cp

import redis
from flask import Flask, request

from r2r_offer_utils.logging import setup_logger


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
    # print(offer_data)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

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
