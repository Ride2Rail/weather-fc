#!/usr/bin/env python3

import os
import sys
import pathlib
import logging
import configparser as cp

import redis
from flask import Flask, request, abort

# from r2r_offer_utils import normalization

app = Flask(__name__)
cache = redis.Redis(host='cache', port=6379)

##### Config
service_basename = os.path.splitext(os.path.basename(__file__))[0]
config_file = '{name}.conf'.format(name=service_basename)
config = cp.ConfigParser()
config.read(config_file)
#####

##### Logging
# create logger
logger = logging.getLogger(service_basename)
logger.setLevel(logging.DEBUG)

# create formatter
formatter_fh = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s')
formatter_ch = logging.Formatter('[%(asctime)s][%(levelname)s](%(name)s): %(message)s')

default_log = pathlib.Path(config.get('logging', 'default_log'))
try:
    default_log.parent.mkdir(parents=True, exist_ok=True)
    default_log.touch(exist_ok=True)

    basefh = logging.FileHandler(default_log, mode='a+')
except Exception as err:
    print("WARNING: could not create log file '{log}'"
          .format(log=default_log), file=sys.stderr)
    print("WARNING: {err}".format(err=err), file=sys.stderr)
#####


@app.route('/compute', methods=['POST'])
def extract():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    # print(offer_data)

    response = app.response_class(
        response='{{"request_id": "{}"}}'.format(request_id),
        status=200,
        mimetype='application/json'
    )

    # normalization.zscore(...)

    return response


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-host',
                        default='localhost',
                        help='Redis hostname [default: localhost].')
    parser.add_argument('--redis-port',
                        default=6379,
                        type=IntRange(1, 65536),
                        help='Redis port [default: 6379].')

    # remove default logger
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    # create file handler which logs INFO messages
    fh = logging.FileHandler("{}.log".format(service_basename), mode='a+')
    fh.setLevel(logging.INFO)

    # create console handler and set level to DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # add formatter to ch
    ch.setFormatter(formatter_ch)
    fh.setFormatter(formatter_fh)

    # add ch to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    FLASK_PORT = 5000

    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

    os.environ["FLASK_ENV"] = "development"

    cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    app.run(port=FLASK_PORT, debug=True)

    exit(0)
