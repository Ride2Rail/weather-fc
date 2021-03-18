#!/usr/bin/env python3

from flask import Flask, request, abort
import redis

# from r2r_offer_utils import normalization

app = Flask(__name__)
cache = redis.Redis(host='redis', port=6379)


@app.route('/compute', methods=['POST'])
def extract():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.mget('{}:offers'.format(request_id), 0, -1)

    import ipdb; ipdb.set_trace()

    response = app.response_class(
        response='{{"request_id": "{}"}}'.format(request_id),
        status=200,
        mimetype='application/json'
    )

    # normalization.zscore(...)

    return response


if __name__ == '__main__':
    import os

    FLASK_PORT = 5000

    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

    os.environ["FLASK_ENV"] = "development"

    cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    app.run(port=FLASK_PORT, debug=True)

    exit(0)
