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
    # cache.lrange('{}:offers'.format(request_id), 0, -1)

    response = app.response_class(
        response='{{"request_id": "{}"}}'.format(request_id),
        status=200,
        mimetype='application/json'
    )

    # normalization.zscore(...)

    return response


# if __name__ == '__main__':
# 	app.run(...)
