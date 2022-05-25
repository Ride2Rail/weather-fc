import redis
import json


#############################################################################
#############################################################################
#############################################################################
def extract_data_from_cache(
        pa_cache,
        pa_request_id,
        pa_offer_level_items,
        pa_tripleg_level_items):
    output_offer_level_items = dict()
    output_tripleg_level_items = dict()

    offers = pa_cache.lrange('{}:offers'.format(pa_request_id), 0, -1)
    offer_ids = [offer.decode('utf-8') for offer in offers]
    output_offer_level_items["offer_ids"] = offer_ids
    if offer_ids is not None:
        for offer in offer_ids:
            output_offer_level_items[offer] = {}
            for offer_level_item in pa_offer_level_items:
                # assembly key for offer level
                temp_key = "{}:{}:{}".format(pa_request_id, offer, offer_level_item)
                # extract offer level data from cache
                if (offer_level_item == "bookable_total") or (offer_level_item == "complete_total"):
                    temp_data = pa_cache.hgetall(temp_key)
                else:
                    temp_data = pa_cache.get(temp_key).decode('utf-8')
                output_offer_level_items[offer][offer_level_item] = temp_data
            # extract information at the tripleg level
            output_tripleg_level_items[offer] = {}
            if len(pa_tripleg_level_items) > 0:
                temp_key = "{}:{}:legs".format(pa_request_id, offer)
                tripleg = pa_cache.lrange(temp_key, 0, -1)
                tripleg_ids = [leg.decode('utf-8') for leg in tripleg]
                output_tripleg_level_items[offer]["triplegs"] = tripleg_ids
                for tripleg_id in tripleg_ids:
                    output_tripleg_level_items[offer][tripleg_id] = {}
                    for tripleg_level_item in pa_tripleg_level_items:
                        temp_key = "{}:{}:{}:{}".format(pa_request_id, offer, tripleg_id, tripleg_level_item)
                        tripleg_data = pa_cache.get(temp_key).decode('utf-8')
                        output_tripleg_level_items[offer][tripleg_id][tripleg_level_item] = tripleg_data
    return output_offer_level_items, output_tripleg_level_items


#############################################################################
#############################################################################
#############################################################################
def store_simple_data_to_cache(
        pa_cache,
        pa_request_id,
        pa_data,
        pa_sub_key
):
    for offer in pa_data:
        temp_key = "{}:{}:{}".format(pa_request_id, offer, pa_sub_key)
        pa_cache.set(temp_key, pa_data[offer])
    return 1
#############################################################################
#############################################################################
#############################################################################
