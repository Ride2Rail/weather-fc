#!/usr/bin/env python

import bisect
from collections import OrderedDict

# DICTIONARIES

# TEMPERATURE
weather_scenarios_apparent_temperature = {
    "uncomfortably cold": {
        "main": "uncomfortable temperature",
        "range": [-273.15, 0.0],
    },
    "cool": {
        "main": "comfortable temperature",
        "range": [0.0, 15.0],
    },
    "comfortable": {
        "main": "comfortable temperature",
        "range": [15.0, 25.0],
    },
    "warm": {
        "main": "comfortable temperature",
        "range": [25.0, 32.0],
    },
    "uncomfortably hot": {
        "main": "uncomfortable temperature",
        "range": [32.0, 100.0],
    }
}

temperature_ranges = [
    t_min
    for (t_min, t_max) in [
        weather_scenarios_apparent_temperature[k]["range"]
        for k in weather_scenarios_apparent_temperature.keys()
    ]
]

tmp_temperature_ranges = {
    weather_scenarios_apparent_temperature[k]["range"][0]: {
        "category": k,
        "main": weather_scenarios_apparent_temperature[k]["main"],
    }
    for k in weather_scenarios_apparent_temperature.keys()
}

weather_scenarios_apparent_temperature_ranges = OrderedDict()
for t in temperature_ranges:
    weather_scenarios_apparent_temperature_ranges[t] = tmp_temperature_ranges[t]

# CLOUDS
# https://openweathermap.org/weather-conditions
weather_scenarios_clouds = {'clear sky': {'category': 'clear sky',
                                          'main': 'clear',
                                          'range': [0.0, 11.0]
                                          },
                            'few clouds': {'category': 'partially cloudy',
                                           'main': 'clear',
                                           'range': [11.0, 25.0]
                                           },
                            'scattered clouds': {'category': 'partially cloudy',
                                                 'main': 'clouds',
                                                 'range': [25.0, 50.0]
                                                 },
                            'broken clouds': {'category': 'partially cloudy',
                                              'main': 'clouds',
                                              'range': [50.0, 84.0]
                                              },
                            'overcast clouds': {'category': 'completely cloudy',
                                                'main': 'clouds',
                                                'range': [84.0, 100.0]
                                                }
                            }

clouds_percentages = [
    cloudiness_min
    for (cloudiness_min, cloudiness_max) in [
        weather_scenarios_clouds[k]["range"]
        for k in weather_scenarios_clouds.keys()
    ]
]

tmp_cloudiness_ranges = {
    weather_scenarios_clouds[k]["range"][0]: {
        "description": k,
        "category": weather_scenarios_clouds[k]["category"],
        "main": weather_scenarios_clouds[k]["main"]
    }
    for k in weather_scenarios_clouds.keys()
}

weather_scenarios_clouds_ranges = OrderedDict()
for t in clouds_percentages:
    weather_scenarios_clouds_ranges[t] = tmp_cloudiness_ranges[t]

# PRECIPITATION
weather_scenarios_precipitation = {'light rain': {'category': 'light',
                                                  'main': 'rain'
                                                  },
                                   'moderate rain': {'category': 'moderate',
                                                     'main': 'rain'
                                                     },
                                   'heavy intensity rain': {'category': 'heavy',
                                                            'main': 'rain'
                                                            },
                                   'light snow': {'category': 'light',
                                                  'main': 'snow'
                                                  },
                                   'snow': {'category': 'heavy',
                                            'main': 'snow'
                                            }
                                   }

# WIND
weather_scenarios_wind = {'calm': {'category': 'light breeze',
                                   'speed': [0, 0.5],
                                   'beaufort number': 0,
                                   },
                          'light air': {'category': 'light breeze',
                                        'speed': [0.5, 1.5],
                                        'beaufort number': 1,
                                        },
                          'light breeze': {'category': 'light breeze',
                                           'speed': [1.5, 3.3],
                                           'beaufort number': 2,
                                           },
                          'gentle breeze': {'category': 'light breeze',
                                            'speed': [3.3, 5.5],
                                            'beaufort number': 3,
                                            },
                          'moderate breeze': {'category': 'strong breeze',
                                              'speed': [5.5, 7.9],
                                              'beaufort number': 4,
                                              },
                          'fresh breeze': {'category': 'strong breeze',
                                           'speed': [7.9, 10.7],
                                           'beaufort number': 5,
                                           },
                          'strong breeze': {'category': 'strong breeze',
                                            'speed': [10.7, 13.8],
                                            'beaufort number': 6,
                                            },
                          'high wind': {'category': 'gale',
                                        'speed': [13.8, 17.1],
                                        'beaufort number': 7,
                                        },
                          'gale': {'category': 'gale',
                                   'speed': [17.1, 20.7],
                                   'beaufort number': 8,
                                   },
                          'strong/severe gale': {'category': 'gale',
                                                 'speed': [20.7, 24.4],
                                                 'beaufort number': 9,
                                                 },
                          'storm': {'category': 'gale',
                                    'speed': [24.4, 28.4],
                                    'beaufort number': 10,
                                    },
                          'violent storm': {'category': 'gale',
                                            'speed': [28.4, 32.6],
                                            'beaufort number': 11,
                                            },
                          'hurricane force': {'category': 'gale',
                                              'speed': [32.6, 50],
                                              'beaufort number': 12,
                                              }
                          }

wind_speeds = [
    v_min
    for (v_min, v_max) in [
        weather_scenarios_wind[k]["speed"] for k in weather_scenarios_wind.keys()
    ]
]

tmp_wind_ranges = {
    weather_scenarios_wind[k]["speed"][0]: {
        "beaufort number": weather_scenarios_wind[k]["beaufort number"],
        "description": k,
        "category": weather_scenarios_wind[k]["category"],
    }
    for k in weather_scenarios_wind.keys()
}

weather_scenarios_wind_ranges = OrderedDict()
for v in wind_speeds:
    weather_scenarios_wind_ranges[v] = tmp_wind_ranges[v]

# MAP WEATHER SCENARIOS
weather_scenarios = {
    "neutral/good": {
        "clouds": ["none", "clear sky"],
        "precipitation": ["none", "light"],
        "wind": ["light breeze"],
        "temperature": ["comfortable"],
    },
    "cold": {
        "clouds": ["any"],
        "precipitation": ["any"],
        "wind": ["any"],
        "temperature": ["cool"],
    },
    "warm": {
        "clouds": ["any"],
        "precipitation": ["any"],
        "wind": ["any"],
        "temperature": ["warm"],
    },
    "uncomfortable temperature": {
        "clouds": ["any"],
        "precipitation": ["any"],
        "wind": ["any"],
        "temperature": ["uncomfortably cold", "uncomfortably hot"],
    },
    "rainy/snowy": {
        "clouds": ["any"],
        "precipitation": ["moderate", "heavy"],
        "wind": ["any"],
        "temperature": ["any"],
    },
    "cloudy": {
        "clouds": ["partially cloudy", "completely cloudy"],
        "precipitation": ["any"],
        "wind": ["any"],
        "temperature": ["any"],
    },
    "windy": {
        "clouds": ["any"],
        "precipitation": ["any"],
        "wind": ["strong breeze", "gale"],
        "temperature": ["any"],
    },
}


def check_rain_snow(forecast):
    """The categories of rain and snow are not alway availabe. This function checks for if they are present
    in the data and returns None otherwise"""
    weather_scenarios = ['rain', 'snow']
    main = dict()
    for condition in forecast['weather']:
        main[condition['main']] = condition['description']

    for scenario in forecast['weather']:
        if scenario['main'].lower() in weather_scenarios:
            return scenario['description']
        else:
            return 'None'


def map_temperature_category(temperature):
    temperature_ranges_corrected = [float(t - 0.001) for t in temperature_ranges]

    pos = bisect.bisect_left(temperature_ranges_corrected, temperature)

    # since we start from zero, we need to shift by 1
    pos = pos - 1

    if pos >= len(temperature_ranges_corrected):
        pos = len(temperature_ranges_corrected) - 1
    elif pos <= 0:
        pos = 0

    key = temperature_ranges[pos]
    # print('key: {}, pos: {}'.format(key, pos))

    temperature_category = None
    temperature_main = None
    if weather_scenarios_apparent_temperature_ranges.get(key, None) is not None:
        temperature_category = weather_scenarios_apparent_temperature_ranges[key][
            "category"
        ]
        temperature_main = weather_scenarios_apparent_temperature_ranges[key]["main"]

    return temperature_category, temperature_main


def map_cloud_category(cloudiness):
    clouds_percentages_corrected = [float(c - 0.001) for c in clouds_percentages]

    pos = bisect.bisect_left(clouds_percentages_corrected, cloudiness)

    # since we start from zero, we need to shift by 1
    pos = pos - 1

    if pos >= len(clouds_percentages_corrected):
        pos = len(clouds_percentages_corrected) - 1
    elif pos <= 0:
        pos = 0

    key = clouds_percentages[pos]
    # print('key: {}, pos: {}'.format(key, pos))

    cloud_category = None
    cloud_description = None
    if weather_scenarios_clouds_ranges.get(key, None) is not None:
        cloud_category = weather_scenarios_clouds_ranges[key][
            "category"
        ]
        cloud_description = weather_scenarios_clouds_ranges[key]["description"]

    return cloud_category, cloud_description


def map_precipitation_category(description):
    key = description.lower()
    precipitation_category = None
    precipitation_main = None
    if weather_scenarios_precipitation.get(key, None) is not None:
        precipitation_category = weather_scenarios_precipitation[key]["category"]
        precipitation_main = weather_scenarios_precipitation[key]["main"]

    return precipitation_category, precipitation_main


# Given boundaries, find interval
# See:
# https://stackoverflow.com/a/13942715/2377454
def map_wind_category(wind_speed):
    if wind_speed <= 0:
        wind_speed = 0

    wind_speed_corrected = [float(v - 0.001) for v in wind_speeds]

    pos = bisect.bisect_left(wind_speed_corrected, wind_speed)

    # since we start from zero, we need to shift by 1
    pos = pos - 1

    if pos >= len(wind_speed_corrected):
        pos = len(wind_speed_corrected) - 1
    elif pos <= 0:
        pos = 0

    key = wind_speeds[pos]

    wind_category = None
    wind_description = None
    wind_beaufort_number = None
    if weather_scenarios_wind_ranges.get(key, None) is not None:
        wind_category = weather_scenarios_wind_ranges[key]["category"]
        wind_description = weather_scenarios_wind_ranges[key]["description"]
        wind_beaufort_number = weather_scenarios_wind_ranges[key]["beaufort number"]

    return wind_category, wind_description, wind_beaufort_number


#   * weather_characteristic: one of clouds, rain, wind, or temperature
#   * weather_condition: the condition for that weather characteristics
# return the matching scenarios
def match_scenarios(weather_characteristic, weather_condition):
    wcond = weather_condition
    if weather_condition is None:
        wcond = "none"
    wcond = wcond.lower()

    matching_scenarios = set()
    for scenario, weather_dict in weather_scenarios.items():
        if "any" in map(
                str.lower, weather_dict[weather_characteristic]
        ) or wcond in map(str.lower, weather_dict[weather_characteristic]):
            matching_scenarios.add(scenario)

    return matching_scenarios


def map_weather_scenarios(clouds, precipitation, wind, temperature):
    match_clouds = match_scenarios("clouds", clouds)
    match_precipitation = match_scenarios("precipitation", precipitation)
    match_wind = match_scenarios("wind", wind)
    match_temperature = match_scenarios("temperature", temperature)

    match_scenario = set.intersection(
        match_clouds, match_precipitation, match_wind, match_temperature
    )

    scenario = list()
    if len(match_scenario) >= 1:
        scenario = sorted(match_scenario)

    return scenario
