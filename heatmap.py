'''
Reads the [counts_file] file to get a count of some interesting
factor of analysis aggregated by precinct and a geojson file with
the counties of NYC (can be downloaded from https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
under School, Police, Health & Fire) and returns a geojson in which each precinct is colored
depending on the relative counts.
Usage: python heatmap.py [precinct_geojson_file] [counts_file] [output_file]
'''

from __future__ import print_function
import json
import sys
from matplotlib.cm import get_cmap

precinct_geojson_file = sys.argv[1]
counts_file = sys.argv[2]
output_file = sys.argv[3]


def create_crimes_dict(by_precinct_file):
    crimes_dict = {}
    with open(by_precinct_file,'r') as f:
        for line in f:
            line = line.strip()
            line = line[1:-1]
            line = line.split(',')
            precinct = int(line[0][1:-1])
            count = int(line[1])
            crimes_dict[precinct] = count
    return crimes_dict


def normalize_dict(dic):
    maximum = -1.
    for i in dic.values():
        if i > maximum:
            maximum = i

    normalized = {i: val * 1.0 / maximum for i, val in dic.items()}
    return normalized


def get_color(cmap, value):
    rgba = cmap(value)
    rgb = [int(x * 255) for x in rgba[:-1]]
    hex_color = "#{0:02x}{1:02x}{2:02x}".format(rgb[0], rgb[1], rgb[2])
    return hex_color


if __name__ == "__main__":

    p_json = json.load(open(precinct_geojson_file, 'r'))
    crimes_dict = create_crimes_dict(counts_file)
    crimes_dict_norm = normalize_dict(crimes_dict)
    cmap = get_cmap('YlOrRd')

    color_dict = {i: get_color(cmap, j) for i, j in crimes_dict_norm.items()}

    for feature in p_json['features']:
        p = feature['properties']['Precinct']
        feature['properties']['fill'] = color_dict[p]
        feature['properties']['fill-opacity'] = 0.8
        feature['properties']['stroke-width'] = 1.5

    json.dump(p_json, open(output_file, 'w'))
