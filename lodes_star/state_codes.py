import csv

class Geographies:
    with open('lodes_star/geographies.csv') as csvfile:
        next(csvfile)
        csv_reader = csv.reader(csvfile)
        geographies = dict(csv_reader)


class State:
    with open('lodes_star/states.csv') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        states = list(csv_reader)

    abb2code = {x['abb']: x['code'] for x in states}
    code2abb = {v: k for k, v in abb2code.items()}

    name2code = {x['name']: x['code'] for x in states}
    code2name = {v: k for k, v in name2code.items()}

    name2abb = {x['name']: x['abb'] for x in states}
    abb2name = {v: k for k, v in name2abb.items()}
