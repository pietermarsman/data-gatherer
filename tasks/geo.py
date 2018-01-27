import datetime
import json
import os
from hashlib import md5

import luigi
from dateutil import parser
from geopy import Nominatim

from config import settings
from intangible import GeoCoordinate, Country, State, Town, Road, HouseNumber
from misc import sanitize_str, GoogleDrive, get_time_node


def get_geo_node(lat, lon):
    geolocator = Nominatim()
    geo_name = "%.6f, %.6f" % (lat, lon)
    location = geolocator.reverse(geo_name).raw
    address = location.get('address')
    print(json.dumps(address, sort_keys=True))

    geo = GeoCoordinate.get_or_create({"name": geo_name, "lat": lat, "lon": lon})[0]
    if address is not None and 'house_number' in address:
        country = Country.get_or_create({'name': address['country']})[0]

        states = country.states.filter(name=address['state']).all()
        if len(states) > 0:
            state = states[0]
        else:
            state = State(name=address['state']).save()
            country.states.connect(state)

        town_name = address.get('town', address.get('city'))
        towns = state.towns.filter(name=town_name).all()
        if len(towns) > 0:
            town = towns[0]
        else:
            town = Town(name=town_name).save()
            state.towns.connect(town)

        road_name = address.get('road', address.get('footway', address.get('pedestrian', address.get('cycleway'))))
        roads = town.roads.filter(name=road_name).all()
        if len(roads) > 0:
            road = roads[0]
        else:
            road = Road(name=road_name).save()
            town.roads.connect(road)

        house_numbers = road.house_numbers.filter(name=address['house_number']).all()
        if len(house_numbers) > 0:
            house_number = house_numbers[0]
        else:
            house_number = HouseNumber(name=address['house_number']).save()
            road.house_numbers.connect(house_number)

        house_number.geos.connect(geo)

    return geo


class GeoDrive(luigi.Task):
    query = luigi.Parameter()
    mime_type = luigi.Parameter()
    folder = luigi.Parameter()
    date = luigi.DateParameter()

    def output(self):
        dir_date = "{date:%Y/%m/%d/}".format(date=self.date)
        file_name = "%s_%s_%s.json" % (
        md5(self.query.encode('utf-8')).hexdigest(), sanitize_str(self.mime_type), sanitize_str(self.folder))
        file_path = os.path.join(settings['io']['out'], self.__class__.__name__, dir_date, file_name)
        return luigi.LocalTarget(file_path)


class ExtractGeoDrive(GeoDrive):
    def run(self):
        gd = GoogleDrive()
        query = self.query.format(folder=self.folder, mime_type=self.mime_type, date=self.date)
        result = gd.list_files(query)
        file_names = result['files']

        if len(file_names) > 0:
            file_id = file_names[0]['id']
            values = gd.get_file(file_id, 'utf-8')
            values = json.loads(values)
        else:
            values = {}

        with self.output().open('w') as f:
            json.dump(values, f)


class TransformGeo(GeoDrive):
    def requires(self):
        return [ExtractGeoDrive(query=self.query, mime_type=self.mime_type, folder=self.folder, date=self.date)]

    def run(self):
        with self.input()[0].open() as f:
            data = json.load(f)

        records = []
        for feature in data.get('features', []):
            record = {
                "lat": feature["geometry"]["coordinates"][1],
                "lon": feature["geometry"]["coordinates"][0],
                "accuracy": float(feature["properties"]["accuracy"]),
                "datetime": datetime.datetime.strptime(feature['properties']['time'],
                                                       '%Y-%m-%dT%H:%M:%S.%fZ').isoformat()
            }
            records.append(record)

        records = [record for record in records if record['accuracy'] < 40]

        with self.output().open('w') as f:
            json.dump(records, f)


class LoadGeo(GeoDrive):
    def requires(self):
        return [TransformGeo(query=self.query, mime_type=self.mime_type, folder=self.folder, date=self.date)]

    def run(self):
        with self.input()[0].open() as f:
            records = json.load(f)

        for record in records:
            dt_node = get_time_node(parser.parse(record['datetime']), "Minute")
            geo_node = get_geo_node(record['lat'], record['lon'])
            geo_node.datetime.connect(dt_node)

        with self.output().open('w') as f:
            f.write('Loaded %d records' % len(records))


class LoadAllGeo(luigi.WrapperTask):
    query = luigi.Parameter()
    mime_type = luigi.Parameter()
    folder = luigi.Parameter()
    start_date = luigi.DateParameter(default=datetime.datetime(2018, 1, 18))
    end_date = luigi.DateParameter(default=datetime.datetime.today())

    def dates(self):
        n_days = (self.end_date - self.start_date).days
        dates = [self.end_date - datetime.timedelta(days=x + 1) for x in range(n_days)]
        return dates

    def requires(self):
        return [LoadGeo(query=self.query, mime_type=self.mime_type, folder=self.folder, date=date) for date in
                self.dates()]
