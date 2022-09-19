from collections import defaultdict
from datetime import date
from functools import lru_cache
from itertools import combinations
import math
import os
import pickle5 as pickle
import re
from string import capwords
from unidecode import unidecode
from zipfile import ZipFile
import locationiq
import pycountry
from rtree import index
import time
from locationiq.rest import ApiException
from typing import Optional
from populate_data.models import  BoundingBox
from sqlmodel import Field, Session, SQLModel, create_engine, select


# from .lib_platformX.utils import jaccard_similarity
# from .lib_platformX.csv_utils import UnicodeFileObjectReader

from populate_data.models import Country
from lib_platformX.utils import jaccard_similarity
from lib_platformX.csv_utils import UnicodeFileObjectReader

# data_path = ["app", "geoloc_logic", "location_files"]
data_path = ["data", "location_files"]

try:
    from secrets import api_key
except:
    api_key = "pk.0b7e056b93083909c2b380f7a25bd278"

postgres_url = "postgresql://remi:pwd@localhost:5432/geolocation_data"
engine = create_engine(postgres_url, echo=False)


def distance(longlat1, longlat2):
    """
    Calculates the distance between two longitude/latitude values using the
    Haversine formula
    Note: longitude comes first in each coordinate pair, to follow x/y
    conventions
    :param longlat1: the first longitude/latitude value
    :param longlat2: the second longitude/latitude value
    :return: the distance in metres
    """
    delta_lambda = math.radians(longlat2[0] - longlat1[0])
    phi1 = math.radians(longlat1[1])
    phi2 = math.radians(longlat2[1])
    delta_phi = phi2 - phi1
    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    earth_radius = 6371000.0  # mean radius
    return round(2 * earth_radius * math.asin(math.sqrt(a)), 3)


class Geocoder:

    delimiters = r"\s|\t|\n|/|,|;|\(|\)|\[|\]"
    place_delimiters = delimiters + r"|\-"
    configuration = locationiq.Configuration()
    configuration.api_key["key"] = api_key
    match_components = {
        "number": ["house_number"],
        "street": ["road"],
        "village_suburb": [
            "borough",
            "neighbourhood",
            "quarter",
            "suburb",
            "village",
            "town",
            "city",
        ],
        "town_city": [
            "borough",
            "neighbourhood",
            "quarter",
            "suburb",
            "village",
            "town",
            "city",
        ],
        "postal_code": ["postcode"],
        "country_code": ["country_code"],
    }
    match_components_query_order = [
        "number",
        "street",
        "village_suburb",
        "town_city",
        "postal_code",
        "country_code",
    ]
    match_components_response_order = [
        "company",
        "building",
        "number",
        "estate",
        "street",
        "village_suburb",
        "town_city",
        "region",
        "postal_code",
        "country",
        "country_code",
    ]
    response_format = "json"
    normalise_city = 1
    address_details = 1
    match_quality = 1
    osm_location_attributes = {
        "distance": "distance",
        "osm_type": "osm_type",
        "osm_id": "osm_id",
        "bounding_box": "boundingbox",
        "latitude": "lat",
        "longitude": "lon",
        "display_name": "display_name",
        "class": "_class",
        "type": "type",
        "importance": "importance",
    }
    osm_address_attributes = [
        "house_number",
        "road",
        "residential",
        "borough",
        "neighbourhood",
        "quarter",
        "hamlet",
        "suburb",
        "island",
        "village",
        "town",
        "city",
        "city_district",
        "county",
        "state",
        "state_district",
        "postcode",
        "country",
        "country_code",
        "state_code",
    ]
    matchquality_attributes = ["matchcode", "matchtype", "matchlevel"]

    @staticmethod
    def match_address(address):
        matched_address = AddressMatcher(address)
        matched_address.match()
        return matched_address.address

    @classmethod
    def jls_extract_def(cls):

        return

    @classmethod
    def geocode(
        cls,
        address,
        sort_by="address",
        min_similarity=0.6,
        max_addresses=1,
        exclude_components=None,
    ):
        if not exclude_components:
            exclude_components = ["region", "country_code", "bounding_box"]
        with locationiq.ApiClient(cls.configuration) as api_client:
            api_client.host = "https://eu1.locationiq.com/v1"
            api_instance = locationiq.SearchApi(api_client)
            matched_address = cls.match_address(address)
            address_string = ", ".join(
                [
                    matched_address[component]["value"]
                    for component in cls.match_components
                    if matched_address[component]["value"]
                ]
            )
            bbox = matched_address["bounding_box"]["value"]
            # viewbox = (", ".join([str(value) for value in bbox])
            # if bbox else "")

            kwargs = {
                "q": address_string,
                "format": cls.response_format,
                "normalizecity": cls.normalise_city,
                "countrycodes": matched_address["country_code"]["value"],
                "addressdetails": cls.address_details,
                "postaladdress": matched_address["postal_code"]["value"],
                "bounded": 1,
                "limit": 10,
                "namedetails": 1,
                "dedupe": 1,
                "matchquality": cls.match_quality,
            }
            # if viewbox:
            #     kwargs["viewbox"] = viewbox
            #     kwargs["bounded"] = 1
            results = []

            def post_reponse(response: list()):
                for entry in response:
                    location = {
                        "address": {
                            attr: matched_address[attr]["value"]
                            for attr in cls.match_components_response_order
                        },
                        "formatted_address": ", ".join(
                            [
                                matched_address[component]["value"]
                                for component in cls.match_components_response_order
                                if matched_address[component]["value"]
                                and component not in exclude_components
                            ]
                        ),
                        "osm": {
                            "details": {
                                attr: getattr(
                                    entry, cls.osm_location_attributes[attr])
                                for attr in cls.osm_location_attributes
                            },
                            "address": {
                                attr: getattr(entry.address, attr)
                                for attr in cls.osm_address_attributes
                            },
                        },
                        "validation": {},
                    }
                    if bbox and entry.lat and entry.lon:
                        bbox_centroid = (
                            (bbox[0] + bbox[2]) / 2.0,
                            (bbox[1] + bbox[3]) / 2.0,
                        )
                        location["validation"]["distance"] = distance(
                            (float(entry.lon), float(entry.lat)), bbox_centroid
                        )
                    else:
                        location["validation"]["distance"] = float("inf")
                    location["validation"]["address_match_level"] = ""
                    for component in reversed(cls.match_components):
                        input_value = matched_address[component]["value"]
                        if input_value:
                            for match_field in cls.match_components[component]:
                                match_field_value = getattr(
                                    entry.address, match_field)
                                if match_field_value:
                                    if (
                                        jaccard_similarity(
                                            input_value.lower(),
                                            match_field_value.lower(),
                                        )
                                        >= min_similarity
                                    ):
                                        location["validation"][
                                            "address_match_level"
                                        ] = match_field
                                    else:
                                        # break
                                        pass
                    results.append(location)
                if sort_by == "distance":
                    results.sort(
                        key=lambda x: (
                            -x["validation"]["distance"],
                            x["validation"]["address_match_level"],
                        )
                    )
                else:
                    results.sort(
                        key=lambda x: (
                            x["validation"]["address_match_level"],
                            -x["validation"]["distance"],
                        )
                    )

                return results[: min(len(results), max_addresses)]

        try:
            response = api_instance.search(**kwargs)
            return post_reponse(response)

        except locationiq.exceptions.ApiException as e:
            if e.body == '{"error":"Unable to geocode"}':

                # clean duplicate in street data
                street_candidate = matched_address["street"]["value"]
                street_candidate = list(
                    dict.fromkeys(matched_address["street"]["value"].split())
                )
                street_candidate = street_candidate

                for token in range(len(street_candidate)):

                    time.sleep(1)  # be gentle with the server

                    # update adress with one word less
                    street_candidate = street_candidate[1:]
                    matched_address["street"]["value"] = " ".join(
                        street_candidate)

                    # address construct
                    candidate_streeet = ", ".join(
                        [
                            matched_address[component]["value"]
                            for component in cls.match_components
                            if matched_address[component]["value"]
                        ]
                    )

                    kwargs.update({"q": candidate_streeet})
                    print("##########")
                    print(kwargs)
                    print("##########")
                    try:
                        response = api_instance.search(**kwargs)
                        break

                    except BaseException:
                        pass

        return post_reponse(response)


class GeoData:
    """
    Class to load and serve geospatial data from GeoNames etc.
    """

    # Functions to load data

    @staticmethod
    @lru_cache(maxsize=10)
    def get_country_data(_day):
        # unpack country names with 2 letters
        res= {country.alpha_2: country.name for country in pycountry.countries}
        return res

    @staticmethod
    @lru_cache(maxsize=10)
    def get_country_names_data(_day):
        country_names_file = os.path.join(*data_path, "country_names.zip")
        with ZipFile(country_names_file) as zip_in:
            with zip_in.open(zip_in.namelist()[0], "r") as csv_in:
                country_names = defaultdict(dict)
                for (
                    country_name,
                    country_code,
                    name,
                    language,
                    language_code,
                    _,
                ) in UnicodeFileObjectReader(csv_in, delimiter="\t"):
                    country_name_lower = country_name.lower()
                    if country_name_lower not in country_names:
                        country_names[country_name_lower] = {
                            "country": country_name,
                            "code": country_code,
                            "languages": {"en"},
                        }
                    name = name.lower()
                    if name not in country_names:
                        country_names[name] = {
                            "country": country_name,
                            "code": country_code,
                            "languages": {language_code},
                        }
                    else:
                        country_names[name]["languages"].add(language_code)
                return country_names

    @staticmethod
    @lru_cache(maxsize=10)
    def get_region_data(_day):
        regions = defaultdict(dict)
        for subdivision in [
            entry for entry in pycountry.subdivisions if entry.type != "Nation"
        ]:
            region_lower = subdivision.name.lower()
            if region_lower not in regions:
                regions[region_lower] = {
                    "region": subdivision.name,
                    "country_codes": {subdivision.country_code},
                }
            else:
                regions[region_lower]["country_codes"].add(
                    subdivision.country_code)
        return regions

    @staticmethod
    @lru_cache(maxsize=10)
    def get_admin_codes_data(_day):
        admin_codes_file = os.path.join(*data_path, "admin_codes.zip")
        # admin_codes_file = os.path.join( "location_files", "admin_codes.zip")
        with ZipFile(admin_codes_file) as zip_in:
            with zip_in.open(zip_in.namelist()[0], "r") as csv_in:
                admin_codes = {}
                for (admin_code, name, level) in UnicodeFileObjectReader(
                    csv_in, delimiter="\t"
                ):
                    admin_codes[admin_code] = {
                        "name": name, "level": int(level)}
                return admin_codes

    @staticmethod
    @lru_cache(maxsize=10)
    def get_postal_code_regexes_data(_day):
        postal_code_regexes_file = os.path.join(
            *data_path, "postal_code_regexes.zip")
        with ZipFile(postal_code_regexes_file) as zip_in:
            with zip_in.open(zip_in.namelist()[0], "r") as csv_in:
                postal_code_regexes = {}
                for country_code, regex in UnicodeFileObjectReader(
                    csv_in, delimiter="\t"
                ):
                    postal_code_regexes[country_code] = regex
        return postal_code_regexes

    @classmethod
    @lru_cache(maxsize=10)
    def get_postal_codes_data(cls, _day):
        pickle_file_path = os.path.join(*data_path, "postal_codes.pickle")
        if os.path.exists(pickle_file_path):
            with open(pickle_file_path, "rb") as pickle_file:
                postal_code_data = pickle.load(pickle_file)
                rtree_index = index.Index(
                    os.path.join(*data_path, "rtree_index.idx"))
                return (
                    postal_code_data["postal_codes"],
                    postal_code_data["bounding_boxes"],
                    rtree_index,
                    postal_code_data["postal_code_ids"],
                )

        # If there is no pickle file, the source zip files will need to be
        # processed, but this will be slow
        bounding_boxes = defaultdict(dict)
        # rtree_index = index.Index(
        # os.path.join("app", "geoloc_logic",
        # "location_files", "rtree_index.idx")
        # )
        rtree_index = index.Index(os.path.join(*data_path, "rtree_index.idx"))
        postal_codes_files = [
            os.path.join(*data_path, "postal_codes.zip"),
            # os.path.join("location_files", "postal_codes.zip"),
            # os.path.join("location_files", 'CA_postal_codes.zip'),
            # os.path.join("location_files", 'GB_postal_codes.zip'),
            # os.path.join("location_files", 'NL_postal_codes.zip')
        ]
        postal_code_ids = {"forward": defaultdict(
            dict), "reverse": defaultdict(dict)}
        postal_codes = {}
        for postal_codes_file in postal_codes_files:
            with ZipFile(postal_codes_file) as zip_in:
                with zip_in.open(zip_in.namelist()[0], "r") as csv_in:
                    exclude_country_codes = (
                        ["CA", "GB", "NL"]
                        if postal_codes_file == "postal_codes.zip"
                        else []
                    )
                    for (
                        country_code,
                        postal_code,
                        place_name,
                        admin_name1,
                        admin_code1,
                        admin_name2,
                        admin_code2,
                        admin_name3,
                        admin_code3,
                        latitude,
                        longitude,
                        accuracy,
                    ) in UnicodeFileObjectReader(csv_in, delimiter="\t"):
                        if country_code not in postal_codes:
                            postal_codes[country_code] = defaultdict(list)
                        if country_code not in exclude_country_codes:
                            postal_codes[country_code][
                                postal_code.lower().replace(" ", "")
                            ].append(
                                {
                                    "place_name": place_name,
                                    "admin_name1": admin_name1,
                                    "admin_code1": admin_code1,
                                    "admin_name2": admin_name2,
                                    "admin_code2": admin_code2,
                                    "admin_name3": admin_name3,
                                    "admin_code3": admin_code3,
                                    "latitude": float(latitude),
                                    "longitude": float(longitude),
                                    "accuracy": int(accuracy) if accuracy else 0,
                                }
                            )
        for country_code, postal_code_data in postal_codes.items():
            for i, (postal_code, records) in enumerate(postal_code_data.items()):
                min_lat = min([record["latitude"] for record in records])
                max_lat = max([record["latitude"] for record in records])
                min_long = min([record["longitude"] for record in records])
                max_long = max([record["longitude"] for record in records])
                bounding_boxes[country_code][postal_code] = (
                    min_long,
                    min_lat,
                    max_long,
                    max_lat,
                )
                postal_code_ids["forward"][country_code][i] = postal_code
                postal_code_ids["reverse"][country_code][postal_code] = i
                rtree_index.insert(i, (min_long, min_lat, max_long, max_lat))
        with open(pickle_file_path, "wb") as pickle_file:
            pickle.dump(
                {
                    "postal_codes": postal_codes,
                    "bounding_boxes": bounding_boxes,
                    "postal_code_ids": postal_code_ids,
                },
                pickle_file,
            )
        # Flush the index to disk so it's available to be loaded next time
        rtree_index.close()
        # Re-open the index so it can be returned
        rtree_index = index.Index(os.path.join(*data_path, "rtree_index.idx"))
        return postal_codes, bounding_boxes, rtree_index, postal_code_ids

    @classmethod
    @lru_cache(maxsize=10)
    def get_places_data(cls, _day):
        places_file = os.path.join(*data_path, "populated_places_1000.zip")

        with ZipFile(places_file) as zip_in:
            with zip_in.open(zip_in.namelist()[0], "r") as csv_in:
                places = defaultdict(list)
                for (
                    geonameid,
                    name,
                    latitude,
                    longitude,
                    feature_class,
                    feature_code,
                    country_code,
                    admin1_code,
                    admin2_code,
                    admin3_code,
                    admin4_code,
                    population,
                ) in UnicodeFileObjectReader(csv_in, delimiter="\t"):
                    name_lower = unidecode(
                        "".join(re.split(Geocoder.place_delimiters, name.lower()))
                    )
                    places[name_lower].append(
                        {
                            "geoname_id": geonameid,
                            "name": name,
                            "latitude": latitude,
                            "longitude": longitude,
                            "feature_class": feature_class,
                            "feature_code": feature_code,
                            "country_code": country_code,
                            "admin1_code": admin1_code,
                            "admin2_code": admin2_code,
                            "admin3_code": admin3_code,
                            "admin4_code": admin4_code,
                            "population": population,
                        }
                    )
        return places

    @staticmethod
    def _create_rtree_index():
        return index.Index()


class AddressMatcher:

    building_indicators = {
        "Suite": {"words": ["suite", "ste", "#"], "offset": 1, "full_word": False},
        "Studio": {"words": ["studio"], "offset": 1, "full_word": False},
        "Apartment": {"words": ["apartment", "apt"], "offset": 1, "full_word": False},
        "Appartment": {
            "words": ["appartment", "appt"],
            "offset": 1,
            "full_word": False,
        },
        "Unit": {"words": ["unit", "no", "no."], "offset": 1, "full_word": False},
        "Office": {"words": ["office"], "offset": 1, "full_word": False},
        "Floor": {"words": ["floor"], "offset": -1, "full_word": True},
        "Building": {"words": ["building", "bldg"], "offset": 1, "full_word": True},
        "House": {"words": ["house"], "offset": "all", "full_word": True},
    }

    company_indicators = {
        "Ltd": {"words": ["limited", "ltd"]},
        "Inc": {"words": ["incorporated", "inc"]},
        "GmbH": {"words": ["gmbh"]},
        "AG": {"words": ["ag"]},
        "KG": {"words": ["kg"]},
        "UG": {"words": ["ug"]},
    }
    industrial_estate_indicators = {
        "first": [
            "industrial",
            "industry",
            "trading",
            "trade",
            "business",
            "enterprise",
        ],
        "second": ["estate", "est", "park", "pk", "centre", "center"],
    }
    estate_substitutions = {"est": "estate", "pk": "park"}
    place_substitutions = {"st": "saint"}
    street_substitutions = {
        "st": {"word": "street", "position": -1},
        "rd": {"word": "road"},
        "ave": {"word": "avenue"},
        "dr": {"word": "drive", "position": -1},
        "ln": {"word": "lane"},
        "hwy": {"word": "highway"},
        "pkwy": {"word": "parkway"},
        "n": {"word": "north"},
        "e": {"word": "east"},
        "w": {"word": "west"},
        "s": {"word": "south"},
        "bd": {"word": "boulevard"},
        "blvd": {"word": "boulevard"},
        "r": {"word": "rue", "position": 0},
        "str": {"word": "straße", "position": -2},
    }
    match_functions = {
        "country_code": "country_code",
        "country": "country",
        "postal_code": "postal_code",
        "region": "region",
        "town_city": "place",
        "village_suburb": "place",
    }

    def __init__(self, address):
        self.input_address = address
        self.address_tokens = [
            token.strip().rstrip(".")
            for token in re.split(Geocoder.delimiters, address)
            if token is not None and token != ""
        ]
        self.unparsed_tokens = self.address_tokens
        self.address = {
            "company": {"value": None, "indexes": []},
            "building": {"value": None, "indexes": []},
            "estate": {"value": None, "indexes": []},
            "number": {"value": None, "indexes": []},
            "street": {"value": None, "indexes": []},
            "village_suburb": {"value": None, "indexes": []},
            "town_city": {"value": None, "indexes": []},
            "region": {"value": None, "indexes": []},
            "postal_code": {"value": None, "indexes": []},
            "country": {"value": None, "indexes": []},
            "country_code": {"value": None, "indexes": []},
            "bounding_box": {"value": None, "indexes": []},
        }
        self.latitude = None
        self.longitude = None

    # Match functions
    def match_country_code(self, candidate, **kwargs):
        candidate = candidate.upper()
        if len(candidate) != 2:
            return None
        if candidate in GeoData.get_country_data(date.today()):
            return candidate
        # US states and Canadian provinces do not have overlapping codes so it
        # is safe to treat them in sequence
        us_state = self.match_us_state_code(candidate)
        if us_state:
            self.address["region"]["value"] = us_state
            return "US"
        canadian_province = self.match_canadian_province_code(candidate)
        if canadian_province:
            self.address["region"]["value"] = canadian_province
            return "CA"

    @classmethod
    def match_country(cls, candidate, **kwargs):

        with Session(engine) as session:
            query = select(Country).where(
                Country.name == candidate)
            results = session.exec(query)

        if results.first() is not None:
            return results.name

        # match = GeoData.get_country_names_data(date.today()).get(candidate.lower())
        # if match:
        #     return match["country"]

    def match_region(self, candidate, **kwargs):
        match = GeoData.get_region_data(date.today()).get(candidate.lower())
        if match:
            country_code = self.address["country_code"]["value"]
            if country_code in match["country_codes"] or (
                not country_code and len(match["country_codes"]) == 1
            ):
                return match["region"]
            else:
                return None

    @staticmethod
    def match_us_state_code(candidate):
        try:
            pass
            state = ""  # quick fix flake8
            # state = us.states.lookup(candidate.upper())
            return "US"
        except BaseException:
            return None

    @staticmethod
    def match_canadian_province_code(candidate):
        # Yukon, Northwest Territories and Nunavut are actually territories
        # rather than provinces, but will be handled
        # in the same manner
        canadian_codes = {
            "NL": "Newfoundland and Labrador",
            "PE": "Prince Edward Island",
            "NS": "Nova Scotia",
            "NB": "New Brunswick",
            "QC": "Quebec",
            "ON": "Ontario",
            "MB": "Manitoba",
            "SK": "Saskatchewan",
            "AB": "Alberta",
            "BC": "British Columbia",
            "YT": "Yukon",
            "NT": "Northwest Territories",
            "NU": "Nunavut",
        }
        return canadian_codes.get(candidate.upper())

    @staticmethod
    def country_from_region(region):
        match = GeoData.get_region_data(date.today()).get(region.lower())
        if match and len(match["country_codes"]) == 1:
            return match["country_codes"].pop()

    def match_postal_code(self, candidate, **kwargs):
        country_code = self.address["country_code"]["value"]
        if country_code:
            regex = GeoData.get_postal_code_regexes_data(
                date.today()).get(country_code)
            if regex:
                match = re.match(regex, candidate.upper())
                if match:
                    return candidate

    def match_place(self, candidate, **kwargs):
        if self.address[kwargs["element"]]["value"]:
            # The place may already have been matched, e.g. in case of
            # ambiguity between country and US state codes
            return
        town_city_range = self.address["town_city"]["indexes"]
        if town_city_range and set(range(*kwargs["token_range"])) & set(
            range(*town_city_range)
        ):
            # Don't attempt to use a candidate that is part of an already
            # matched town/city
            return
        matches = GeoData.get_places_data(date.today()).get(
            unidecode(
                "".join(
                    [
                        self.substitute_place(token)
                        for token in re.split(
                            Geocoder.place_delimiters, candidate.lower()
                        )
                    ]
                )
            )
        )
        # Remove anything that's already been matched to a region or to a
        # town/city
        if matches:
            matches = [
                match
                for match in matches
                if match["name"]
                not in [
                    self.address["region"]["value"],
                    self.address["town_city"]["value"],
                ]
            ]
            country_code = self.address["country_code"]["value"]
            if country_code:
                state_matches = province_matches = []
                country_matches = [
                    m for m in matches if m["country_code"] == country_code
                ]
                state = self.match_us_state_code(country_code)
                if state:
                    state_matches = [
                        m
                        for m in matches
                        if m["country_code"] == "US"
                        and m["admin1_code"] == country_code
                    ]
                province = self.match_canadian_province_code(country_code)
                if province:
                    province_matches = [
                        m
                        for m in matches
                        if m["country_code"] == "CA"
                        and m["admin1_code"] == country_code
                    ]
                matches = country_matches + state_matches + province_matches
            matches = sorted(matches, key=lambda x: int(
                x["population"]), reverse=True)
            if matches:
                match = matches[0]
                self.latitude = match["latitude"]
                self.longitude = match["longitude"]
                self.address["country_code"]["value"] = match["country_code"]
                if not self.address["region"]["value"]:
                    admin_code = (
                        ".".join(
                            [
                                match["country_code"],
                                match["admin1_code"],
                                match["admin2_code"],
                            ]
                        )
                        if match["country_code"] in ["GB", "GR"]
                        else ".".join([match["country_code"], match["admin1_code"]])
                    )
                    region = GeoData.get_admin_codes_data(
                        date.today()).get(admin_code)
                    if region:
                        self.address["region"]["value"] = region["name"]
                return match["name"]

    def match_from_elements(self, element):
        match_function = getattr(
            self, "_".join(["match", self.match_functions[element]])
        )
        kwargs = {"element": element}
        for i in range(len(self.address_tokens), -1, -1):
            for j in range(i):
                candidate = " ".join(self.address_tokens[j:i])
                kwargs["token_range"] = (j, i)
                # Special handling: only look for a country code in the last
                # token as otherwise abbreviations such
                # as ST (Street) and CL (Close) will be wrongly considered as
                # countries (São Tomé and Príncipe, Chile)
                if element == "country_code" and candidate != self.address_tokens[-1]:
                    continue
                # Special handling: if town_city has already been found,
                # village_suburb must be immediately adjacent
                if (
                    element == "village_suburb"
                    and self.address["town_city"]["value"]
                    and i != self.address["town_city"]["indexes"][0]
                ):
                    continue
                match = match_function(candidate, **kwargs)
                if match:
                    self.address[element]["value"] = match
                    self.address[element]["indexes"] = [j, i]
                    return

    def check_sequence(self):
        # Initially cross-check town/city against region: we would expect
        # town/city to occur first in the address,
        # so remove region if this is not the case
        town_city_indexes = self.address["town_city"]["indexes"]
        region_indexes = self.address["region"]["indexes"]
        if (
            town_city_indexes
            and region_indexes
            and region_indexes[1] <= town_city_indexes[0]
        ):
            self.address["region"]["value"] = None
            self.address["region"]["indexes"] = []
        country_indexes = self.address["country"]["indexes"]
        country_code_indexes = self.address["country_code"]["indexes"]
        country = self.address["country"]["value"]
        country_code = self.address["country"]["value"]
        if country_indexes and country_code_indexes:
            max_index = max(
                [
                    element["indexes"][1]
                    for element in self.address.values()
                    if element["indexes"]
                ]
            )
            if (
                country != GeoData.get_country_data(
                    date.today()).get(country_code)
            ) or (country and country_code_indexes[1] < max_index):
                self.address["country"]["value"] = None
                self.address["country"]["indexes"] = []

    def get_bounding_box(self):

        postal_code_normalised = self.address["postal_code"]["value"].upper().replace(" ", "")
        country_code = self.address["country_code"]["value"]

        if not postal_code_normalised or not country_code:
            return None

        with Session(engine) as session:
            statement = select(BoundingBox).where(
                                                    BoundingBox.country_code ==country_code ).where(
                                                    BoundingBox.postal_code == postal_code_normalised)
            result = session.exec(statement).first()
        
        if not result:
            return None
        # (
        #     postal_codes,
        #     bounding_boxes,
        #     rtree_index,
        #     postal_code_ids,
        # ) = GeoData.get_postal_codes_data(date.today())
        # postal_code_country_data = postal_codes.get(country_code)

        # if not postal_code_country_data:
        #     return None

        # postal_code_normalised = postal_code.lower().replace(" ", "")
        # postal_code_data = postal_code_country_data.get(postal_code_normalised)
        # if not postal_code_data:
        #     return None

        # bbox = bounding_boxes[country_code][postal_code_normalised]

        # centroid = ((bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0)
        # nearest_ids = rtree_index.nearest(bbox, 5)
        # min_distance = float("inf")
        # for candidate_id in nearest_ids:
        #     candidate = postal_code_ids["forward"][country_code][candidate_id]
        #     candidate_normalised = candidate.lower().replace(" ", "")
        #     if candidate_normalised == postal_code_normalised:
        #         continue
        #     min_long, min_lat, max_long, max_lat = bounding_boxes[country_code][
        #         candidate_normalised
        #     ]

        #     candidate_centroid = (
        #         (min_long + max_long) / 2.0,
        #         (min_lat + max_lat) / 2.0,
        #     )
        #     min_distance = min(min_distance, distance(
        #         centroid, candidate_centroid))
        # offset = max(min_distance, min_offset)
        # long_offset = round(
        #     offset / (111111.0 * math.cos(math.radians(centroid[1]))), 7
        # )
        # lat_offset = round(offset / 111111.0, 7)
        # self.address["bounding_box"]["value"] = (
        #     centroid[0] - long_offset,
        #     centroid[1] - lat_offset,
        #     centroid[0] + long_offset,
        #     centroid[1] + lat_offset,
        # )
        self.address["bounding_box"]["value"] =  result.BoundingBox
        self.address["bounding_box"]["indexes"] = []

    def match(self):
        for element in ["country_code", "country"]:
            self.match_from_elements(element)
        self.complete_country_data()
        self.match_from_elements("region")
        self.complete_country_data(check_region=True)
        for element in ["town_city", "village_suburb", "postal_code"]:
            self.match_from_elements(element)
            self.complete_country_data()
        self.check_sequence()
        self.match_street()
        self.match_company_indicators()
        self.match_building_indicators()
        self.match_industrial_estate_indicators()
        if self.address["street"]["value"]:
            self.match_number()
        if self.address["street"]["value"]:
            self.substitute_street()
        self.complete_country_data()
        self.get_bounding_box()

    def complete_country_data(self, check_region=False):
        country = self.address["country"]["value"]
        country_code = self.address["country_code"]["value"]
        if check_region and not country and not country_code:
            region = self.address["region"]["value"]
            if region:
                self.address["country_code"]["value"] = self.country_from_region(
                    region)
                country_code = self.address["country_code"]["value"]
        if country and not country_code:
            self.address["country_code"]["value"] = GeoData.get_country_names_data(
                date.today()
            ).get(country.lower())["code"]
        elif country_code and not country:
            # self.match_from_elements('place')
            # The country code may have changed, so get it again
            country_code = self.address["country_code"]["value"]
            self.address["country"]["value"] = GeoData.get_country_data(
                date.today()
            ).get(country_code)

    @property
    def valid_address(self):
        valid = True
        for element1, element2 in combinations(self.address, 2):
            indexes1 = self.address[element1]["indexes"]
            indexes2 = self.address[element2]["indexes"]
            if indexes1 and indexes2 and set(range(*indexes1)) & set(range(*indexes2)):
                valid = False
        return valid

    def match_street(self):
        if not self.address["town_city"]["value"]:
            return
        parsed_indexes = []
        for element_data in [
            value for value in self.address.values() if value["indexes"]
        ]:
            for idx in range(*element_data["indexes"]):
                parsed_indexes.append(idx)
        self.unparsed_tokens = [
            token
            for token in self.unparsed_tokens
            if self.unparsed_tokens.index(token) not in parsed_indexes
        ]
        parsed_indexes = [
            value["indexes"][0] for value in self.address.values() if value["indexes"]
        ]
        if parsed_indexes and min(parsed_indexes) > 0:
            street_tokens = self.unparsed_tokens[: min(parsed_indexes)]
            street_candidate = " ".join(
                [token.lower() for i, token in enumerate(street_tokens)]
            )
            self.address["street"]["value"] = capwords(street_candidate)

    @property
    def fully_parsed(self):
        return not self.unparsed_tokens

    def match_building_indicators(self):
        if not self.address["street"]["value"]:
            return
        street_tokens = re.split(
            Geocoder.delimiters, self.address["street"]["value"])
        indicator_entries = set()
        for i, token in enumerate(street_tokens):
            for indicator_name, indicator in self.building_indicators.items():
                offset = indicator["offset"]
                for word in indicator["words"]:
                    if word == token.lower():
                        if offset == "all":
                            indicator_entries.add(
                                " ".join(street_tokens[:i] + [indicator_name])
                            )
                            del street_tokens[: i + 1]
                        else:
                            building_name = (
                                " ".join(
                                    [indicator_name, street_tokens[i + offset]])
                                if offset > 0
                                else " ".join(
                                    [street_tokens[i + offset], indicator_name]
                                )
                            )
                            indicator_entries.add(building_name)
                            if offset > 0:
                                del street_tokens[i: i + offset + 1]
                            else:
                                del street_tokens[i + offset: i + 1]
                    elif not indicator["full_word"] and word in token.lower():
                        remainder = token.replace(word, "")
                        building_name = (
                            " ".join([indicator_name, remainder])
                            if offset > 0
                            else " ".join([remainder, indicator_name])
                        )
                        indicator_entries.add(building_name)
                        del street_tokens[i]
        self.address["street"]["value"] = " ".join(street_tokens)
        self.address["building"]["value"] = " ".join(
            [capwords(entry) for entry in indicator_entries]
        )

    def match_company_indicators(self):
        if not self.address["street"]["value"]:
            return
        street_tokens = re.split(
            Geocoder.delimiters, self.address["street"]["value"])
        indicator_entries = set()
        for i, token in enumerate(street_tokens):
            for indicator_name, indicator in self.company_indicators.items():
                for word in indicator["words"]:
                    if word == token.lower():
                        indicator_entries.add(
                            " ".join(
                                [" ".join(street_tokens[:i]), indicator_name])
                        )
                        del street_tokens[: i + 1]
        self.address["street"]["value"] = " ".join(street_tokens)
        self.address["company"]["value"] = " ".join(
            [capwords(entry) for entry in indicator_entries]
        )

    def match_industrial_estate_indicators(self):
        if not self.address["street"]["value"]:
            return
        street_tokens = [
            token.lower()
            for token in re.split(Geocoder.delimiters, self.address["street"]["value"])
        ]
        indicator_ranges = {}
        indicator_entries = set()
        indicator_names = [
            " ".join([first, second])
            for first in self.industrial_estate_indicators["first"]
            for second in self.industrial_estate_indicators["second"]
        ]
        if len(street_tokens) < 3:
            return
        for i in range(len(street_tokens), 0, -1):
            indicator_range = (max(0, i - 2), i)
            candidate_tokens = [
                token for token in street_tokens[slice(*indicator_range)]
            ]
            industrial_estate_candidate = " ".join(candidate_tokens)
            for indicator_name in indicator_names:
                if indicator_name in industrial_estate_candidate:
                    indicator_ranges[indicator_range] = industrial_estate_candidate
        for indicator_range in sorted(indicator_ranges):
            indicator_entries.add(
                " ".join(
                    [
                        capwords(self.estate_substitutions.get(token, token))
                        for token in street_tokens[: indicator_range[1]]
                    ]
                )
            )
            del street_tokens[: indicator_range[1]]
        self.address["street"]["value"] = " ".join(
            [capwords(token) for token in street_tokens]
        )
        self.address["estate"]["value"] = " ".join(
            [capwords(entry) for entry in indicator_entries]
        )

    def match_number(self):
        street_tokens = [
            token.lower()
            for token in re.split(Geocoder.delimiters, self.address["street"]["value"])
        ]
        output_street_tokens = []
        for token in street_tokens:
            if not self.address["number"]["value"] and (
                re.match(r"^\d", token) or re.match(r"\d$", token)
            ):
                self.address["number"]["value"] = token
            else:
                output_street_tokens.append(token)
            street_candidate = " ".join(
                [token.lower() for i, token in enumerate(output_street_tokens)]
            )
            self.address["street"]["value"] = capwords(street_candidate)

    def substitute_place(self, token):
        return self.place_substitutions.get(token, token)

    def substitute_street(self):
        self.street_tokens = [
            token.lower().strip().rstrip(".")
            for token in re.split(Geocoder.delimiters, self.address["street"]["value"])
            if token is not None and token != ""
        ]
        substitutions = []
        for idx, token in enumerate(self.street_tokens):
            substitution = self.street_substitutions.get(token)
            if substitution:
                if (
                    not substitution.get("position")
                    or substitution["position"] == idx
                    or substitution["position"] == idx - len(self.street_tokens)
                ):
                    substitutions.append(substitution["word"])
                else:
                    substitutions.append(token)
            else:
                substitutions.append(token)
        self.address["street"]["value"] = capwords(" ".join(substitutions))
