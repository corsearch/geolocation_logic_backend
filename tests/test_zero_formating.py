# test_with_pytest.py
import time
from geocode import *
from xport_to_zero import formating
test_address = (
    "Andreas Bünnecke Dreimorgenstück 11 Weilburg Hessen 35781 DE",)

format_keys = {
    "geoFormatedAddess",
    "geoAddressTypes",
    "geoLocationType",
    "geoPostalCode",
    "geoPostalTown",
    "geoLocation",
    "geoRegion",
    "geoRoute",
    "geoStreetAddress",
    "geoStreetNumber"}


def test_geocode():

    for address in test_address:

        result = Geocoder.geocode(address)
        result = formating(result)

    assert type(result) == type(dict())
    # compare set of attributes
    assert format_keys == set(result.keys())
