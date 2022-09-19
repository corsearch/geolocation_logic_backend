# test_with_pytest.py
import time
from geocode import Geocoder
from test_data import addresses


def test_content_parser():
    """ "  LocationIQ is used but just the result of the parser is  tested"""

    for add in addresses:

        result = Geocoder.geocode(add["address"])
        time.sleep(1)  # as long as we don't have a proper API key

        # test parsing
        assert result[0]["address"]["company"] == add["Company"]
