
def formating(data_from_locationIQ):
    """
    forming data for easing integration in Zero
    """
    # cleaning
    to_python_objects = data_from_locationIQ
    osm_data = to_python_objects[0]["osm"]
    parts = ["details", "address"]

    # mapping
    mapping_locationIQ_zero = {
        "geoFormatedAddess": {"level": "details", "data": "display_name"},
        "geoAddressTypes": {"level": "details", "data": "osm_type"},
        "geoLocationType": {"level": "details", "data": "type"},
        "geoPostalCode": {"level": "address", "data": "postcode"},
        "geoPostalTown": {"level": "address", "data": "city"},
        "geoRegion": {"level": "address", "data": "city"},
        "geoRoute": {"level": "address", "data": "road"},
        "geoStreetAddress": {"level": "details", "data": "display_name"},
        "geoStreetNumber": {"level": "address", "data": "house_number"},
    }

    # data processing
    response = {}
    for key in mapping_locationIQ_zero.items():
        mapping = key[0]
        level = key[1]["level"]
        data = key[1]["data"]
        response[mapping] = osm_data[level][data]

    response["geoLocation"] = str({"latitude": osm_data["details"]["latitude"],
                                  "longitude": osm_data["details"]["longitude"]})

    return response
