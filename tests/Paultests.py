
# class test:
#     def __init__(self):
#         self.addresses = ""

#     def run_locationIQ(self):

#         configuration = locationiq.Configuration()
#         configuration.api_key["key"] = "pk.0b7e056b93083909c2b380f7a25bd278"

#         # Defining host is optional and default to
#         # https://eu1.locationiq.com/v1
#         configuration.host = "https://eu1.locationiq.com/v1"
#         # Enter a context with an instance of the API client
#         with locationiq.ApiClient(configuration) as api_client:
#             # Create an instance of the API class
#             api_instance = locationiq.SearchApi(api_client)
#         # str | Address to geocode

#         kwargs = {
#             "q": "4281, Express Lane, Sarasota, 34238, US",
#             "format": "json",
#             "countrycodes": "US",
#             "normalizecity": 1,
#             "addressdetails": 1,
#             "bounded": 1,
#             "limit": 10,
#             "accept_language": "en",
#             "namedetails": 1,
#             "dedupe": 1,
#             "statecode": 0,
#             "matchquality": 0,
#         }
#         try:
#             # Forward Geocoding
#             api_response = api_instance.search(**kwargs)
#             api_response == api_response  # quick fix flake8
#         except ApiException as e:
#             print("Exception when calling SearchApi->search: %s\n" % e)


# earth_radius = 6371000.0  # mean radius
