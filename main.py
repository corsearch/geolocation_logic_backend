# import time
from geocode import Geocoder


def call_API():
    # data = []
    print("processing ...")
    # C = 0
    # for address_data in addresses:
    #     C = C + 1
    #     print("############")
    #     print(C ," - Search for:", address_data["address"])
    #     result=Geocoder.geocode( address_data["address"] )
    #     data.append(result)
    #     time.sleep(1)
    # result= Geocoder.geocode("AMAZON UK SERVICES LTD 1 PRINCIPAL PLACE"+
    # " WORSHIP STREET Worship Street LONDON Greater London EC2A 2FA GB")

    # example multicase query
    result = Geocoder.geocode(
        "Andreas Bünnecke Dreimorgenstück 11 Weilburg Hessen 35781 DE"
    )

    return result


if __name__ == "__main__":
    print(call_API())
    print("finished ! ")
