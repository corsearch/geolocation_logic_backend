# test_with_pytest.py
import time
from geocode import Geocoder

test_address =( "Andreas Bünnecke Dreimorgenstück 11 Weilburg Hessen 35781 DE",
                "PBSHOP.CO.UK LTD Unit 22, Horcott Industrial Estate, Horcott Road Fairford Gloucestershire GL7 4BX GB",
                "1969 Rutgers University Blvd Unit C Lakewood United States of America New Jersey 08701 US",
                "10 Avenue de Camberwell Avenue de Camberwell Sceaux 92330 FR",
                "AMAZON UK SERVICES LTD 1 PRINCIPAL PLACE WORSHIP STREET Worship Street LONDON Greater London EC2A 2FA GB"
)



def test_all_adress_pass():

    data=[]
    for address in test_address:
   
        result=Geocoder.geocode( address )
        data.append(result)
        time.sleep(1)
        print(result)  
        
    assert len(data)== 5        



