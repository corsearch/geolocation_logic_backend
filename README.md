<h1> Backend Logic Geolocation "version DB" </h1>

This repo is for  the Python module plugged behind the gelocation service.
It is python code that parses  and processes addresses. The processed addresses are then send a to third part for geolocation.


<h2> Requirements</h2>

- python3.10
- docker compose

<h2> get started </h2>

option 1 :   first time you run the  the code
    - from the  folder  /populate_data  run  the bash script:
        ./set_db_data.sh
        it can last a couple of minutes. The data ingestion process is finished when you  can  read : "Populating DB finished "

option 2 :  otherwise
        - !!!!  docker compose up  to start the db

- make sure to have the dependencies with   : pip install -r requierements.py
- run the code with default arguments  with  : python3 main.py




<h2> Tests </h2>

- run the tests with : pytest
- run test coverage:  pytest --cov  geocode



<h2>  Useful Links </h2>

- [to  microservice geolocation repo ](https://github.com/corsearch/service_geolocation)

- [to the chassis repo ](https://github.com/CloudBats/fastapi-holistic/)

- [to LocationIQ  documentation ](https://github.com/location-iq/locationiq-python-client)


