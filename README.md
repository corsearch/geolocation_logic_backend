<h1> Backend Logic Geolocation "version DB" </h1>

This repo is for  the Python module plugged behind the gelocation service.
It is python code that parses  and processes addresses.
The processed addresses are
then send a to third part for geolocation.


<h2> Requirements</h2>

- python3.10
- docker compose

<h2> Get started </h2>

option 1 :  Is it the first time you run the  the code ? then follow below.
- from the  folder  /populate_data  run  the bash script:
        ./set_db_data.sh

this script is to ingest the necessary input data in the db.
it can last a couple of minutes.
The data ingestion process is finished
when you  can  read : "Populating DB finished "

option 2 :  Otherwise
- From  the /populate_data run "docker compose up"  to start the db

After option 1 or 2
- make sure to have the dependencies with   : pip install -r requierements.py
- By now you  should be able to run the code with default arguments  with  : python3 main.py




<h2> Tests </h2>

- run the tests with : pytest
- run test coverage:  pytest --cov  geocode



<h2>  Useful Links </h2>

- [to  microservice geolocation repo ](https://github.com/corsearch/service_geolocation)

- [to the chassis repo ](https://github.com/CloudBats/fastapi-holistic/)

- [to LocationIQ  documentation ](https://github.com/location-iq/locationiq-python-client)


