from models import Country, PostalCcode
import pycountry
from typing import List
from models import Country,BoundingBox,PostalCcode
import pandas as pd
from sqlmodel import Session, create_engine, select,SQLModel
import time

# sqlite_url = f"sqlite:///{sqlite_file_name}"
postgres_url = "postgresql://remi:pwd@localhost:5432/geolocation_data"
engine = create_engine(postgres_url, echo=False)

def populate_postalcode():
    with Session(engine) as session:
        with open("raw_data/all_postalcode.txt") as infile:
            counter = 0
            for line in infile:

                data = line.split("\t")
                print(data)
                print(len(data))

                postal_code = PostalCcode(
                    country_code=data[0],
                    postal_code=data[1],
                    place_name=data[2],
                    admin_name1=data[3],
                    admin_code1=data[4],
                    admin_name2=data[5],
                    admin_code2=data[6],
                    admin_name3=data[7],
                    admin_code3=data[8],
                    latitude= str(data[9]),
                    longitude=str(data[10]),
                    accuracy=data[11],
                )

                session.add(postal_code)

                counter = counter + 1

                if counter > 1000:
                    session.commit()
                    counter = 0

        session.commit()

def populate_country():
    with Session(engine) as session:
        with open("raw_data/country_names.csv") as infile:
            for line in infile:
                data = line.split("\t")
                if data[-1] == ("\n" or ""):
                    data = data[:-1]
                    print(data)
                    country = Country(
                        name=data[0],
                        country_code=data[1],
                        language=data[2],
                        country_name=data[3],
                        language_code=data[4],
                    )
                    session.add(country)
                    session.commit()

def add_pycountry_to_countries_db():
    potential_country_to_add = C = list(pycountry.countries)
    with Session(engine) as session:
        for country_ in potential_country_to_add:
            # check if country already in db:
            query = select(Country).where(
                Country.name == str(country_.name).capitalize())
            results = session.exec(query)
            if not (results):
                country = Country(
                    name=country_.name.lower(),
                    country_code=country_.alpha_2,
                    language=data[2],
                    country_name=country_.name.lower(),
                    language_code="English",
                )
                Session.add(country)
                Session.commit()


def populate_bbox():
    statement = """
            SELECT
                    postal_code ,
                    country_code,
                    max(longitude)AS max_long,
                    min(longitude)AS min_long,
                    max(latitude)AS max_lat,
                    min (latitude )AS min_lat,
                    CONCAT(postal_code ,'-',country_code  )AS ID
            FROM
                    postalccode p
            GROUP BY postal_code , country_code
        """
    with Session(engine) as session:
        results = session.execute(statement)
        results= results.fetchall()

        df  = pd.DataFrame(results)
        df['to be corrected'] = df['max_long'].eq(df['min_long'])
        BBox =   df[["id" , 'postal_code',"country_code" ,'to be corrected','min_long', 'max_long','min_lat', 'max_lat']]

        # to correct
        BBox_to_correct = BBox.loc[BBox['to be corrected'] == True]
        BBox_to_correct["max_lat"] = BBox_to_correct["max_lat"]+0.005  #  555 meters translation
        BBox_to_correct["max_long"] = BBox_to_correct["max_long"]+0.005  #  555 meters translation
        BBox_to_correct.reset_index()
        
        #  original
        BBox_original = BBox.loc[BBox['to be corrected'] == False]

        def commit_df(df):
            counter = 0
            for index, row in df.iterrows():
                session.add(BoundingBox( country_code = row["country_code"],
                                    postal_code = row["postal_code"],
                                    BoundingBox = [row["min_long"], row["max_long"],row["min_lat"],row["max_lat"]]))

                counter =counter +1
                if counter > 1000:
                    session.commit()
                    counter = 0
            session.commit()

        commit_df(BBox)
        commit_df(BBox_original)





if __name__ == '__main__':
    print( "Populating DB staarted"   )
    print( "######################"   )
    print ( " !!! it can last 2-4 min !!!" )
    print( "######################"   )
    time.sleep(3)

    populate_postalcode()

    populate_country()
    add_pycountry_to_countries_db()
    populate_bbox()

    print( "Populating DB finished "   )



