from sodapy import Socrata
import pandas as pd

cities: "dict[str, str]" = {"data.cityofnewyork.us":"dm9a-ab7w",
        "data.lacity.org":"nbyu-2ha9",
        "data.austintexas.gov": "3syk-w9eu",
        "data.cityofchicago.org": "ydr8-5enu",
        "data.mesaaz.gov": "2gkz-7z4f"}

def get_data():
    for city in cities:
        client = Socrata(city, None)
        results = client.get(cities[city])
        df = pd.DataFrame.from_dict(results)
        df.to_csv(f"raw_data/{city.split('.')[1]}.csv")
        print(f"Saved {city.split('.')[1]}.csv")

while 1:
    try:
        get_data()
        break
    except:
        pass
