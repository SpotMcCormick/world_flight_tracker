import requests
import polars as pl

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position",
    "last_contact", "longitude", "latitude", "baro_altitude",
    "on_ground", "velocity", "true_track", "vertical_rate",
    "sensors", "geo_altitude", "squawk", "spi", "position_source"
]

url = "https://opensky-network.org/api/states/all"
# params = {
#     "lamin": 24.5,
#     "lomin": -125.0,
#     "lamax": 49.5,
#     "lomax": -66.5
# }

response = requests.get(url)
data = response.json()

rows = [dict(zip(FIELDS, state)) for state in data["states"]]
df = pl.DataFrame(rows)

print(df.head())
print(df.shape)
df.write_csv("data.csv")