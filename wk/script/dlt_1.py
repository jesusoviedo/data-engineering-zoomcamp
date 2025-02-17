import pandas as pd
import dlt
from dlt.sources.helpers import requests

# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data'
)

# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    data.append(response.json())

# Extract, normalize, and load the data
pipeline.run(data, table_name='player', write_disposition="replace")


columns = [
    'avatar_url', 'player_id', 'profile_url', 'member_url', 'name', 'username', 
    'title', 'followers', 'country_url', 'country', 'last_online', 'joined', 
    'status', 'is_streamer', 'verified', 'league', 'timestamp', 'session_id'
]

with pipeline.sql_client() as client:
    query = "SELECT * FROM player" 
    result = client.execute(query)

    df = pd.DataFrame(result.fetchall(), columns=columns)

print(df)