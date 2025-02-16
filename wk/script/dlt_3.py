import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


@dlt.resource(name="character_rickandmorty")
def character():
    client = RESTClient(
        base_url="https://rickandmortyapi.com",
        paginator=PageNumberPaginator(base_page=1, total_path="info.pages"),
        data_selector="results"
    )

    for page in client.paginate("/api/character"):    
        yield page


pipeline = dlt.pipeline(
    pipeline_name="pipeline_rickandmorty_bq",
    destination="bigquery",
    dataset_name="rickandmorty_dataset"
)


load_info = pipeline.run(
    character, 
    write_disposition="replace"
)

print(load_info)