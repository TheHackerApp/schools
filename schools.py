from dataclasses import dataclass

import click
import polars as pl
from algoliasearch.search_client import SearchClient
from dotenv import load_dotenv


@click.group
@click.option(
    "--source",
    "-s",
    type=click.Path(exists=True),
    help="The source file to read from",
    default="schools.csv",
)
@click.pass_context
def schools(ctx: click.Context, source: str):
    """
    Manage verified schools
    """
    load_dotenv()

    ctx.obj = pl.scan_csv(source)


@schools.command
@click.argument("output", type=click.Path(allow_dash=True))
@click.pass_obj
def for_database(obj: pl.LazyFrame, output: str):
    """
    Format the school data for the database
    """
    df = obj.select("id", "name").collect()

    if output == "-":
        print(df.write_csv())
    else:
        df.write_csv(output)


@dataclass
class SearchContext:
    schools: pl.LazyFrame
    client: SearchClient


@schools.group
@click.option(
    "--app-id",
    help="The Algolia application ID",
    envvar="ALGOLIA_APP_ID",
    metavar="APP_ID",
)
@click.option(
    "--api-key",
    help="The Algolia API key",
    envvar="ALGOLIA_API_KEY",
    metavar="API_KEY",
)
@click.pass_context
def search(ctx: click.Context, app_id: str | None, api_key: str | None):
    """
    Manage the search index
    """
    client = SearchClient.create(app_id, api_key)
    ctx.obj = SearchContext(schools=ctx.obj, client=client)


@search.command
@click.option("--name", "-n", help="The name of the index", default="schools")
@click.pass_obj
def initialize(obj: SearchContext, name: str):
    """
    Initialize the index
    """
    index = obj.client.init_index(name)
    index.set_settings(
        {
            "searchableAttributes": ["name", "abbreviations,alternatives"],
            "indexLanguages": ["en"],
            "queryLanguages": ["en"],
            "hitsPerPage": 5,
            "paginationLimitedTo": 50,
        }
    )


@search.command
@click.option("--name", "-n", help="The name of the index", default="schools")
@click.pass_obj
def seed(obj: SearchContext, name: str):
    """
    Seed the index with the schools
    """
    records = (
        obj.schools.rename({"id": "objectID"})
        .with_columns(
            pl.col("abbreviations")
            .str.replace("\\{", "[")
            .str.replace("\\}", "]")
            .str.json_decode(dtype=pl.List(str)),
            pl.col("alternatives")
            .str.replace("\\{", "[")
            .str.replace("\\}", "]")
            .str.json_decode(dtype=pl.List(str)),
        )
        .collect()
        .to_dicts()
    )

    index = obj.client.init_index(name)
    index.save_objects(records)


if __name__ == "__main__":
    schools()
