import click
import polars as pl


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


if __name__ == "__main__":
    schools()
