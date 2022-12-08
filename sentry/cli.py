import click
from .api import summarize as api_summarize


__RABBIT_URL__ = "amqp://guest:guest@localhost//"

@click.group()
def cli():
    pass

@click.command()
@click.argument("graph_key")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def summarize(graph_key, rabbit_url):
    api_summarize()

@click.command()
def list():
    click.echo("Listing graphs!")

@click.command()
def pause():
    click.echo("Pausing!")

@click.command()
def clear_failure():
    click.echo("Clearing failure!")

@click.command()
def refresh():
    click.echo("Refreshing status!")

@click.command()
def build():
    click.echo("Building!")

@click.command()
def schedule():
    click.echo("Scheduling!")



cli.add_command(summarize)
cli.add_command(pause)
cli.add_command(clear_failure)
cli.add_command(refresh)
cli.add_command(build)
cli.add_command(schedule)
