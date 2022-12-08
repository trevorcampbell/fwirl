import click

@click.group()
def cli():
    pass

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

@click.command()
def list():
    click.echo("Listing graphs!")

@click.command()
def summarize():
    click.echo("Summarizing a graph!")



cli.add_command(pause)
cli.add_command(clear_failure)
cli.add_command(refresh)
cli.add_command(build)
cli.add_command(summarize)
cli.add_command(schedule)
