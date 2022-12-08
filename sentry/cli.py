import click

@click.command()
def hello():
    click.echo("Hello world!")

# TODO:
# - pause asset (certain/all assets)
# - clear failure (certain/all assets)
# - update status now (certain/all assets)
# - run build now (certain/all assets)
# - print summary of status
# - update schedule for an ongoing build/update task

