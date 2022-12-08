import click
from .api import summarize as api_summarize, 
                 ls as api_ls, 
                 refresh as api_refresh, 
                 build as api_build,
                 pause as api_pause,
                 unpause as api_unpause,
                 schedule as api_schedule,
                 unschedule as api_unschedule


__RABBIT_URL__ = "amqp://guest:guest@localhost//"

@click.group()
def cli():
    pass

@click.command()
@click.argument("graph_key")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def summarize(graph_key, rabbit_url):
    api_summarize(graph_key, rabbit_url)
cli.add_command(summarize)

@click.command()
@click.argument("graph_key")
@click.option("--assets", is_flag=True, default=False)
@click.option("--schedules", is_flag=True, default=False)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def ls(graph_key, assets, schedules, rabbit_url):
    api_ls(graph_key, assets, schedules, rabbit_url)
cli.add_command(ls)

@click.command()
@click.argument("graph_key")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def refresh(graph_key, rabbit_url):
    api_refresh(graph_key, rabbit_url)
cli.add_command(refresh)

@click.command()
@click.argument("graph_key")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def build(graph_key, rabbit_url):
    api_build(graph_key, rabbit_url)
cli.add_command(build)

@click.command()
@click.argument("graph_key")
@click.option("--asset", default=None)
@click.option("--schedule", default=None)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def pause(graph_key, asset, schedule, rabbit_url):
    api_pause(graph_key, asset, schedule, rabbit_url)
cli.add_command(pause)

@click.command()
@click.argument("graph_key")
@click.option("--asset", default=None)
@click.option("--schedule", default=None)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def unpause(graph_key, asset, schedule, rabbit_url):
    api_unpause(graph_key, asset, schedule, rabbit_url)
cli.add_command(pause)



@click.command()
@click.argument("graph_key")
@click.argument("schedule_key")
@click.argument("schedule")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def schedule(graph_key, schedule_key, schedule, rabbit_url):
    api_schedule(graph_key, schedule_key, schedule, rabbit_url)
cli.add_command(schedule)

@click.command()
@click.argument("graph_key")
@click.argument("schedule_key")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def unschedule(graph_key, schedule_key, rabbit_url):
    api_unschedule(graph_key, schedule_key, rabbit_url)
cli.add_command(unschedule)

@click.command()
def clear_failure():
    click.echo("Clearing failure!")
cli.add_command(clear_failure)

