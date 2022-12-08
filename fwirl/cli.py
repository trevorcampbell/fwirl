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
@click.argument("graph")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def summarize(graph, rabbit_url):
    api_summarize(graph, rabbit_url)
cli.add_command(summarize)

@click.command()
@click.argument("graph")
@click.option("--assets", is_flag=True, default=False)
@click.option("--schedules", is_flag=True, default=False)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def ls(graph, assets, schedules, rabbit_url):
    api_ls(graph, assets, schedules, rabbit_url)
cli.add_command(ls)

@click.command()
@click.argument("graph")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def refresh(graph, rabbit_url):
    api_refresh(graph, rabbit_url)
cli.add_command(refresh)

@click.command()
@click.argument("graph")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def build(graph, rabbit_url):
    api_build(graph, rabbit_url)
cli.add_command(build)

@click.command()
@click.argument("graph")
@click.option("--asset", default=None)
@click.option("--schedule", default=None)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def pause(graph, asset, schedule, rabbit_url):
    api_pause(graph, asset, schedule, rabbit_url)
cli.add_command(pause)

@click.command()
@click.argument("graph")
@click.option("--asset", default=None)
@click.option("--schedule", default=None)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def unpause(graph, asset, schedule, rabbit_url):
    api_unpause(graph, asset, schedule, rabbit_url)
cli.add_command(pause)



@click.command()
@click.argument("graph")
@click.argument("schedule")
@click.argument("cron_string")
@click.option("--asset", default=None)
@click.option("--rabbit_url", default=__RABBIT_URL__)
def schedule(graph, schedule, cron_str, asset, rabbit_url):
    api_schedule(graph, schedule, cron_str, asset, rabbit_url)
cli.add_command(schedule)

@click.command()
@click.argument("graph")
@click.argument("schedule")
@click.option("--rabbit_url", default=__RABBIT_URL__)
def unschedule(graph, schedule, rabbit_url):
    api_unschedule(graph, schedule, rabbit_url)
cli.add_command(unschedule)

@click.command()
def clear_failure():
    click.echo("Clearing failure!")
cli.add_command(clear_failure)

