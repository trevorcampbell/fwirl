.. _cli:

Command-Line Interface
======================

fwirl ships with a CLI for controlling a running :class:`~fwirl.AssetGraph`
server over RabbitMQ.  Every command accepts a ``--rabbit_url`` option to
specify a non-default broker URL.

Usage::

   fwirl <command> [OPTIONS] GRAPH

Graph key
---------

``GRAPH`` is the string key passed to :class:`~fwirl.AssetGraph` when the
server was started.

Commands
--------

``summarize``
~~~~~~~~~~~~~

Print a human-readable status summary of the running graph::

   fwirl summarize my_graph

``ls``
~~~~~~

List assets, schedules, and/or jobs::

   fwirl ls my_graph [--assets] [--schedules] [--jobs]

``build``
~~~~~~~~~

Trigger a build pass::

   fwirl build my_graph [--asset ASSET_KEY]

``refresh``
~~~~~~~~~~~

Trigger a status refresh without rebuilding::

   fwirl refresh my_graph [--asset ASSET_KEY]

``pause``
~~~~~~~~~

Pause an asset or schedule::

   fwirl pause my_graph KEY

``unpause``
~~~~~~~~~~~

Resume a paused asset or schedule::

   fwirl unpause my_graph KEY

``schedule``
~~~~~~~~~~~~

Add a recurring schedule::

   fwirl schedule my_graph SCHEDULE_KEY ACTION CRON_STRING [--asset ASSET_KEY]

* ``ACTION`` is either ``build`` or ``refresh``.
* ``CRON_STRING`` is a standard five-field cron expression, e.g.
  ``"0 6 * * *"`` for 06:00 every day.

``unschedule``
~~~~~~~~~~~~~~

Remove an existing schedule::

   fwirl unschedule my_graph SCHEDULE_KEY

``shutdown``
~~~~~~~~~~~~

Send a graceful shutdown signal to the graph server::

   fwirl shutdown my_graph

``webserver start`` / ``webserver stop``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start or stop the fwirl web server (provides a graphical status view)::

   fwirl webserver start
   fwirl webserver stop
