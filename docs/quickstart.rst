Quickstart
==========

Installation
------------

.. code-block:: bash

   pip install fwirl

fwirl requires a running `RabbitMQ <https://www.rabbitmq.com/>`_ instance only
when using the :ref:`server mode <server-mode>` or the remote
:mod:`fwirl.api` / :ref:`CLI <cli>`.  Stand-alone scripted usage (calling
:meth:`~fwirl.AssetGraph.build` and :meth:`~fwirl.AssetGraph.refresh`
directly) works without RabbitMQ.

Defining assets
---------------

Subclass :class:`~fwirl.Asset` and implement two async methods:

* :meth:`~fwirl.Asset.timestamp` — return when the asset was last produced, or
  :attr:`~fwirl.AssetStatus.Unavailable` if it does not exist yet.
* :meth:`~fwirl.Asset.build` — produce (or re-produce) the asset.

.. code-block:: python

   import fwirl
   import pendulum as plm

   class MyAsset(fwirl.Asset):
       """A simple in-memory asset."""

       def __init__(self, key, dependencies):
           self._built = False
           super().__init__(key, dependencies)

       async def timestamp(self):
           if self._built:
               return self._ts
           return fwirl.AssetStatus.Unavailable

       async def build(self):
           # ... do the actual work here ...
           self._built = True
           self._ts = plm.now()

Building a graph
----------------

Create an :class:`~fwirl.AssetGraph`, add assets, and call
:meth:`~fwirl.AssetGraph.build`:

.. code-block:: python

   g = fwirl.AssetGraph("my_graph")

   root  = MyAsset("root",  [])
   child = MyAsset("child", [root])
   leaf  = MyAsset("leaf",  [child])

   g.add_assets([leaf])   # dependencies are discovered automatically
   g.build()              # builds root → child → leaf in order

You can also call :meth:`~fwirl.AssetGraph.refresh` first to check which
assets are stale without rebuilding them, then call
:meth:`~fwirl.AssetGraph.build` to rebuild only those that need it.

Scheduling
----------

Attach recurring schedules with :meth:`~fwirl.AssetGraph.schedule`:

.. code-block:: python

   # Refresh all asset statuses every minute, then build stale ones every hour
   g.schedule("refresh_minutely", "refresh", "* * * * *")
   g.schedule("build_hourly",     "build",   "0 * * * *")

.. _server-mode:

Server mode
-----------

Call :meth:`~fwirl.AssetGraph.run` to start the graph as a long-running
server that listens for commands on RabbitMQ:

.. code-block:: python

   g.run()   # blocks; use Ctrl-C or fwirl shutdown <graph_key> to stop

While the server is running you can control it from another process using the
:mod:`fwirl.api` module or the :ref:`fwirl CLI <cli>`.

External assets
---------------

Use :class:`~fwirl.ExternalAsset` for data that can change outside fwirl's
control (e.g. a file written by a third party).  fwirl polls the value at a
configurable interval and marks the asset stale when the value changes:

.. code-block:: python

   import pendulum as plm

   class FileExternalAsset(fwirl.ExternalAsset):
       def __init__(self, key, path, dependencies):
           self._path = path
           super().__init__(key, dependencies,
                            min_polling_interval=plm.duration(minutes=5))

       async def get(self):
           with open(self._path) as f:
               return f.read()

       def diff(self, val):
           return val != self._cached_val

Managed resources
-----------------

Attach shared resources (database connections, hardware locks, …) to assets
via the ``resources`` constructor argument.  fwirl calls
:meth:`~fwirl.Resource.init` before a build pass and
:meth:`~fwirl.Resource.close` afterwards:

.. code-block:: python

   class DBConnection(fwirl.Resource):
       def init(self):
           self.conn = connect_to_db()

       def close(self):
           self.conn.close()

   db = DBConnection("main_db")
   asset = MyDBAsset("db_asset", [], resources=[db])

Notifications
-------------

Pass a ``notifiers`` mapping to :class:`~fwirl.AssetGraph` to receive alerts
through any service supported by the
`notifiers <https://notifiers.readthedocs.io/>`_ library:

.. code-block:: python

   g = fwirl.AssetGraph("my_graph", notifiers={
       "slack": {
           "params": {"webhook_url": "https://hooks.slack.com/..."},
           "level": "ERROR",
       }
   })
