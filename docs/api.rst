API Reference
=============

Assets
------

.. autoclass:: fwirl.Asset
   :members:

.. autoclass:: fwirl.ExternalAsset
   :members:

.. autoclass:: fwirl.AssetStatus
   :members:
   :no-index:

Asset Graph
-----------

.. autoclass:: fwirl.AssetGraph
   :members:

Resources
---------

.. autoclass:: fwirl.resource.Resource
   :members:

Graph Workers
-------------

.. autoclass:: fwirl.worker.GraphWorker
   :members:

Schedules
---------

.. autoclass:: fwirl.schedule.Schedule
   :members:

Remote API
----------

These functions communicate with a running :class:`~fwirl.AssetGraph` server
over RabbitMQ.  They are also exposed as :ref:`CLI commands <cli>`.

.. autofunction:: fwirl.api.summarize
.. autofunction:: fwirl.api.ls
.. autofunction:: fwirl.api.build
.. autofunction:: fwirl.api.refresh
.. autofunction:: fwirl.api.pause
.. autofunction:: fwirl.api.unpause
.. autofunction:: fwirl.api.schedule
.. autofunction:: fwirl.api.unschedule
.. autofunction:: fwirl.api.shutdown
