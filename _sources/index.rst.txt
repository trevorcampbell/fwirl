fwirl
=====

.. toctree::
   :maxdepth: 2
   :caption: Contents

   quickstart
   api
   cli

fwirl is a lightweight Python library for building and maintaining collections
of *assets* — any artifact (file, database record, in-memory object, …) that
has a notion of being *current* or *stale* relative to its upstream
dependencies.  It handles dependency tracking, status refreshes, cron-based
scheduling, optional notifications, and a RabbitMQ-based remote control
interface, so you can focus on defining *what* to build rather than *when* and
*how* to trigger builds.
