Adam gene features counter
==========================

This app has been created to learn ADAM. At the moment it only opens a gtf file and prints some features.


Gettings started
----------------

To run this app:

1) Configure input and output path as well as gtf file name:

2) Start spark 1.20 (build for Hadoop 2.4) in a standalone mode:

3) Open an sbt console:

$ sbt

3) In sbt console assembly jar to be send to Spark:

$ assembly

4) Run the project from sbt console:

$ run