Adam gene features counter
==========================

This app has been created to learn ADAM. At the moment it only opens a gtf file and prints some features.


Gettings started
----------------

To run this app:
1) Configure input and output path as well as gtf file name
2) Start spark 1.20 (build for Hadoop 2.4) in a standalone mode
3) Open sbt console:
$ sbt
3) (in sbt console) package a jar
$ package
4) (in sbt console) send the task
$ run