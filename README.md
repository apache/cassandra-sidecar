# Apache Cassandra Sidecar [WIP]

This is a Sidecar for the highly scalable Apache Cassandra database.
For more information, see [the Apache Cassandra web site](http://cassandra.apache.org/) and [CIP-1](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=95652224).

**This is project is still WIP.**

Requirements
------------
  1. Java >= 1.8 (OpenJDK or Oracle), or Java 11
  2. Apache Cassandra 4.0.  We depend on virtual tables which is a 4.0 only feature.

Getting started
---------------

After you clone the git repo, you can use the gradle wrapper to build and run the project. Make sure you have 
Apache Cassandra running on the host & port specified in `conf/sidecar.yaml`.

    $ ./gradlew run
  
You can use `build`, `test` to build & test the project.

CircleCI Testing
-----------------

You will need to use the "Add Projects" function of CircleCI to set up CircleCI on your fork.  When promoted to create a branch, 
do not replace the CircleCI config, choose the option to do it manually.  CircleCI will pick up the in project configuration.


Wondering where to go from here?
--------------------------------
  * Join us in #cassandra on [ASF Slack](https://s.apache.org/slack-invite) and ask questions 
  * Subscribe to the Users mailing list by sending a mail to
    user-subscribe@cassandra.apache.org
  * Visit the [community section](http://cassandra.apache.org/community/) of the Cassandra website for more information on getting involved.
  * Visit the [development section](http://cassandra.apache.org/doc/latest/development/index.html) of the Cassandra website for more information on how to contribute.
