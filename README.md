# Apache Cassandra Sidecar [WIP]

This is a Sidecar for the highly scalable Apache Cassandra database.
For more information, see [the Apache Cassandra web site](http://cassandra.apache.org/) and [CIP-1](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=95652224).

**This is project is still WIP.**

Requirements
------------
  1. Java >= 1.8 (OpenJDK or Oracle), or Java 11
  2. Apache Cassandra 4.0.  We depend on virtual tables which is a 4.0 only feature.
  3. A Kubernetes cluster for running integration tests.  [MiniKube](https://kubernetes.io/docs/tutorials/hello-minikube/) can be used to run Kubernetes locally.

Getting started: Running The Sidecar
--------------------------------------

After you clone the git repo, you can use the gradle wrapper to build and run the project. Make sure you have 
Apache Cassandra running on the host & port specified in `conf/sidecar.yaml`.

    $ ./gradlew run
  
Configuring Cassandra Instance
------------------------------

While setting up cassandra instance, make sure the data directories of cassandra are in the path stored in sidecar.yaml file, else modify data directories path to point to the correct directories for stream APIs to work.

Testing
---------

We rely on Kubernetes for creating docker containers for integration tests.

The easiest way to get started locally is by installing [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/). 

Start minikube with a command similar to the following.  Use a netmask appropriate for your local network, and allow minikube to use as much RAM as you can afford to:

    minikube start --insecure-registry "192.168.0.0/16" --addons registry --memory 8G --cpus=4

This will create a MiniKube cluster using the default driver.  On OSX, this is hyperkit.

Enabling the tunnel is required in certain environments for tests to connect to the instances.

In a separate tab (or background process) run the following:

    minikube tunnel

Check the dashboard to ensure your installation is working as expected:
    
    minikube dashboard

Set the environment property for the Minikube container (we recommend you do this as part of your system profile):

You can use an existing Kubernetes environment by setting the appropriate project properties either through environment variables

    export SIDECAR_DOCKER_REGISTRY="http://$(minikube ip):5000"

Gradle will register the required test containers with the local docker registry.    You can enable this after setting up Minikube by doing the following:

*Note*: If using MiniKube, the Docker daemon will need to be configured to push to your Minikube repo insecurely.  
This should be added to the `daemon.json` config, usually found in /etc/docker, or in the Docker Engine section of the docker preferences:

      "insecure-registries": [
        "192.168.64.14:5000"
      ]
    
You can use `build`, `test` to build & test the project.

Please see the developer documentation in docs/src/development.adoc for more information.

CircleCI Testing
-----------------

You will need to use the "Add Projects" function of CircleCI to set up CircleCI on your fork.  When promoted to create a branch, 
do not replace the CircleCI config, choose the option to do it manually.  CircleCI will pick up the in project configuration.

Contributing
------------

We warmly welcome and appreciate contributions from the community. Please see [CONTRIBUTING.md](CONTRIBUTING.md)
if you wish to submit pull requests.

Wondering where to go from here?
--------------------------------
  * Join us in #cassandra on [ASF Slack](https://s.apache.org/slack-invite) and ask questions 
  * Subscribe to the Users mailing list by sending a mail to
    user-subscribe@cassandra.apache.org
  * Visit the [community section](http://cassandra.apache.org/community/) of the Cassandra website for more information on getting involved.
  * Visit the [development section](http://cassandra.apache.org/doc/latest/development/index.html) of the Cassandra website for more information on how to contribute.
  * File issues with our [Sidecar JIRA](https://issues.apache.org/jira/projects/CASSANDRASC/issues/)
