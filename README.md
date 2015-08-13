Phoenix
======================

Phoenix is a Secor Mesos Framework.

Build
-------------
Assuming $WORKING_DIR is you repos directory.

To build the fatjar:
    
    # cd $WORKING_DIR && git clone https://github.com/stealthly/phoenix.git phoenix   
    # cd phoenix && ./gradlew jar

To run the phoenix scheduler you will need Secor tar archive:
    
    # cd $WORKING_DIR && git clone https://github.com/pinterest/secor.git secor
    # cd secor && mvn clean package

You can find archive `secor-0.2-SNAPSHOT-bin.tar.gz` under `secor/target`.

Environment Configuration
--------------------------

Before running phoenix, set the location of libmesos:

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

If the host running scheduler has several IP addresses you may also need to

    # export LIBPROCESS_IP=<IP_ACCESSIBLE_FROM_MASTER>
    
You will also need java in you PATH.

Scheduler Configuration
----------------------

The scheduler is configured through the command line.

Following options are available:
```
Usage: scheduler [options]

  -c <value> | --config <value>
        Path to config file. Required.
  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if defined in config if EM_API env is set.
  --aws-access-key <value>
        Aws access key. Optional if defined in config file or env AWS_ACCESS_KEY_ID is set.
  --aws-secret-key <value>
        Aws secret key. Optional if defined in config file or env AWS_SECRET_ACCESS_KEY is set.

```

Run the scheduler
-----------------

Before running the scheduler ensure you have secor archive (`secor-0.2-SNAPSHOT-bin.tar.gz`) and phoenix jar file (`phoenix-0.1-SNAPSHOT.jar`)
in `dist` directory - scheduler will look for artifacts in this directory. 

    # java -jar -Dlog4j.configuration=phoenix-log4j.properties phoenix-0.1-SNAPSHOT.jar scheduler -c phoenix.properties -a master:7000

You can find default log4j and phoenix config file under `src/main/resources` in this project. 

Quick start
-----------

In order not to pass the API url to each CLI call lets export the URL as follows:

```
# export EM_API=http://master:7000
```

To start one secor task with default configuration 

```
# java -jar -Dlog4j.configuration=phoenix-log4j.properties phoenix-0.1-SNAPSHOT.jar add --id 0
Added server 0

cluster:
server:
  id: 0
  state: added
  server request template:
  cpu: None
  mem: None
  config overrides: Map()
```

You now have a cluster with 1 server that is added with default params, it will be started once mesos have resources to satisfy default task parameters.

To check the status:

```
# java -jar -Dlog4j.configuration=phoenix-log4j.properties phoenix-0.1-SNAPSHOT.jar status
cluster:
server:
  id: 0
  state: running
  server request template:
  cpu: Some(0.5)
  mem: Some(256.0)
  config overrides: Map()

```

To delete the secor task (will stop and remove the server):

```
# .java -jar -Dlog4j.configuration=phoenix-log4j.properties phoenix-0.1-SNAPSHOT.jar delete --id 0
Deleted server 0
```

Navigating the CLI
==================

Requesting help
---------------

```
# java -jar phoenix-0.1-SNAPSHOT.jar help
Usage: <command>

Commands:
  help       - print this message.
  help [cmd] - print command-specific help.
  scheduler  - start scheduler.
  status     - print cluster status.
  add        - add servers to cluster.
  delete     - delete servers in cluster.
```

Adding servers to the cluster
-------------------------------

```
# java -jar phoenix-0.1-SNAPSHOT.jar help add
Usage: add [options]

  -i <value> | --id <value>
        Server id. Required.
  -c <value> | --cpu <value>
        CPUs for server. Optional.
  -m <value> | --mem <value>
        Memory for server. Optional.
  --override Secor config override k1=v1,k2=v2...
        
  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.


override config example examples (see Secor documentation for more details):
  secor.max.file.size.bytes=100000    
  secor.consumer.threads=5                      
```

Removing servers from the cluster
----------------------------------

```
# java -jar phoenix-0.1-SNAPSHOT.jar help delete
Usage: delete [options]

  -i <value> | --id <value>
        CPUs for server. Required.
  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
```

Troubleshooting
==================

Goto Mesos UI (<mesos-master-host>:5050) check the framework (default secor-mesos) and its tasks.

Logs in task's sandbox: `stdout`, `stderr` and `secor-secor_backup.log` may be helpful.