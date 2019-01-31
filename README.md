## What is this repository?

This repository gathers the various scripts and tools that are necessary for writing data from PanDA to my InfluxDB instance.

### Access InfluxDB instance

The InfluxDB instance is hosted at CERN and can be accessed through 

`influx -username <username> -password '' -ssl -host dbod-eschanet.cern.ch -port 8080 -unsafeSsl`

If you have no access credentials, please ask me.

### Monitoring

The data in the InfluxDB instance is being visualised in a Grafana dashboard, also hosted at CERN.

The Grafana dashboard is accessible through CERN SSO at [this site](https://monit-eschanet.web.cern.ch).

### How to use the stuff in this repository

Pretty simple. The `run.sh` script is executed through a cron job and is responsible for setting up the right environment and executing the `get_job_stats.py` script, which actually gathers the data using the `Client` class from `Client.py`.


