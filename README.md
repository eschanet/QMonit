## What is this repository?

This repository gathers the various scripts and tools that are necessary for QMonit.

### Monitoring

The data in the InfluxDB instance is being visualised in [MONIT Grafana](https://monit-grafana.cern.ch/d/000000301/home?orgId=17).

At the moment, there are three main dashboards that are being fed by QMonit:
- [Jobs Monitoring](https://monit-grafana.cern.ch/d/VbKvjL2Zk/jobs-monitoring?orgId=17) : Low-latency monitoring of all ATLAS sites based on queue-level information from PanDA.
- [Suspicious sites](https://monit-grafana.cern.ch/d/LZifjLhZk/suspicious-sites?orgId=17) : Built on top of the same low-latency information from PanDA, showing you sites where the current number of jobs varies drastically from their moving average.
- [DAOD distribution](https://monit-grafana.cern.ch/d/tIMFCL2Zk/daod-distribution?orgId=17) : Are we placing DAODs optimally on the grid? This dashboard compares analysis jobs running against each datadisk with the amount of DAODs placed on the datadisks.

A run-down of the dashboards and the entire setup is given in [COMING SOON]().

### Technical setup

![Technical details](commonHelpers/img/technical_details.pdf?raw=true "Technical details")


### Want to run it on your own?





### How to use the stuff in this repository

Pretty simple. The `run.sh` script is executed through a cron job and is responsible for setting up the right environment and executing the `get_job_stats.py` script, which actually gathers the data using the `Client` class from `Client.py`.
