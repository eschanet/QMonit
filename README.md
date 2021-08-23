[![CI status](https://github.com/eschanet/QMonit/workflows/tests/badge.svg)](https://github.com/eschanet/QMonit/actions?query=workflow%3Atests)
[![Python 2.7](https://img.shields.io/badge/python-2.7-blue.svg)](https://www.python.org/downloads/release/python-270/)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://lbesson.mit-license.org/)
[![GitHub release](https://img.shields.io/github/v/release/eschanet/qmonit)](https://github.com/eschanet/qmonit/releases/)
[![GitHub tag](https://img.shields.io/github/tag/eschanet/qmonit.svg)](https://github.com/eschanet/qmonit/tags/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)



# Low-latency PanDA queue monitoring

This repository gathers the various scripts and tools that are necessary for QMonit &mdash; a lightweight monitoring tool for PanDA queues with a latency of only 10 minutes.

# Monitoring

The data in the InfluxDB instance is being visualised in [MONIT Grafana](https://monit-grafana.cern.ch/d/000000301/home?orgId=17).

At the moment, there are three main dashboards that are being fed by QMonit:
- [Jobs Monitoring](https://monit-grafana.cern.ch/d/VbKvjL2Zk/jobs-monitoring?orgId=17) : Low-latency monitoring of all ATLAS sites based on queue-level information from PanDA.
- [Suspicious sites](https://monit-grafana.cern.ch/d/LZifjLhZk/suspicious-sites?orgId=17) : Built on top of the same low-latency information from PanDA, showing you sites where the current number of jobs varies drastically from their moving average.
- [DAOD distribution](https://monit-grafana.cern.ch/d/tIMFCL2Zk/daod-distribution?orgId=17) : Are we placing DAODs optimally on the grid? This dashboard compares analysis jobs running against each datadisk with the amount of DAODs placed on the datadisks.

A run-down of the dashboards and the entire setup is given in [COMING SOON]().

### Technical setup

Below is a figure of the rough technical setup of this project. The green blob `cron jobs` contains all the code that is tracked in this repository and is responsible for uploading, downloading and downsampling data as well as computing derived quantities that need to be readily available.

The cron jobs can be run on any machine, provided that you have the MySQL and InfluxDB python clients available, as well as a connection to the PanDA server (more on this below).

![Technical details](commonHelpers/img/technical_details.png?raw=true "Technical details")

### Want to run it on your own?

You don't need to. This setup runs on an ADC Service Account on lxplus, so you don't have to deal with any of this. If you want to run it anyway, here is a couple of things you'll need.

1. Clone the repository:  
<pre><code>git clone ssh://git@gitlab.cern.ch:7999/eschanet/QMonit.git</code></pre>

2. Get the InfluxDB and MySQL python clients  
<pre><code>pip install influxdb
pip install mysql-connector</code></pre>

3. Add the necessary cron jobs to your crontab  
<pre><code>#Getting my actual data for InfluxDB   
0 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run.sh >/dev/null 2>&1  
10 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run.sh >/dev/null 2>&1  
20 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run.sh >/dev/null 2>&1
30 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run.sh >/dev/null 2>&1
40 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run.sh >/dev/null 2>&1
50 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run.sh >/dev/null 2>&1
#I also need to write some datadisk stuff once in a while
5 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run_1h.sh >/dev/null 2>&1
#Downsampling is a pain in InfluxDB using CQs, so lets do it manually
15 * * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run_downsample_1h.sh >/dev/null 2>&1
6 0 * * * lxplus.cern.ch &lt;path_to_QMonit_base>/run_downsample_1d.sh >/dev/null 2>&1
</code></pre>

4. Last but not least, you'll need credentials to the databases. These are of course not included in this repository, but should, in general, be provided using a `config.cfg` file, placed at the base of the `QMonit` directory and providing the following:
<pre><code>[credentials]
password = &lt;idb_and_mysql_pwd>
username = &lt;idb_and_mysql_user>
database = &lt;idb_and_mysql_db>
<br/>
[credentials_elasticsearch]
password = &lt;es_pwd>
username = &lt;es_user>
host = &lt;es_host>
<br/>
[credentials_monit_grafana]
token = &lt;token>
url = &lt;url></code></pre>
