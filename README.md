[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build Status](https://github.com/paypal/NNAnalytics/actions/workflows/gradle.yml/badge.svg?branch=master)](https://github.com/paypal/NNAnalytics/actions/workflows/gradle.yml)
[![Documentation Status](https://readthedocs.org/projects/nnanalytics/badge/?version=latest)](https://nnanalytics.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/paypal/NNAnalytics/branch/master/graph/badge.svg)](https://codecov.io/gh/paypal/NNAnalytics)
[![Join the chat at https://gitter.im/NNAnalytics/Lobby](https://badges.gitter.im/NNAnalytics/Lobby.svg)](https://gitter.im/NNAnalytics/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# <img src="docs/images/NNA-logo.png" width="174" height="120" />

"A Standby read-only HDFS NameNode, with no RPC server, that services clients over a REST API, utilizes Java 8 Stream API, all for the purpose of performing large and complicated scans of the entire file system metadata for end users."

Run a demo locally and instantly! Just run the following command on a workspace directory:
```
git clone https://github.com/paypal/NNAnalytics.git nna && cd ./nna && ./gradlew -PmainClass=org.apache.hadoop.hdfs.server.namenode.analytics.TestWithMiniClusterWithStreamEngine execute
```
Then go to http://localhost:4567 and you will have an NNA instance complete with a mini HA-enabled HDFS instance all updating in real time on your local machine!

__________________________________________________________________________________________________________________

# Architecture: Legacy vs NNA

<img src="docs/images/NNA-arch-1.png" width="917" height="478" />
<img src="docs/images/NNA-arch-2.png" width="861" height="526" />

__________________________________________________________________________________________________________________

# Documentation & Getting Started

  * [Click here to read the docs](http://nnanalytics.readthedocs.io/)

__________________________________________________________________________________________________________________

# Presentations

  * [SlideShare](https://www2.slideshare.net/PlamenJeliazkov/namenode-analytics-querying-hdfs-namespace-in-real-time)
  * [DataWorks Summit](https://www.youtube.com/watch?v=9xlB5C88tbk)

__________________________________________________________________________________________________________________
