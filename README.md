[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build Status](https://travis-ci.com/paypal/NNAnalytics.svg?branch=master)](https://travis-ci.com/paypal/NNAnalytics)
[![Documentation Status](https://readthedocs.org/projects/nnanalytics/badge/?version=latest)](https://nnanalytics.readthedocs.io/en/latest/)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/acc8afc858ff485ea67653b23c8ea82b)](https://github.com/paypal/NNAnalytics/pulls) 
[![Join the chat at https://gitter.im/NNAnalytics/Lobby](https://badges.gitter.im/NNAnalytics/Lobby.svg)](https://gitter.im/NNAnalytics/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# <img src="docs/images/NNA-logo.png" width="174" height="120" />

"A Standby read-only HDFS NameNode, with no RPC server, that services clients over a REST API, utilizes Java 8 Stream API, all for the purpose of performing large and complicated scans of the entire file system metadata for end users."

Run a demo locally and instantly! Just run the following command on a workspace directory:
```
git clone https://github.com/paypal/NNAnalytics.git nna && cd ./nna && ./gradlew -PmainClass=TestWithMiniCluster execute
```
Then go to http://localhost:4567 and you will have an NNA instance complete with a mini HA-enabled HDFS instance all updating in real time on your local machine!

__________________________________________________________________________________________________________________


# Documentation & Getting Started

  * [Click here to read the docs](http://nnanalytics.readthedocs.io/)

__________________________________________________________________________________________________________________
