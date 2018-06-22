**How To Build:**

0. Check the [Supported Hadoop Versions](Supported_Hadoop_Versions/) page for what versions you can build for.
1. To build the RPM: `./gradlew clean buildRpm -PhadoopVersion=X.X.X`
2. Find the RPM in: `cd build/distributions/`