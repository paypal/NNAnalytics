# NameNode Analytics

"A Standby read-only HDFS NameNode, with no RPC server, that services clients over a REST API, utilizes Java 8 Stream API, all for the purpose of performing large and complicated scans of the entire file system metadata for end users."

## Current supported Hadoop versions:

1. 2.4.x (run with -PhadoopVersion=2.4.0)
2. 2.6.x (run with -PhadoopVersion=2.6.1) -- 2.6.0 had a bug that prevented keytab re-log.
3. 2.7.x (run with -PhadoopVersion=2.7.0)
3. 2.8.x (run with -PhadoopVersion=2.8.0)
3. 2.9.x (run with -PhadoopVersion=2.9.0)
3. 3.0.x (run with -PhadoopVersion=3.0.0)

If you are building for 2.5.x you can re-use the 2.4.x build.

## How to test locally:

1. (Temporary measure) Create a directory for NNA to use: `mkdir /usr/local/nn-analytics`.
2. Run the `public static void main` method in `TestNNAnalytics.java` under `src/test/java`.
3. A local instance of NNA should start and be accessible at http://localhost:4567.

## How to build:

1. To build the RPM: `./gradlew clean buildRpm -PhadoopVersion=X.X.X`
2. Find the RPM in: `cd build/distributions/`

## How to install:

1. SCP the RPM to any client or server node.
2. Install RPM on host: `yum install <rpm_path>`
3. `cd /usr/local/nn-analytics/` on host and verify installation.
4. Make sure `db/` and `dfs/name/` have plenty of hard drive space. You can create shortcuts instead to other mounts if needed.

## How to configure:

1. SCP over configurations from your Standby NameNode into `/usr/local/nn-analytics/config`
2. Modify your `hdfs-site.xml` and ensure that Kerberos information is valid. NNA requires a keytab with `nn` user principal if you are using Kerberos. Just like a real NameNode.
3. Modify `security.properties` and set up LDAP authentication, SSL, and authorization.
3. Ensure that `security.properties` file is only read-able by hdfs and root users: `chown hdfs: security.properties && chmod 400 security.properties`

## How to run:

0. (Optional) Check out `/usr/local/nn-analytics/bin/nn_loader` and observe that the JVM set-up is adequate for your needs. Modify the shell scripts as necessary for your environment. It is possible to change the classpath here and point to your own HDFS installations.
1. As root, run: `service nn-analytics start`
2. (Optional) On your browser hit: `http(s)://HOSTNAME:8080/namespace.html` and fetch the latest image and then `service nn-analytics restart` if this is your first run.
3. If any issues occur check `/var/log/nn-analytics` for logs.
