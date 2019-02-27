**How To Install:**

1. SCP the RPM to any client or server node.
2. Install RPM on host: `yum install <rpm_path>`
3. `cd /usr/local/nn-analytics/` on host and verify installation.
4. Make sure `db/` and `dfs/name/` have plenty of hard drive space. You can create shortcuts instead to other mounts if needed.

If you are upgrading to 1.6.x NNA (or higher) from a 1.5.x (or lesser) version, please make sure you change the main Java class, usually found scripts in `/usr/local/nn-analytics/bin` from `com.paypal.namenode.WebServerMain` to `org.apache.hadoop.hdfs.server.namenode.analytics.WebServerMain`.
