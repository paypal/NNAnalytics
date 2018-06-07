## How to run:

0. (Optional) Check out `/usr/local/nn-analytics/bin/nn_loader` and observe that the JVM set-up is adequate for your needs. Modify the shell scripts as necessary for your environment. It is possible to change the classpath here and point to your own HDFS installations.
1. As root, run: `service nn-analytics start`
2. (Optional) On your browser hit: `http(s)://HOSTNAME:8080/namespace.html` and fetch the latest image and then `service nn-analytics restart` if this is your first run.
3. If any issues occur check `/var/log/nn-analytics` for logs.