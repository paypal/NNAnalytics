**How To Configure:**

0. Check `/usr/local/nn-analytics/bin/nna_env` and update the JVM settings as you wish to meet your needs.

The general rule of thumb is to configure your NNA JVM settings exactly as you would your Standby NameNode.
Typically it is good to follow a sizing guide to configure your heap as a function of your file system load.
For example:

| Number of Files | FsImage | Java Heap (Xmx + Xms) | Young Gen (NewSize + MaxNewSize) |
| --------------- | ------- | :--------------------:| --------------------------------:|
| <1mil - 5mil    | 1-4 GB  | 3379m                 | 512m                             |
| 5mil - 20mil    | 4-7 GB  | 10982m                | 1280m                            |
| 20mil - 50mil   | 7-10 GB | 26752m                | 3072m                            |
| 50mil - 100mil  | 10+ GB  | 52659m                | 6144m                            |
| 100mil - 150mil | 15+ GB  | 78566m                | 8960m                            |
| 150mil - 250mil | 25+ GB  | 204800m               | 10240m                           |
| 300mil+         | 30+ GB  | 244800m               | 25600m                           |

1. SCP over configurations from your Standby NameNode into `/usr/local/nn-analytics/config`
2. Modify your `hdfs-site.xml` and ensure that Kerberos information is valid. NNA requires a keytab with `nn` user principal if you are using Kerberos. Just like a real NameNode.
3. Modify `application.properties` and set up LDAP authentication, SSL, and authorization.
4. Ensure that `application.properties` file is only read-able by hdfs and root users: `chown hdfs: application.properties && chmod 400 application.properties`

Definitions of configuration NNA-specific properties:

* `nna.port=<integer>` - Default is 8080. Represents the main web UI port.
* `nna.historical=<true | false>` - Default is false. True enables a locally embedded HSQL DB to trend data. Not recommended in production.
* `nna.support.bootstrap.overrides=<true | false>` - Default is true. True will override certain hdfs-site.xml configurations to prevent NNA from communicating with the active cluster. False means it will use configurations as-is. Recommended true in production.
* `nna.suggestions.reload.sleep.ms=<integer>` - Default is 900000.
* `ldap.enable=<true | false>` - Default is false. True enables LDAP authentication.
* `ldap.trust.store.path=<file path as file:/path/to/store>` - Default is empty.
* `ldap.trust.store.password=<password>` - Default is empty. If set, ensure file has 400 permissions.
* `ldap.use.starttls=<true | false>` - Default is false.
* `ldap.url=<ldap://host:port>` - Default is empty.
* `ldap.base.dn.1=<uid=%u,ou=example,dc=test>` - %u will be replaced with username. More DNs to try can be added with .2, .3, etc.
* `ldap.connect.timeout=<integer>` - Default is 1000.
* `ldap.response.timeout=<integer>` - Default is 1000.
* `ldap.connection.pool.min.size=<integer>` - Default is 1.
* `ldap.connection.pool.max.size=<integer>` - Default is 2.
* `ssl.keystore.path=<file path as file:/path/to/store>` - Default is empty. If set, SSL will be enabled.
* `ssl.keystore.password=<password>` - Default is empty. If set, ensure file has 400 permissions.
* `jwt.signature.secret=<string of 32 numbers>` - Default is 11111111111111111111111111111111.
* `jwt.encryption.secret=<string of 16 numbers>` - Default is 0000000000000000.
* `authorization.enable=false` - Default is false. True enables NNA authorization.
* `nna.admin.users=<comma-seperated list of usernames>` - An * enables all users as ADMINs.
* `nna.write.users=<comma-seperated list of usernames>` - An * enables all users as WRITERs.
* `nna.readonly.users=<comma-seperated list of usernames>` - An * enables all users as READERs.
* `nna.cache.users=<comma-seperated list of usernames>` - An * enables all users as CACHE users.
* `nna.localonly.users=<comma-seperated list of username:password pairs>` - Local-only accounts; recommended for any applications that intend to use NNA API.
* `nna.query.engine.impl=<string>` - The full canonical class name of the QueryEngine implementation to use. Current existing implementations are `org.apache.hadoop.hdfs.server.namenode.JavaStreamQueryEngine` (recommended and the default) and `org.apache.hadoop.hdfs.server.namenode.JavaCollectionQEngine` (currently experimental).

** If you have a `/usr/local/nn-analytics/config/security.properties` file please rename it to `/usr/local/nn-analytics/config/application.properties`. The `security.properties` file is now deprecated. Eventually the new `application.properties` file will also be moved to an XML file in the style of other Hadoop ecosystem configurations.

*Below is additional experimental configuration.*

If you are using a Hadoop 3.x build of NNA, it is possible to override the `NNA_MAIN` export in `/usr/local/nn-analytics/bin/nna_env` to point to `org.apache.hadoop.hdfs.server.namenode.analytics.HadoopWebServerMain`. 
This will launch NNA in a Jersey HTTP Server just like the NameNode uses as opposed to the SparkJava HTTP Server. There should be no difference in REST API usage.