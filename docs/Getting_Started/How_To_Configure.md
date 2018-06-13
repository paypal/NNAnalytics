**How To Configure:**

1. SCP over configurations from your Standby NameNode into `/usr/local/nn-analytics/config`
2. Modify your `hdfs-site.xml` and ensure that Kerberos information is valid. NNA requires a keytab with `nn` user principal if you are using Kerberos. Just like a real NameNode.
3. Modify `security.properties` and set up LDAP authentication, SSL, and authorization.
4. Ensure that `security.properties` file is only read-able by hdfs and root users: `chown hdfs: security.properties && chmod 400 security.properties`
