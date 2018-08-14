**How To Configure:**

1. SCP over configurations from your Standby NameNode into `/usr/local/nn-analytics/config`
2. Modify your `hdfs-site.xml` and ensure that Kerberos information is valid. NNA requires a keytab with `nn` user principal if you are using Kerberos. Just like a real NameNode.
3. Modify `security.properties` and set up LDAP authentication, SSL, and authorization.
4. Ensure that `security.properties` file is only read-able by hdfs and root users: `chown hdfs: security.properties && chmod 400 security.properties`

Definitions of configuration NNA-specific properties:

* `nna.port=<integer>` - Default is 8080. Represents the main web UI port.
* `nna.historical=<true | false>` - Default is false. True enables a locally embedded HSQL DB to trend data. Not recommended in production.
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
* `ssl.keystore.path=<file path as file:/path/to/store>`
* `ssl.keystore.password=<password>` - Default is empty. If set, ensure file has 400 permissions.
* `jwt.signature.secret=<string of 32 numbers>` - Default is 11111111111111111111111111111111.
* `jwt.encryption.secret=<string of 16 numbers>` - Default is 0000000000000000.
* `authorization.enable=false` - Default is false. True enables NNA authorization.
* `nna.admin.users=<comma-seperated list of usernames>` - An * enables all users as ADMINs.
* `nna.write.users=<comma-seperated list of usernames>` - An * enables all users as WRITERs.
* `nna.readonly.users=<comma-seperated list of usernames>` - An * enables all users as READERs.
* `nna.cache.users=<comma-seperated list of usernames>` - An * enables all users as CACHE users.
* `nna.localonly.users=<comma-seperated list of username:password pairs>` - Local-only accounts; recommended for any applications that intend to use NNA API.