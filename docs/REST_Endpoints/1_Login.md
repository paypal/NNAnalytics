**Login:**

`/login` is a POST only call that expects a username and password to be provided as query parameters.
It will attempt to login using local accounts first. If no local account is found it will then attempt via whatever other authentication mechanism is enabled; (i.e. LDAP, etc).

Response code of 200 will return a JSON Web Token in the headers that the browser will cache as a cookie and can use to authenticate in place of username and password credentials.
If there is no JSON Web Token returned back that is because you have not enabled authentication; in which case all users will be "default_unsecured_user".

Response code of 401 means login credentials failed.