**Config:**

`/config` is a GET only call that only CACHE users and higher can access.

Response code is 200 and an XML representation of the HDFS configurations used to bootstrap.

Response code of 403 means you are not authorized to view this endpoint.