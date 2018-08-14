**Filter:**

`/filter` is a GET only call that only READER users can access.
It takes several required parameters named `?set=<files|dirs>&filters=<filter>:<filterOps>&sum=<sum>`.
You may replace `sum` with a `find` if you wish. You may also perform multiple sums on the same filtered set; for example: `&sum=count,diskspaceConsumed`.
It also takes some optional parameters such as `&limit=<number>` to limit the size of the result set if you just want a small sample.

Experimentally, there are parameters for sending out an email of the response: `&emailTo=<toAddress>&emailCc=<ccAddresses>&emailFrom=<fromAddress>&emailHost=<emailServerAddress>&emailConditions=<filter>:<filterOps>`.

Dropping `sum` or `find` parameters entirely will output a list of paths.

Response code is 200 and either a single or multiple lines of numerical value(s) or a plaintext dump of INode paths representing the resulting set.

Response code of 403 means you are not authorized to view this endpoint.