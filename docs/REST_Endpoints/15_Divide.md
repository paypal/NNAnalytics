**Divide:**

*This is an experimental API and likely to be replaced in the future.*

`/divide` is a GET only call that only READER users can access.
It takes several required parameters named `?set1=<files|dirs>&filters1=<filter>:<filterOps>&sum1=<sum>&?set2=<files|dirs>&filters2=<filter>:<filterOps>&sum2=<sum>`.

Experimentally, there are parameters for sending out an email of the response: `&emailTo=<toAddress>&emailCc=<ccAddresses>&emailFrom=<fromAddress>&emailHost=<emailServerAddress>&emailConditions=<filter>:<filterOps>`.

Response code is 200 and a single numerical value representing the ratio of query1 divided by query2.

Response code of 403 means you are not authorized to view this endpoint.