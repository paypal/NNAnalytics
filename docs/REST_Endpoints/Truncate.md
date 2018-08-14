**Truncate:**

`/truncate` is a GET only call that only ADMIN users can access.
It takes a required parameter `?table=<tableName>&limit=<numDaysToRetain>` which represents which embedded DB table you wish to truncate and the last number of days you wish to retain for that table.

Response code is 200.

Response code of 403 means you are not authorized to view this endpoint.