**Top:**

`/top` is a GET only call that only READER users can access.
It takes an optional `?limit=<numOfUsers>` argument that represents a number of number of usernames desired for each issue.

Response code is 200 and a JSON dump of mapping some number of top users to issues.

Response code of 403 means you are not authorized to view this endpoint.