**Token:**

`/token` is a GET only call that only CACHE users can access.

Response code is 200 and a JSON dump of mapping users to the last known timestamp when a delegation token was issued to them.

Response code of 403 means you are not authorized to view this endpoint.