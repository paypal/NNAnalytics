**Log:**

`/log` is a GET only call that only ADMIN users can access.
It takes an optional `?limit=<numOfChars>` argument that represents a number of characters to read from the bottom-up of the log.

Response code is 200 and a plaintext dump of the NNA log.

Response code of 403 means you are not authorized to view this endpoint.