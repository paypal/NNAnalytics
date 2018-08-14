**Info:**

`/info` is a GET only call that only CACHE users and higher can access.

Response code is 200 and a plaintext dump of information about NNA; including what queries are running and how in-sync it is with the active cluster.

Response code of 403 means you are not authorized to view this endpoint.