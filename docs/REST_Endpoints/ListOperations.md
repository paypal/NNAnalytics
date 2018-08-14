**ListOperations:**

*This is experimental API.*

`/listOperations` is a GET only call that only WRITER users can access.
It takes an optional `?identity=<id>&limit<number>` argument to see a specific operation (if you possess the ID of it) and the list of the number of last operated on paths.

Response code is 200 and a plaintext dump representation of running operations.

Response code of 403 means you are not authorized to view this endpoint.