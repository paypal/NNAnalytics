**AbortOperation:**

*This is experimental API.*

`/abortOperation` is a GET only call that only WRITER users can access.
It takes a required parameter `?identity=<id>` argument to specify which operation to stop.

Response code is 200 and a plaintext dump representation of the aborted operation.

Response code of 403 means you are not authorized to view this endpoint.

Response code of 404 means the operation you specified was not found.