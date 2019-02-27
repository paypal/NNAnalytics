**RemoveCachedQuery:**

`/removeCachedQuery` is a GET only call that only ADMIN users can access.
It takes a required parameter `?queryName=<name>`.

Response code is 200 and a plaintext message saying the removal from cached queries was successful.

Response code of 403 means you are not authorized to view this endpoint.

Response code of 404 means the query you specified was not found.