**AddDirectory:**

*This endpoint does NOT add directories to the active cluster.*

`/addDirectory` is a GET only call that only ADMIN users can access.
It takes a required parameter `?dir=<dirName>`.

Response code is 200 and a plaintext message saying the addition to directory scanning was successful.

Response code of 403 means you are not authorized to view this endpoint.