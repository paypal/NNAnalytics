**RemoveDirectory:**

*This endpoint does NOT remove directories from the active cluster.*

`/removeDirectory` is a GET only call that only ADMIN users can access.
It takes a required parameter `?dir=<dirName>`.

Response code is 200 and a plaintext message saying the removal from directory scanning was successful.

Response code of 403 means you are not authorized to view this endpoint.

Response code of 404 means the directory you specified was not found.