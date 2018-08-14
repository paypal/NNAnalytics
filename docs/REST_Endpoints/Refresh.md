**Refresh:**

`/refresh` is a GET only call that only ADMIN users can access.
Only list of ADMINs, READERs, WRITERs, CACHE users, and LOCAL accounts can be refreshed.

Response code is 200 and a plaintext dump of newly loaded security configurations regarding authorization.

Response code of 403 means you are not authorized to view this endpoint.