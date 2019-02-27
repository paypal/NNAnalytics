**SetCachedQuery:**

`/setCachedQuery` is a GET only call that only ADMIN users can access.
It takes a required parameter `?queryName=<name>&queryType=<filter|histogram>&<queryParams>`.
If the cached query name already exists, then it will be overridden by your query parameters.

Response code is 200 and a plaintext message saying the addition of the custom cached query was successful.

Response code of 403 means you are not authorized to view this endpoint.