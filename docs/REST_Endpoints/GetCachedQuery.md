**GetCachedQuery:**

`/getCachedQuery` is a GET only call that only CACHE_READER users can access.
It takes a required parameter `?queryName=<name>`.

Response code is 200 and a plaintext value or JSON histogram of the latest cached result of your query, if successful.

Response code of 403 means you are not authorized to view this endpoint.

Response code of 404 means the query you specified was not found.