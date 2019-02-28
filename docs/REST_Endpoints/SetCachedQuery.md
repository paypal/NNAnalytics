**SetCachedQuery:**

`/setCachedQuery` is a GET only call that only ADMIN users can access.
It takes a required parameter `?queryName=<name>&queryType=<filter|histogram>&<queryParams>`.
If the cached query name already exists, then it will be overridden by your query parameters.

You must specify a `sum` type. Cached queries will not dump a list of paths. If you need path information it is advised you try to make use of the `parentDir` histogram.
If you are using `&queryType=histogram`, then please note that `useLock` and `histogramConditions` are not supported during a cached histogram query.

Response code is 200 and a plaintext message saying the addition of the custom cached query was successful.

Response code of 403 means you are not authorized to view this endpoint.