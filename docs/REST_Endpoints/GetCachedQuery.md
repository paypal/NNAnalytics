**GetCachedQuery:**

`/getCachedQuery` is a GET only call that only CACHE users and higher can access.
It takes a required parameter `?queryName=<name>`.

Response code is 200 and a plaintext value or either a JSON or CSV histogram of the latest cached result of your query, if successful.
You may also get a response code of 200 and a value as "null" if your query has not successfully been computed and cached yet.
If it has been a while since you set the cached query then you may wish to check the NNA logs.

Response code of 403 means you are not authorized to view this endpoint.

Response code of 404 means the query you specified was not found.

For example: If you wish to cache a histogram for all empty files by user, normally queried like so, `/histogram?set=files&filters=fileSize:eq:0&type=user&sum=count&histogramOutput=csv&sortDescending=true`, you would want to set up a cached query like so, `/setCachedQuery?queryName=<queryName>&queryType=histogram&set=files&filters=fileSize:eq:0&type=user&sum=count&histogramOutput=csv&sortDescending=true`, and then fetch the results once available using `/getCachedQuery?queryName=<queryName>`.