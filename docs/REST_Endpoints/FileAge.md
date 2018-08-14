**FileAge:**

`/fileAge` is a GET only call that only CACHE users can access.
It takes a required parameter `?sum=<count|diskspaceConsumed>` to specify looking at cached file age breakdowns by count or by diskspace usage.

Response code is 200 and a JSON presentation of a time range histogram mapping the age of files to their combined count or diskspace. 

Response code of 403 means you are not authorized to view this endpoint.