**ContentSummary:**

`/contentSummary` is a GET only call that only READER users can access.
It takes a required parameter `?path=<path>` to specify the location you wish to run the ContentSummary operation on.

*Unlike other queries, `/contentSummary` will not take the NNA Query Lock and therefore will not make you wait for other queries to run. It will run in parallel, just like in an Active NameNode.*

There are several optional parameters available to the histogram query:
* `&useLock=<boolean>` if you wish to take the FSNamesystem lock as part of your query if you are seeing inconsistencies between histograms. This ensures the INode set will not change underneath mid-query.
* `&useQueryLock=<boolean>` if you wish to take the NNA Query lock as part of your query. This will make you wait for subsequent queries to finish before yours runs.

Response code is 200 and an XML representation of the HDFS configurations used to bootstrap.

Response code of 403 means you are not authorized to view this endpoint.

Response code of 404 means the path you specified does not exist.