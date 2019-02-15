**Histogram:**

`/histogram` is a GET only call that only READER users can access.
It takes several required parameters named `?set=<files|dirs>&filters=<filter>:<filterOps>&type=<type>&sum=<sum>`.
You may replace `sum` with a `find` if you wish. Using a `find` as part of a histogram query will perform the find operation across the bins.

There are several optional parameters available to the histogram query:
* `&timeRange=<daily|weekly|monthly|yearly>` to specify a time range for histograms that deal with time ranges.
* `&parentDirDepth=<number>` to the depth at which to group by parent directories if using the `parentDir` histogram type.
* `&sortAscending=<boolean>` if you wish to sort the bins in ascending order and `&sortDescending=<boolean>` if you wish to sort the bins in descending order.
* `&useLock=<boolean>` if you wish to take the FSNamesystem lock as part of your query if you are seeing inconsistencies between histograms. This ensures the INode set will not change underneath mid-query.
* `&top=<number>` to get only the top number of bins and `&bottom=<number` to get only the bottom number of bins.
* `&histogramOutput=<csv|json>` to get the output in either CSV or JSON format.
* `&rawTimestamps=<boolean>` to get the value as a raw number timestamp representation. Only applies if using CSV output format against a histogram query and a find on `modTime` or `accessTime`.

Experimentally, there are parameters for sending out an email of the response: `&emailTo=<toAddress>&emailCc=<ccAddresses>&emailFrom=<fromAddress>&emailHost=<emailServerAddress>&emailConditions=<filter>:<filterOps>`.

Dropping `sum` or `find` parameters entirely will output a list of paths.

Response code is 200 and is some representation, either CSV or JSON, or a histogram where the bins are by the `type` and the y-axis represents the `sum` or `find`.

Response code of 403 means you are not authorized to view this endpoint.