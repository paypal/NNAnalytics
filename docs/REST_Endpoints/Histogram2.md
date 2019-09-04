**Histogram2:**

*This is experimental API.*

`/histogram2` is a GET only call that only READER users can access to perform 2-level histogram grouping functions.
It takes several required parameters named `?set=<files|dirs>&filters=<filter>:<filterOps>&type=<type>&sum=<sum>`.
The main difference in `histogram2` is that it takes two arguments in the type field seperated by a comma.
Ex: `&type=user,fileType`.
It only takes a summation operator today but will likely take `find` like regular `/histogram` in the future.

There are several optional parameters available to the histogram query:
* `&timeRange=<daily|weekly|monthly|yearly>` to specify a time range for histograms that deal with time ranges.
* `&parentDirDepth=<number>` to the depth at which to group by parent directories if using the `parentDir` histogram type.
* `&useLock=<boolean>` if you wish to take the FSNamesystem lock as part of your query if you are seeing inconsistencies between histograms. This ensures the INode set will not change underneath mid-query.
* `&histogramOutput=<csv|json>` to get the output in either CSV or JSON format.
* `&histogramConditions=<filter>:<filterOps>` if you wish to only get columns based on some conditional.

Response code is 200 and is some representation, either CSV or JSON, or a histogram where the bins are by the `type` and the y-axis represents the `sum`.

Response code of 403 means you are not authorized to view this endpoint.