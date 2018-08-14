**SubmitOperation:**

*This is experimental API.*

`/submitOperation` is a GET only call that only WRITER users can access.
It takes several parameters similiar to `/filter`: `?set=<files|dirs>&filters=<filter>:<filterOp>&sum=<sum>` or you can utilize a `find` as well to find a result INode set.
The new parameter here is `&operation=<delete|setReplication:<repFactor>|setStoragePolicy:<policyId>>`. which represents what you would like to happen to the resulting INode set.


You can always find the full list of available operations by going to `/operations` REST endpoint.

Response code is 200 and a plaintext value which is the operation identity to be used for aborting / listing the operation specifically.

Response code of 403 means you are not authorized to view this endpoint.

For example: `/submitOperation?set=files&filters=modTime:olderThanYears:1&operation=setReplication:2` will start an operation to set the replication factor of all files created more than a year ago to 2.