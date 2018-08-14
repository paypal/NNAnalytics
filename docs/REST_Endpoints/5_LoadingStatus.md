**LoadingStatus:**

`/loadingStatus` is a GET only call that only ADMIN users can access.

Response code is 200 and a JSON representation of the NameNode bootstrap process and how far along it is.

Response code of 403 means you are not authorized to view this endpoint.