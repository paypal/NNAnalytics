**FetchNamespace:**

`/fetchNamespace` is a GET only call that only ADMIN users can access.

Response code is 200; representing that an FsImage was fetched off the live cluster. This is a blocking call and may take a while to complete.

Response code of 403 means you are not authorized to view this endpoint.