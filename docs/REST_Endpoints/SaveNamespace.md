**SaveNamespace:**

`/saveNamespace` is a GET only call that only ADMIN users can access.
It takes an optional `?legacy=<true|false>&dir=<path>` argument that represents a desire to produce a legacy format FsImage.

Response code is 200; representing that an FsImage was produced. This is a blocking call and will take a while to complete.

Response code of 403 means you are not authorized to view this endpoint.