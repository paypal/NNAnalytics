**ReloadNamespace:**

`/reloadNamespace` is a GET only call that only ADMIN users can access.

Response code is 200; representing that NNA is now refreshing its in-memory state off the latest FsImage in its local storage.

Response code of 403 means you are not authorized to view this endpoint.