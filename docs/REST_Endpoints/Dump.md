**Dump:**

`/dump` is a GET only call that only READER users can access.
It takes a `?path=<path>` argument that represents a single INode in the file system.

Response code is 200 and a plaintext representation of all information about that INode.

Response code of 403 means you are not authorized to view this endpoint.