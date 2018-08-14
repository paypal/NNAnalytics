**System:**

`/system` is a GET only call that only ADMIN users can access.

Response code is 200 and a plaintext view of system metrics like CPU load and JVM memory usage.

Response code of 403 means you are not authorized to view this endpoint.