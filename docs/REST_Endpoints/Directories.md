**Directories:**

`/directories` is a GET only call that only CACHE users can access.
It takes an optional parameter `?dir=<dirName>&sum=<count|diskspaceConsumed>` which can be used to obtain count / space information about directories that NNA scans.

Response code is 200 and a JSON dump of directories that NNA scans showing the count or space underneath. 
Directories can be added to NNA for specific scanning via `/addDirectory` and `/removeDirectory` ADMIN endpoints.

Response code of 403 means you are not authorized to view this endpoint.