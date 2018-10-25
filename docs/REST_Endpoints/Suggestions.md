**Suggestions:**

`/suggestions` is a GET only call that only CACHE users can access.
It takes an optional parameter `?username=<username>` if you wish to look at cached information about a specific user. 
If no user is specified you will see the information of the entire system.

If you make the call with `?all` as the parameter then the JSON dump will contain information of all users.

Response will be a list where keys are users and each key will have a list of key-value pairs that represent captured metrics for each user.

Example response:
```json
{
  username1: {
    smallFiles24hDs: 0,
    lastLogin: -1,
    tinyFiles: 0,
    dsQuotaThreshCount: 0,
    oldFiles1yr: 0,
    largeFiles: 0,
    smallFiles1yr: 0,
    emptyDirs24h: 0,
    oldFiles2yr: 0,
    emptyDirs24hMem: 0,
    capacity: 9999999999,
    tinyFiles1yr: 0,
    emptyFiles: 0,
    oldFiles1yrDs: 0,
    smallFilesMem: 0,
    numFiles: 0,
    numDirs: 1,
    emptyFilesMem: 0,
    smallFiles: 0,
    totalFiles: 69,
    tinyFilesDs: 0,
    smallFiles24hMem: 0,
    oldFiles2yrDs: 0,
    dsQuotaCount: 0,
    tinyFiles24hDs: 0,
    emptyDirs: 0,
    smallFiles24h: 0,
    nsQuotaCount: 0,
    diskspace: 0,
    tinyFiles24h: 0,
    diskspace24h: 0,
    emptyFiles24h: 0,
    emptyDirs1yr: 0,
    timeTaken: 215966,
    emptyFiles1yr: 0,
    mediumFiles: 0,
    tinyFiles24hMem: 0,
    emptyFiles24hMem: 0,
    tinyFilesMem: 0,
    emptyDirsMem: 0,
    totalDirs: 70,
    numFiles24h: 0,
    smallFilesDs: 0,
    nsQuotaThreshCount: 0,
    reportTime: 1540484393196
  },
  username2: {
    smallFiles24hDs: 0,
    lastLogin: 1234567890,
    tinyFiles: 0,
    dsQuotaThreshCount: 0,
    oldFiles1yr: 0,
    largeFiles: 0,
    smallFiles1yr: 0,
    emptyDirs24h: 0,
    oldFiles2yr: 0,
    emptyDirs24hMem: 0,
    capacity: 9999999999,
    tinyFiles1yr: 0,
    emptyFiles: 0,
    oldFiles1yrDs: 0,
    smallFilesMem: 0,
    numFiles: 0,
    numDirs: 1,
    emptyFilesMem: 0,
    smallFiles: 0,
    totalFiles: 8999,
    tinyFilesDs: 0,
    smallFiles24hMem: 0,
    oldFiles2yrDs: 0,
    dsQuotaCount: 1,
    tinyFiles24hDs: 0,
    emptyDirs: 1,
    smallFiles24h: 0,
    nsQuotaCount: 1,
    diskspace: 0,
    tinyFiles24h: 0,
    diskspace24h: 0,
    emptyFiles24h: 0,
    emptyDirs1yr: 0,
    timeTaken: 215966,
    emptyFiles1yr: 0,
    mediumFiles: 0,
    tinyFiles24hMem: 0,
    emptyFiles24hMem: 0,
    tinyFilesMem: 0,
    emptyDirsMem: 100,
    totalDirs: 9001,
    numFiles24h: 0,
    smallFilesDs: 0,
    nsQuotaThreshCount: 0,
    reportTime: 1540484393196
  }
}
```

Response code is 200 and a JSON dump of issues mapping to their numerical representation. 

Response code of 403 means you are not authorized to view this endpoint.