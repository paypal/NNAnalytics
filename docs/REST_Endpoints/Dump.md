**Dump:**

`/dump` is a GET only call that only READER users can access.
It takes a `?path=<path>` argument that represents a single INode in the file system.

Example response:
```json
{
  accessTime: "Dec 31, 1969 4:00:00 PM",
  aclsCount: "NONE",
  children: [
    "dir0",
    "dir1",
    "dir2",
    "dir4",
    "dir5",
    "dir6",
    "dir7",
    "dir8",
    "dir9"
  ],
  dsQuota: -1,
  isWithSnapshot: true,
  modTime: "Dec 31, 1969 4:00:00 PM",
  nodeId: 16385,
  nsQuota: 9223372036854776000,
  path: "/",
  permisssions: "root:supergroup:rwxr-xr-x",
  snapshottable: true,
  storagePolicy: {
    id: 7,
    name: "HOT",
    storageTypes: [
      "DISK"
    ],
    creationFallbacks: [ ],
    replicationFallbacks: [
      "ARCHIVE"
    ],
    copyOnCreateFile: false
  },
  type: "directory",
  xAttrs: "NONE"
}
```

Response code is 200 and a JSON representation of all information about that INode.

Response code of 403 means you are not authorized to view this endpoint.