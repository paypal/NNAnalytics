**Filters:**

`filter` is used by all NNA queries and the options available to use on your query will depend on what you choose as your starting `set`.

You can always find the full list of available filters by going to `/filters` REST endpoint.

1. `accessTime` - Usable by files. Filters the working INode set by some last accessed timestamp. Please note this only applies if your Hadoop cluster has accesstime enabled.
2. `modTime` - Usable by files and dirs. Filters the working INode set by last modified timestamp. 
3. `fileSize` - Usable by files. Filters the working INode set by some file size (in bytes). (No replication factor)
4. `diskspaceConsumed` - Usable by files. Filters the working INode set by some disk space size (in bytes). (Includes replication factor)
5. `memoryConsumed` - Usable by files and dirs. Filters the working INode set by some memory size (in bytes). 
6. `fileReplica` - Usable by files. Filters the working INode set by some replication factor. 
7. `blockSize` - Usable by files. Filters the working INode set by some block size (in bytes). (No replication factor)
8. `numBlocks` - Usable by files. Filters the working INode set by some number of blocks in the file. (No replication factor)
9. `numReplicas` - Usable by files. Filters the working INode set by some number of blocks in the file. (Includes replication factor)
10. `dirNumChildren` - Usable by dirs. Filters the working INode set by some number of children directly under the directory.
11. `dirSubTreeSize` - Usable by dirs. Filters the working INode set by some number of total disk space bytes under the subtree. (Includes replication factor)
12. `dirSubTreeNumFiles` - Usable by dirs. Filters the working INode set by some number of children (that are files) under the directory subtree.
13. `dirSubTreeNumDirs` - Usable by dirs. Filters the working INode set by some number of  children (that are directories) under the directory subtree.
14. `storageType` - Usable by files and dirs. Filters the working INode set by some [BlockStoragePolicyId](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html#Storage_Types_and_Storage_Policies).
15. `depth` - Usable by files and dirs. Filters the working INode set by some integer representing depth in the INode tree. 
16. `permission` - Usable by files and dirs. Filters the working INode set by some integer representing POSIX permissions. 
17. `name` - Usable by files and dirs. Filters the working INode set by some String representing the name of the file or directory. 
18. `path` - Usable by files and dirs. Filters the working INode set by some String representing the full path of the file or directory. 
19. `user` - Usable by files and dirs. Filters the working INode set by some String representing owner's username of the file or directory. 
20. `group` - Usable by files and dirs. Filters the working INode set by some String representing owner's group name of the file or directory. 
21. `modDate` - Functions like `modTime` but the `value` can be a calendar date, like: `01/01/1989`.
22. `accessDate` Functions like `accessTime` but the `value` can be a calendar date, like: `01/01/1989`.
23. `isUnderConstruction` - Usable by files. Filters the working INode set by some condition of whether the file is under construction.
24. `isWithSnapshot` - Usable by files and dirs. Filters the working INode set by some condition of whether the file or directory is part of a [Snapshot](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html).
25. `hasAcl` - Usable by files and dirs. Filters the working INode set by some condition of whether the file or directory is has a native HDFS ACL.
26. `hasQuota` - Usable by dirs. Filters the working INode set by some condition of whether the directory has either a namespace or disk space quota assigned.
27. `isUnderNsQuota` - Usable by files and dirs. Filters the working INode set by checking whether each INode has any parent up to the root that has a namespace quota assigned.
28. `isUnderDsQuota` - Usable by files and dirs. Filters the working INode set by checking whether each INode has any parent up to the root that has a disk space quota assigned.

All memoryConsumed calculations are estimations performed as documented [here](https://www.cloudera.com/documentation/enterprise/5-8-x/topics/admin_nn_memory_config.html).