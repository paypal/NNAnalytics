**Histograms:**

`type` is used by only the `/histogram` endpoint and the options available to use on your query will depend on what you choose as your starting `set`
It is a no-op in the `/filter` endpoint.

You can always find the full list of available histograms by going to `/histograms` REST endpoint.
You can also find the full list of available histogram output types by going to `/histogramOutputs` REST endpoint.

1. `user` - Usable by files and dirs. Groups the filtered INodes by their owner's username.
2. `accessTime` - Usable by files. Groups the filtered INodes in bucket ranges of their last accessed timestamp. Please note this only applies if your Hadoop cluster has accesstime enabled.
3. `modTime` - Usable by files and dirs. Groups the filtered INodes in bucket ranges of their last modified timestamp.
4. `fileSize` - Usable by files. Groups the filtered INodes in bucket ranges of file size bytes.
5. `diskspaceConsumed` - Usable by files. Groups the filtered INodes in bucket ranges of disk space bytes used.
6. `memoryConsumed` - Usable by files and dirs. Groups the filtered INodes in bucket ranges of NameNode memory bytes used.
7. `fileReplica` - Usable by files. Groups the filtered INodes by their replication factor.
8. `parentDir` - Usable by files and dirs. Groups the filtered INodes by their parent directory. You can use `&parentDirDepth=<integer_depth>` to define a strict depth you want to group by.
9. `storageType` - Usable by files and dirs. Groups the filtered INodes by their [BlockStoragePolicy](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html#Storage_Types_and_Storage_Policies).
10. `group` - Usable by files and dirs. Groups the filtered INodes by their owner's group name.
11. `fileType` - Usable by files and dirs. Groups the filtered INodes by their file's extension.
12. `dirQuota` - Usable by dirs. Groups the filtered INodes by their quotas. You can decide by which quota via the `sum`.

All memoryConsumed calculations are estimations performed as documented [here](https://www.cloudera.com/documentation/enterprise/5-8-x/topics/admin_nn_memory_config.html).