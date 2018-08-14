**Sums:**

`sum` is used by all NNA queries and the options available to use on your query will depend on what you choose as your starting `set`.

You can always find the full list of available sums by going to `/sums` REST endpoint.

1. `count` - Usable by files and dirs. Simply counts the filtered INodes.
2. `fileSize` - Usable by files. Sums filtered INodes by file size. (No replication factor).
3. `diskspaceConsumed` - Usable by files. Sums filtered INodes by disk space. (Includes replication factor).
4. `memoryConsumed` - Usable by files and dirs. Sums filtered INodes by estimated NameNode memory consumed.
5. `blockSize` - Usable by files. Sums filtered INodes by preferred block size.
6. `numBlocks` - Usable by files. Sums filtered INodes by number of blocks in their file. (No replication factor).
7. `numReplicas` - Usable by files. Sums filtered INodes by number of blocks in their file. (Includes replication factor).
8. `nsQuotaRatioUsed` - Usable by directories. Sums filtered INodes by percentage of namespace quota left.
9. `dsQuotaRatioUsed` - Usable by directories. Sums filtered INodes by percentage of disk space quota left.
10. `nsQuotaUsed` - Usable by directories. Sums filtered INodes by number of namespace items used in quota.
11. `dsQuotaUsed` - Usable by directories. Sums filtered INodes by disk space bytes used in quota.
12. `nsQuota` - Usable by directories. Sums filtered INodes by namespace items allowed in quota.
13. `dsQuota` - Usable by directories. Sums filtered INodes by disk space bytes allowed in quota.