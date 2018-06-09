**How To Query:**

All queries in NNA are URL constructions that hit the endpoint `/filter` or `/histogram`.

There are several ways to construct an NNA query.
The most basic type of queries follow the following format:
`/filter?set=<files|dirs>&filters=[filter:filterOp:value]&sum=<sum>`

Whereby `set` is refering to which set of INodes you wish to work with.
Either the set of all files, or set of all directories.

`filters` refers to all the different combinations of filters you can then apply to that set.
Note that some filters will not be accessible to you depending on which set you are working with.
For example, running with `&filters=fileSize:eq:0` will not work against the set of all directories as directories have no metadata regarding file size in HDFS.
You can chain filters together as well.

Depending on which filter you pick, you can then use a `filterOp` to describe the conditional you are looking for.
Some easy examples are `eq` being "is equal to", `gt` being "greater than", `gte` being "greater than or equal to", and then by extension, `lt` and `lte`.

`sum` will dictate what your final result represents.
Once your INode set has been filtered down to the set of INodes you care about, the `sum` operation will then reduce the filtered set to a final value.
For example, `&sum=count` will simply count the number of INodes in the filtered set.
Or, `&sum=diskspaceConsumed` will sum the file sizes times their replication factors of every file in a filtered set of file INodes.

If you do not include `&sum=<sum>` in your query then `/filter` will instead dump the list of paths that comprise the filtered set. 

On your instance of NNA you can always go to `/sets`, `/filters`, `filterOps`, and `/sums` to see the different options available to you.


There is another more advanced query in the following format:
`/histogram?set=<files|dirs>&filters=[filter:filterOp:value]&type=<type>&sum=<sum>`

The `/histogram` query is nearly identical to the `/filter` query above but it carries an extra parameter, `&type`.
`type` in this case refers to the type of histogram and grouping you wish to achieve.

Take the following example histogram query: `/histogram?set=files&filters=fileSize:eq:0&type=user&sum=count`
This query will take the set of all files, filter down to only empty files, and group the result by their owner, and sum the groupings by their number of empty files.

You can check `/histograms` to see the different values you can use for `type`.
You can also assign a `&histogramOutput=<csv|json>` to get your histogram output as different formats.