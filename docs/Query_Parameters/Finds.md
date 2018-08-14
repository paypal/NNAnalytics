**Finds:**

`find` can be used in place of a `sum` by all NNA queries to perform additional calculations on the resulting set.
`find` works a little differently than `sum` in that it takes an operator and a `filter` value, like so: `&find=<operator>:<filterField>`

You can always find the full list of available finds by going to `/finds` REST endpoint.

When using `find` with a `/filter` query, the find is performed over the resulting INode set.
For example:
  * `&find=avg:modTime` will perform average across all the resulting set's INodes' modification timestamps.
  * `&find=min:modTime` will return the INode that has the oldest (lowest value) modification timestamp.
  * `&find=max:modTime` will return the INode that has the most recent (highest value) modification timestamp.

However, `find` as part of a `/histogram` query will be performed over every grouping instead.
For example:
  * `/histogram?set=files&group=user&find=min:modTime` will get every users oldest file's modification timestamp.

1. `avg` - Performs an average of the `filter` field.
2. `min` - Gets the maximum value of the `filter` field.
3. `max` - Gets the minimum value of the `filter` field.