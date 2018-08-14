**Transforms:**

`transform` can be as part of a `/histogram` query whereby certain results are modified mid-query in order to see 'what could be' as a result.
`transform` works by adding the following parameters to the histogram query:

* `transformConditions=<filter>:<filterOps>` details the conditions to look for before applying a transformation.
* `transformFields=<field>` details which field you wish to transform; for example, `replFactor` or `numReplicas` or `diskspaceConsumed`.
* `transformOutputs=<output>` details which numerical value you wish to use in-place of the live in-memory value for the transform field.

You can always find the full list of available finds by going to `/transforms` REST endpoint.

For example:
  * `/histogram?set=files&type=fileReplica&sum=diskspaceConsumed&transformConditions=fileReplica:notEq:2&transformFields=fileReplica&transformOutputs=2` will output a histogram as if all files had replication factor 2.