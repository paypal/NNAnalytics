**SQL:**

*This is experimental API.*

`/sql` is a POST only call that only READER users can access.

Be sure to post your SQL statement as a request body parameter with key `sqlStatement`.

Queries with no COUNT, SUM, MIN, MAX, or AVG functions -- nor any GROUP BYs, will result in a plain-text dump of file paths from the result set.
Queries with an aggregation function but no GROUP BYs will result in a plain-text dump of just the resulting numerical value.
Queries with a GROUP BY will result in a JSON formatted histogram where the keys are your grouping and value is aggregation per grouping.

This is a very limited SQL-like syntax. Only one type of aggregation may be performed and only one type of grouping as well.

JOINs and other more complex functions are not allowed.

Response code is 200 and is either a plain-text dump of all file paths that match your query, a JSON output of a histogram, or a plain-text result of a single number.
All of which depend on how your query is written.

Response code of 403 means you are not authorized to view this endpoint.