**SQL:**

*This is experimental API.*

`/sql` is a POST only call that only READER users can access.

Be sure to post your SQL statement as a request body parameter with key `sqlStatement`.
Recommend to check SQL API examples at the CQEngine main page: https://github.com/npgall/cqengine#string-based-queries-sql-and-cqn-dialects.

The supported grammar can be found here: https://github.com/npgall/cqengine/blob/master/code/src/main/antlr4/imports/SQLite.g4.

Response code is 200 and is a dump of all file paths that match your query.

Response code of 404 means you are not utilizing a QueryEngine that supports this endpoint.

Response code of 403 means you are not authorized to view this endpoint.