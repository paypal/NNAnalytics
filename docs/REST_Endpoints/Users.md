**Users:**

`/users` is a GET only call that only READER users can access.
It takes an optional `?suggestion=<issueName>` argument that represents which issue breakdown you'd like to see across all users.

Response code is 200 and either a JSON list of all usernames available on the cluster or a mapping of usernames to some number representing the parameter issue requested.

Response code of 403 means you are not authorized to view this endpoint.

For example: `/users?suggestion=emptyFilesUsers` will return a mapping of user names to the last count of empty files found owned by each user.