**Token:**

`/token` is a GET only call that only CACHE users can access.

Response code is 200 and a JSON dump of mapping users to the last known timestamp when a delegation token was issued to them.
A date of `N/A` means this user has never been issued a delegation token since NNA has been observing.

Response code of 403 means you are not authorized to view this endpoint.