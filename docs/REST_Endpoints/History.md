**History:**

*This is a configurable endpoint. Requires enabling historical embedded DB in configuration.*

`/history` is a GET only call that only CACHE users can access.
It takes a required parameters `?username=<username>&fromDate=<date>&toDate=<date>` where `date` is in the form (MM/dd/YYYY) and username represents some user whose cached data points you wish to access.

Response code is 200 and a JSON dump of data points captured in the embedded database. 

Response code of 403 means you are not authorized to view this endpoint.

Response code of 400 means this endpoint is not enabled or your parameters are incorrect.