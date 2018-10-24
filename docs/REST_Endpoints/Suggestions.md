**Suggestions:**

`/suggestions` is a GET only call that only CACHE users can access.
It takes an optional parameter `?username=<username>` if you wish to look at cached information about a specific user. 
If no user is specified you will see the information of the entire system.

If you make the call with `?all` as the parameter then the JSON dump will contain information of all users.

Response code is 200 and a JSON dump of issues mapping to their numerical representation. 

Response code of 403 means you are not authorized to view this endpoint.