**Metrics:**

`/metrics` is a GET only call that only CACHE users and higher can access.

If you are accessing NNA behind a proxy (NGINX, for example), be sure to enable support for the X-Forwarded-For header like so: `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;`

Response code is 200 and a JSON dump containing metrics information. JSON contains an array of users with their total login, logout and query counts as well as a breakdown by ip addresses used.

```json
{
   "users":[
      {
         "totalLoginCount":2,
         "totalQueryCount":19,
         "totalLogoutCount":2,
         "userName":"user1",
         "ips":[
            {
               "0:0:0:0:0:0:0:1":{
                  "queryCount":3,
                  "loginCount":1,
                  "logoutCount":1
               }
            },
            {
               "127.0.0.1":{
                  "queryCount":16,
                  "loginCount":1,
                  "logoutCount":1
               }
            }
         ]
      },
      {
         "totalLoginCount":4,
         "totalQueryCount":32,
         "totalLogoutCount":3,
         "userName":"user2",
         "ips":[
            {
               "0:0:0:0:0:0:0:1":{
                  "queryCount":32,
                  "loginCount":4,
                  "logoutCount":3
               }
            }
         ]
      }
   ]
}
```

Response code of 403 means you are not authorized to view this endpoint.
