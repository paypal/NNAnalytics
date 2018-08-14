**Logout:**

`/logout` is a POST only call that will destroy the user's JSON Web Token session and ask browser to clear the cookie.

Response code of 200 means success.

Response code of 400 means there was no session found for the user or no authentication is enabled.