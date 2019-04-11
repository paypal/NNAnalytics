**CachedMaps:**

`cachedMaps` is a CACHE user call to see the set of all cached map keys that the SuggestionEngine stores.

You can see all cached reports by going to `/cachedMaps` REST endpoint.

You can use the keys to query, for example, `/users?suggestion=<key>` and get the full cached histogram report for that key.
Some example default keys are `numFilesUsers` and `diskspaceUsers`, which are cached reports of all users and their file counts and diskspace usage counts.

Cached maps are updated every SuggestionEngine cycle visible on the Suggestion UI status page.

Response code is 200 and a JSON representation of cached report keys available.

Response code of 403 means you are not authorized to view this endpoint.