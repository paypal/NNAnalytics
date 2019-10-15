**FileTypes:**

`/fileTypes` is a GET only call that only CACHE users can access.
It takes an optional parameter `?user=<user>` to specify looking at cached file type information for a specific user.
It also takes a required parameter `&sum=<count|diskspaceConsumed>` to specify which file type information you wish to look at; either count or diskspace.
Any files that do not have an ending extension are marked as UNKNOWN.

If you make the call with `?all` as the parameter then the JSON dump will contain information of all file types; including count and diskspace usage.

Example response:
```json
{
  fileTypeDs: {
    user1: {
    ZIP: 104478107,
    TXT: 81700614,
    ORC: 68395733,
    PART_R: 60842934,
    JSON: 62727225,
    APP_LOG: 58925360,
    AVRO: 47522694
    }
  },
  fileTypeCount: {
    user1: {
    ZIP: 94,
    TXT: 87,
    ORC: 73,
    PART_R: 96,
    JSON: 83,
    APP_LOG: 77,
    AVRO: 66
    }
  }
}
```

Response code is 200 and a JSON dump of file type counts or diskspace consumed based on request parameters. 

Response code of 403 means you are not authorized to view this endpoint.