**Quotas:**

`/quotas` is a GET only call that only CACHE users can access.
It takes an optional parameter `?user=<user>` to specify looking at cached directory quota information for a specific user.
It also takes a required parameter `&sum=<nsQuotaRatioUsed|dsQuotaRatioUsed>` to specify which quota you wish to look at; either namespace or diskspace.
Directories can be added to NNA for quota scanning via `/addDirectory` and `/removeDirectory` ADMIN endpoints.

If you make the call with `?all` as the parameter then the JSON dump will contain information of all directory quotas.

Example response:
```json
{
  nsQuota: {
    user1: {
      /dir1/dir1: 10,
      /dir1/dir2: 20
    },
    user2: {
      /dir2/dir2: 30,
      /dir3/dir3: 40
    }
  },
  dsQuota: {
    user1: {
      /dir1/dir1: 50,
      /dir1/dir2: 60
    },
    user2: {
      /dir2/dir2: 70,
      /dir3/dir3: 80
    }
  }
}
```

Response code is 200 and a JSON dump of directories and a mapping to a percentage (0-100) of their quota used. 

Response code of 403 means you are not authorized to view this endpoint.