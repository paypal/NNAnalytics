**Quotas:**

`/quotas` is a GET only call that only CACHE users can access.
It takes an optional parameter `?user=<user>` to specify looking at cached directory quota information for a specific user.
It also takes a required parameter `&sum=<nsQuotaRatioUsed|dsQuotaRatioUsed|nsQuotaAssigned|dsQuotaAssigned|nsQuotaUsed|dsQuotaUsed>` to specify which quota information you wish to look at; either namespace or diskspace, percentage usage, assignment, and raw usage.
Any directories that are marked for with a quota will automatically appear here after the next scan.

If you make the call with `?all` as the parameter then the JSON dump will contain information of all directory quotas.
If you use the `?all` call, you may also add `&sum=<quotaUsed|quotaAssigned|quotaRatioUsed>` to the call those breakdowns; by default it will return the ratio used if no `sum` is specified.

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