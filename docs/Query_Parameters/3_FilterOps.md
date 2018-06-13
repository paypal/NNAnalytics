**FilterOps**

`filterOp` is used by all NNA queries and the options available to use on your query will depend on what you chose for your `filter`.
Please remember that `filterOp` sits in between your `filter` and your `value` and is just the conditional linking the two.

Example: `&filter=fileSize:eq:0` is a filter looking for empty files (file size equal to 0 bytes).
Yet, `&filter=fileSize:notEq:0` is the opposite filter, looking for all non-empty files.


1. `lt` - "less than" - Takes a Long as a `value`.
2. `gt` - "greater than" - Takes a Long as a `value`.
3. `lte` - "less than or equal to" - Takes a Long as a `value`.
4. `gte` - "greater than or equal to" - Takes a Long as a `value`. 
5. `eq` - "equal to" - Can take a Long, String, or Boolean, as a `value`.
6. `notEq` - "not equal to" - Can take a Long, String, or Boolean, as a `value`.
7. `startsWith` - "starts with" - Takes a String as a `value`.
8. `notStartsWith` - "does not start with" - Takes a String as a `value`.
9. `endsWith` - "ends with" - Takes a String as a `value`.
10. `notEndsWith` - "does not end with" - Takes a String as a `value`.
11. `contains` - "contains" - Takes a String as a `value`.
12. `notContains` - "does not contain" - Takes a String as a `value`.
13. `minutesAgo` - "minutes before `filter` timestamp" - Takes a Long as a `value`, representing minutes.
14. `hoursAgo` - "hours before `filter` timestamp" - Takes a Long as a `value`, representing hours.
15. `daysAgo` - "days before `filter` timestamp" - Takes a Long as a `value`, representing days.
16. `monthsAgo` - "months before `filter` timestamp" - Takes a Long as a `value`, representing months.
17. `yearsAgo` - "years before `filter` timestamp" - Takes a Long as a `value`, representing years.
18. `olderThanMinutes` - "minutes after `filter` timestamp" - Takes a Long as a `value`, representing minutes.
19. `olderThanHours` - "hours after `filter` timestamp" - Takes a Long as a `value`, representing hours.
20. `olderThanDays` - "days after `filter` timestamp" - Takes a Long as a `value`, representing days.
21. `olderThanMonths` - "months after `filter` timestamp" - Takes a Long as a `value`, representing months.
22. `olderThanYears` - "years after `filter` timestamp" - Takes a Long as a `value`, representing years.
23. `dateEq` - "`filter` timestamp falls on this date" - Take a String as a `value`, representing a date, like, `01/01/1989`.
24. `dateNotEq` - "`filter` timestamp does not falls on this date" - Take a String as a `value`, representing a date, like, `01/01/1989`.
25. `dateLt` - "`filter` timestamp falls on a day before this date" - Take a String as a `value`, representing a date, like, `01/01/1989`.
26. `dateLte` - "`filter` timestamp falls on a day equal to or before this date" - Take a String as a `value`, representing a date, like, `01/01/1989`.
27. `dateStart` - "`filter` timestamp falls on a day starting from..." - Take a String as a `value`, representing a date, like, `01/01/1989`. Must be used in combination with `dateEnd`. 
28. `dateGt` - "`filter` timestamp falls on a day after this date" - Take a String as a `value`, representing a date, like, `01/01/1989`.
29. `dateGte` - "`filter` timestamp falls on a day equal to or after this date" - Take a String as a `value`, representing a date, like, `01/01/1989`.
30. `dateEnd` - "`filter` timestamp falls on a day before..." - Take a String as a `value`, representing a date, like, `01/01/1989`. Must be used in combination with `dateStart`. 