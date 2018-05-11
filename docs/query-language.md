+++
title = "Query Language"
weight = 6
nav = [
    "Conventions",
    "Arguments and Types",
    "Write Operations",
    "Read Operations",
]
+++

## Query Language

### Overview

This section will provide a detailed reference and examples for the Pilosa Query Language (PQL). All PQL queries operate on a single [index](../glossary/#index) and are passed to Pilosa through the `/index/INDEX_NAME/query` endpoint. You may pass multiple PQL queries in a single request by simply concatenating the queries together - a space is not needed. The results format is always:

```
{"results":[...]}
```

There will be one item in the `results` array for each PQL query in the request. The type of each item in the array will depend on the type of query - each query in the reference below lists it's result type.

#### Conventions

* Angle Brackets `<>` denote required arguments
* Square Brackets `[]` denote optional arguments
* UPPER_CASE denotes a descriptor that will need to be filled in with a concrete value (e.g. `ATTR_NAME`, `STRING`)

##### Examples

Before running any of the example queries below, follow the instructions in the [Getting Started](../getting-started/) section to set up an index, frames, and populate them with some data.

The examples just show the PQL quer(ies) needed - to run the query `SetBit(frame="stargazer", col=10, row=1)` against a server using curl, you would:
``` request
curl localhost:10101/index/repository/query \
     -X POST \
     -d 'SetBit(frame="stargazer", col=10, row=1)'
```
``` response
{"results":[true]}
```

#### Arguments and Types

* `frame` The frame specifies on which Pilosa [frame](../glossary/#frame) the query will operate. Valid frame names are lower case strings; they start with an alphanumeric character, and contain only alphanumeric characters and `_-`. They must be 64 characters or less in length.
* `TIMESTAMP` This is a timestamp in quotes with the following format `"YYYY-MM-DDTHH:MM"` (e.g. "2006-01-02T15:04")
* `UINT` An unsigned integer (e.g. 42839)
* `ATTR_NAME` Must be a valid identifier `[A-Za-z][A-Za-z0-9._-]*`
* `ATTR_VALUE` Can be a string, float, integer, or bool.
* `BITMAP_CALL` Any query which returns a bitmap, such as `Bitmap`, `Union`, `Difference`, `Xor`, `Intersect`, `Range`
* `[]ATTR_VALUE` Denotes an array of `ATTR_VALUE`s. (e.g. `["a", "b", "c"]`)

### Write Operations

#### SetBit

**Spec:**

```
SetBit(<frame=STRING>, <row=UINT>, <col=UINT>, 
       [timestamp=TIMESTAMP])
```

**Description:**

`SetBit`, assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given frame with the given column.

**Result Type:** boolean

A return value of `true` indicates that the bit was changed to 1.

A return value of `false` indicates that the bit was already set to 1 and nothing changed.


**Examples:**

```
SetBit(frame="stargazer", col=10, row=1)
```

This query illustrates setting a bit in the stargazer frame. User with id=1 has starred repository with id=10.

SetBit also supports providing a timestamp. To write the date that a user starred a repository.
```
SetBit(frame="stargazer", col=10, row=1, timestamp="2016-01-01T00:00")
```

Setting multiple bits in a single request:
```
SetBit(frame="stargazer", col=10, row=1) SetBit(frame="stargazer", col=10, row=2) SetBit(frame="stargazer", col=20, row=1) SetBit(frame="stargazer", col=30, row=2)
```

#### SetRowAttrs
**Spec:**

```
SetRowAttrs(<frame=STRING>, <row=UINT>, 
            <ATTR_NAME=ATTR_VALUE>, 
            [ATTR_NAME=ATTR_VALUE ...])
```

**Description:**

`SetRowAttrs` associates arbitrary key/value pairs with a row in a frame. Setting a value of `null`, without quotes, deletes an attribute.

**Result Type:** null

SetRowAttrs queries always return `null` upon success.

**Examples:**

```
SetRowAttrs(frame="stargazer", row=10, username="mrpi", active=true)
```

Set username value and active status for user 10. These are arbitrary key/value pairs which have no meaning to Pilosa. You can see the attributes you've set on a row with a [Bitmap](../query-language/#bitmap) query like so `Bitmap(frame="stargazer", stargazer_id=10)`.

```
SetRowAttrs(frame="stargazer", row=10, username=null)
```

Delete username value for user 10.

#### SetColumnAttrs

**Spec:**

```
SetColumnAttrs(<frame=STRING>, <row=UINT>, 
               <ATTR_NAME=ATTR_VALUE>, 
               [ATTR_NAME=ATTR_VALUE ...])
```

**Description:**

`SetColumnAttrs` associates arbitrary key/value pairs with a column in an index.

**Result Type:** null

SetColumnAttrs queries always return `null` upon success. Setting a value of `null`, without quotes, deletes an attribute. To avoid confusion, `frame` cannot be used as an attribute name.

**Examples:**

```
SetColumnAttrs(col=10, stars=123, url="http://projects.pilosa.com/10", active=true)
```

Set url value and active status for project 10. These are arbitrary key/value pairs which have no meaning to Pilosa. You can see the attributes you've set on a column with a [Bitmap](../query-language/#bitmap) query like so `Bitmap(frame="stargazer", col=10)`.

```
SetColumnAttrs(col=10, url=null)
```

Delete url value for repo 10.


#### ClearBit

**Spec:**

```
ClearBit(<frame=STRING>, <row=UINT>, <col=UINT>, 
         [timestamp=TIMESTAMP])
```

**Description:**

`ClearBit`, assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given frame from the given column.

**Result Type:** boolean

A return value of `true` indicates that the bit was toggled from 1 to 0.

A return value of `false` indicates that the bit was already set to 0 and nothing changed.

**Examples:**

```
ClearBit(frame="stargazer", col=10, row=1)
```

Remove relationship between the stargazer in row 1 and the repository in column 10 from the stargazer frame.


#### SetFieldValue

**Spec:**

```
SetFieldValue(<col=UINT>, <frame=STRING>, <FIELD_NAME=INT>)
```

**Description:**

`SetFieldValue` assigns an integer value with the specified field name to the `col` in the given `frame`.

**Result Type:** null

SetFieldValue returns `null` upon success.

**Examples:**

Set the number of pull requests of repository 10.
```
SetFieldValue(col=10, frame="stats", pullrequests=2)
```


### Read Operations

#### Bitmap

**Spec:**

```
Bitmap(<frame=STRING>, (<rowL=UINT> | <col>=UINT))
```

**Description:**

`Bitmap` retrieves the indices of all the set bits in a row or column based on whether the row or column argument is provided in the query. It also retrieves any attributes set on that row or column.

**Result Type:** object with attrs and bits.

e.g. `{"attrs":{"username":"mrpi","active":true},"bits":[10, 20]}`

**Examples:**

Query all repositories that user 1 has starred.
```
Bitmap(frame="stargazer", row=1)
```

Returns `{"attrs":{"username":"mrpi","active":true},"bits":[10, 20]}`

* attrs are the attributes for user 1 
* bits are the repositories which user 1 has starred.

#### Union

**Spec:**

```
Union([BITMAP_CALL ...])
```

**Description:**

Union performs a logical OR on the results of each `BITMAP_CALL` query passed to it.

**Result Type:** object with attrs and bits

attrs will always be empty

**Examples:**

Query all repositories that are contributed by multiple users
```
Union(Bitmap(frame="stargazer", stargazer_id=1), Bitmap(frame="stargazer", stargazer_id=2))
```

Returns `{"attrs":{},"bits":[10, 20, 30]}`.

* bits are repositories that were starred by user 1 OR user 2

#### Intersect


**Spec:**

```
Intersect(<BITMAP_CALL>, [BITMAP_CALL ...])
```

**Description:**

Intersect performs a logical AND on the results of each `BITMAP_CALL` query passed to it.

**Result Type:** object with attrs and bits

attrs will always be empty

**Examples:**

Query repositories which have been starred by two users.

```
Intersect(Bitmap(frame="stargazer", row=1), Bitmap(frame="stargazer", row=2))
```

Returns `{"attrs":{},"bits":[10]}`.

* bits are repositories that were starred by user 1 AND user 2

#### Difference

**Spec:**

```
Difference(<BITMAP_CALL>, [BITMAP_CALL ...])
```

**Description:**

Difference returns all of the bits from the first `BITMAP_CALL` argument passed to it, without the bits from each subsequent `BITMAP_CALL`.

**Result Type:** object with attrs and bits

attrs will always be empty

**Examples:**

Query repositories which have been starred by one user and not another.
```
Difference(Bitmap(frame="stargazer", row=1), Bitmap( frame="stargazer", row=2))
```

Return `{"results":[{"attrs":{},"bits":[20]}]}`

* bits are repositories that were starred by user 1 BUT NOT user 2

```
Difference(Bitmap(frame="stargazer", row=2), Bitmap( frame="stargazer", row=1))
```

Return `{"attrs":{},"bits":[30]}`

* Bits are repositories that were starred by user 2 BUT NOT user 1

#### Xor

**Spec:**

```
Xor(<BITMAP_CALL>, [BITMAP_CALL ...])
```

**Description:**

Xor performs a logical XOR on the results of each `BITMAP_CALL` query passed to it.

**Result Type:** object with attrs and bits

attrs will always be empty

**Examples:**

Query repositories which have been starred by two users.

```
Xor(Bitmap(frame="stargazer", row=1), Bitmap(frame="stargazer", row=2))
```

Returns `{"attrs":{},"bits":[30]}`.

* bits are repositories that were starred by user 1 XOR user 2 (user 1 or user 2, but not both)

#### Count
**Spec:**

```
Count(<BITMAP_CALL>)
```

**Description:**

Returns the number of set bits in the `BITMAP_CALL` passed in.

**Result Type:** int

**Examples:**

Query the number of repositories to which a user has contributed.
```
Count(Bitmap(frame="stargazer", row=1))
```

Return `2`

* Result is the number of repositories that user 1 has starred.

#### TopN

**Spec:**

```
TopN([BITMAP_CALL], <frame=STRING>, [n=UINT],
     [<field=ATTR_NAME>, <filters=[]ATTR_VALUE>])
```

**Description:**

Return the id and count of the top `n` bitmaps (by count of bits) in the frame.
The `field` and `filters` arguments work together to only return Bitmaps which
have the attribute specified by `field` with one of the values specified in
`filters`.

**Result Type:** array of key/count objects

**Caveats:**

* Performing a TopN() query on a frame with cache type ranked will return the top bitmaps sorted by count in descending order.
* Frames with cache type lru will maintain an LRU (Least Recently Used) cache, thus a TopN() query on this type of frame will return bitmaps sorted in order of most recently set bit.
* The frame's cache size determines the number of sorted bitmaps to maintain in the cache for purposes of TopN() queries. There is a tradeoff between performance and accuracy; increasing the cache size will improve accuracy of results at the cost of performance.
* Once full, the cache will truncate the set of bitmaps according to the frame option CacheSize. Bitmaps that straddle the limit and have the same count will be truncated in no particular order.
* The TopN() query's attribute filter is applied to the existing sorted cache of bitmaps. Bitmaps that fall outside of the sorted cache range, even if they would normally pass the filter, are ignored.

**Examples:**

```
TopN(frame="stargazer")
```

Returns `[{"key": 1, "count": 2}, {"key": 2, "count": 2}, {"key": 3, "count": 1}]`

* key is a user ID
* count is amount of repositories
* Results are the number of repositories that each user starred in descending order for all users in the stargazer frame, for example user 1 starred two repositories, user 2 starred two repositories, user 3 starred one repository.

```
TopN(frame="stargazer", n=2)
```

Returns `[{"key": 1, "count": 2}, {"key": 2, "count": 2}]`

* Results are the top two users sorted by number of repositories they've starred in descending order.

```
TopN(Bitmap(frame="language", row=1), frame="stargazer", n=2)
```

Returns `[{"key": 1, "count": 2}, {"key": 2, "count": 1}]`

* Results are the top two users sorted by the number of repositories that they've starred which are written in language 1.

#### Range Queries

**Spec:**

```
Range(<frame=STRING>, <row=UINT>, 
      <start=TIMESTAMP>, <end=TIMESTAMP>)
```

**Description:**

Similar to `Bitmap`, but only returns bits which were set with timestamps
between the given `start` and `end` timestamps. 

**Result Type:** object with attrs and bits


**Examples:**

When you set timestamp using SetBit, you will able to query all repositories that a user has starred within a date range.
```
Range(frame="stargazer", row=1, start="2010-01-01T00:00", end="2017-03-02T03:00")
```

Returns `{{"attrs":{},"bits":[10]}`

* bits are repositories which were starred by user 1 from 2010-01-01 to 2017-03-02


#### Range (BSI)

**Spec:**

```
Range(<frame=STRING>, <FIELD_NAME, COMPARISON_OPERATOR, COMPARISON_VALUE> )
```

**Description:**

The `Range` query is overloaded to work on `field` values as well as `timestamp` values.
Returns bits that are true for the comparison operator.

**Result Type:** object with attrs and bits


**Examples:**

In our source data, commitactivity was counted over the last year.
The following greater-than `Range` query returns all repositories having more than 100 commits.

```
Range(frame="stats", commitactivity > 100)
```

Returns `{{"attrs":{},"bits":[10]}`

* bits are repositories which had at least 100 commits in the last year.

BSI range queries support the following operators:

 Operator | Name                          | Value              
----------|-------------------------------|--------------------
 `>`      | greater-than, GT              | integer            
 `<`      | less-than, LT                 | integer            
 `<=`     | less-than-or-equal-to, LTE    | integer            
 `>=`     | greater-than-or-equal-to, GTE | integer            
 `==`     | equal-to, EQ                  | integer            
 `!=`     | not-equal-to, NEQ             | integer or `null`  
 `><`     | between, BETWEEN              | [integer, integer] 

The `BETWEEN` query specifies an interval with both bounds, using `><` operator, and a two-element list containing the lower and upper bounds of the interval:

```
Range(frame="stats", commitactivity >< [100, 200])
```

This is conceptually equivalent to the interval 100 <= commitactivity <= 200, but this chained comparison syntax is not currently supported. `BETWEEN` query syntax is restricted to greater-than-or-equal-to and less-than-or-equal-to, but any valid interval on the integers can be represented this way.

#### Min

**Spec:**

```
Min([BITMAP_CALL], <frame=STRING>, <field=STRING>)
```

**Description:**

Returns the minimum value of all BSI integer values in the `field` in this `frame`. If the optional `Bitmap` call is supplied, only columns with set bits are considered, otherwise all collumns are considered.

**Result Type:** object with the min and count of columns containing the min value.

**Examples:**

Query the size of all repositories.
```
Min(frame="stats", field="diskusage")
```

Return `{"min":4,"count":2}`

* Result is the smallest repository in kilobytes, plus the number of repositories of that size.

#### Max

**Spec:**

```
Max([BITMAP_CALL], <frame=STRING>, <field=STRING>)
```

**Description:**

Returns the maximum value of all BSI integer values in the `field` in this `frame`. If the optional `Bitmap` call is supplied, only columns with set bits are considered, otherwise all columns are considered.

**Result Type:** object with the max and count of columns containing the max value.

**Examples:**

Query the size of all repositories.
```
Max(frame="stats", field="diskusage")
```

Return `{"max":88,"count":13}`

* Result is the largest repository in kilobytes, plus the number of repositories of that size.

#### Sum

**Spec:**

```
Sum([BITMAP_CALL], <frame=STRING>, <field=STRING>)
```

**Description:**

Returns the count and computed sum of all BSI integer values in the `field` in this `frame`. If the optional `Bitmap` call is supplied, columns with set bits are summed, otherwise the sum is across all columns.

**Result Type:** object with the computed sum and count of the bitmap field.

**Examples:**

Query the size of all repositories.
```
Sum(frame="stats", field="diskusage")
```

Return `{"sum":10,"count":3}`

* Result is the size of all repositories in kilobytes, plus the number of repositories.
