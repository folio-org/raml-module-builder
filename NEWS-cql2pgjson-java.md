# cql2pgjson-java/NEWS.md

This news document is discontinued because cql2pgjson-java has been merged
into raml-module-builder. Use raml-module-builder's NEWS.md instead.

## 4.0.0 2019-04-09
* CQLPG-90 Remove legacy query translation on individual json schema files
* CQLPG-84 Add command line client for debugging
* CQLPG-83 Allow CQL2PGJSON to specify the path for the 'schema.json' file

## 3.1.0 2019-02-18
* CQLPG-86 Allow <> operator for id searches.

## 3.0.4 2019-01-06
* CQLPG-76: Update jackson-databind to 2.9.8 fixing security vulnerabilities.

## 3.0.3 2018-12-13

* CQLPG-75: Fix still issue with schema refs on Windows
* CQLPG-74: Broken maven-surefire-plugin
* CQLPG-73: Replace <prerequisites><maven> by maven-enforcer-plugin
  requireMavenVersion
* CQLPG-72: Finish unit test for $ref http://

## 3.0.2 2018-10-17
* CQLPG-70: Comparing id to invalid UUID: Don't throw error message, simply don't match.
* CQLPG-71: Forbid modifiers like /ignoreCase /masked /respectAccents for the primary key id field.
* CQLPG-67: Default pkColumnName = "id".
* CQLPG-68: Probably incorrect handling of ref in method.
* CQLPG-69: Security vulnerability reported in jackson-databind, PostgreSQL, PostgreSQL JDBC Driver.

## 3.0.1 2018-10-14
* CQLPG-66: Fix validating of array field names.

## 3.0.0 2018-10-12
* CQLPG-63: Drop field name to full field path resolution. This is a
  breaking change, using the unambiguous suffix "email" no longer resolves
  to "personal.email".
* CQLPG-61: Equals empty is not same as equals star. Matching "*" means
  all records, matching "" means all records where that field is defined.
  This is a breaking change.
* CQLPG-60: Follow refs so can have schema cycles.
* CQLPG-58: Performance: Convert id searches using '=' or '==' into primary
  key \_id searches. This is a breaking change because it restricts the
  operators allowed for id and assumes that id is a UUID.

## 2.2.3 2018-09-16
 * CQLPG-55: Trim trailing space and loose * that breaks fulltext search.

## 2.2.2 2018-09-13
 * CQLPG-54: Use the 'simple' dictionary for fulltext instead of 'english',
   to get around stopwords.

## 2.2.1 2018-09-06
 * CQLPG-53: Fix the title='*' search in fulltext, now it finds all where
   title is defined.
 * CQLPG-54: Optimize the title=* OR contributors=* OR identifier=* query.
   This is the most common query from the UI, issued every time before the
   user types anything in the search box. No need to do linear searches for
   that. Now CQL2PGJ ignores the right-hand side of an OR node if the left
   operand has '=' as operator and '*' as the term.

## 2.2.0 2018-08-24
 * CQLPG-37, CQLPG-46: Load the schema.json file and use that to detect if a
   given field has a fulltext index, and if so, translate that part of the
   query to use PGs tsvector stuff to use PGs fulltext indexing.

## 2.1.0 2018-06-26
 * CQLPG-34: Query builder - return where and orderBy clause separately.
 * CIRC-119: Add unit test for long list producing deeply nested SQL query.

## 2.0.0 2018-01-09
 * CQLPG-32: The default relation = now uses "adj" relation for strings (was "all").
 * CQLPG-23: Support "any" relation.
 * CQLPG-33: Performance test for number match.

## 1.3.4 2018-01-05
 * CQLPG-31: CQL number match when invoked without schema
 * CQLPG-30: Trigger on postgres numbers, not only on json numbers

## 1.3.3 2017-12-19
 * CQLPG-29: "sortBy field" sorts by null
 * CQLPG-21, CQLPG-23: Documentation of new relations: all adj

## 1.3.2 2017-12-05
 * MODINVSTOR-38: Fix matching a number in a string field, performance tests
 * CQLPG-21: Implement CQL's adj keyword for phrase matching

## 1.3.1 2017-12-01
 * UICHKOUT-39: Unit test for number in string field
 * CQLPG-4: Correctly handle numbers in number and string fields.

## 1.3.0 2017-11-28
 * CQLPG-6: Replace regexp by LIKE for <> and == for performance
 * CQLPG-9: Use trigram matching (pg_trgm)
 * CQLPG-10: Use unaccent instead of regexp for performance
 * CQLPG-11: IndexPerformanceTest unit test
 * CQLPG-12: bump external libraries to latest stable version
 * CQLPG-13: test ORDER BY performance using the output from CQL conversion
 * CQLPG-14: remove raw term in ORDER BY
 * CQLPG-18: name==123 should use lower(f_unaccent(...)) index
## 1.2.1 2017-11-14
 * Replace CASE jsonb_typeof(field) ... by an AND/OR expression (CQLPG-8)
 * For numeric values use -> not ->> to make use of the index. (CQLPG-7 partially)
## 1.2.0 2017-07-20
 * Implement multi-field queries (FOLIO-727)
## 1.1.0 2017-05-10
 * First code release
 * Update cql-java dependency to v1.13 (FOLIO-596)
