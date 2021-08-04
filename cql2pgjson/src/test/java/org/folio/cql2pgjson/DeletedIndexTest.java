package org.folio.cql2pgjson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

import org.junit.Test;

public class DeletedIndexTest {

  @Test
  public void multiFieldNamesNonUniqueIndex() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("users");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/schemaWithDeletedIndex.json");
    // uniqueIndex, "caseSensitive": true, "removeAccents": false
    assertThat(cql2pgJson.toSql("a == x").toString(), containsString("LIKE 'x'"));
    // index, "caseSensitive": false, "removeAccents": false
    assertThat(cql2pgJson.toSql("b == y").toString(), containsString("LIKE lower('y')"));
    // full text index, "caseSensitive": false, "removeAccents": true
    assertThat(cql2pgJson.toSql("c == z").toString(), containsString("LIKE lower(f_unaccent('z'))"));
  }

}
