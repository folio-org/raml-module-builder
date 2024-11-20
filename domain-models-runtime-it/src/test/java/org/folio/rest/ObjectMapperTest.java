package org.folio.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.folio.dbschema.ObjectMapperTool;
import org.folio.rest.jaxrs.model.Bee;
import org.folio.rest.jaxrs.model.Bees;
import org.junit.jupiter.api.Test;

class ObjectMapperTest {

  @Test
  @SuppressWarnings("java:S125")  // suppress false positive "block of commented-out lines of code should be removed."
  void skipEmptyArrayOnWrite() {
    var bees = new Bees();
    var s = ObjectMapperTool.valueAsString(bees);
    assertThat(s, is("{}"));  // no empty array { "bees": [] }
  }

  @Test
  void readMissingArray() {
    var bees = ObjectMapperTool.readValue("{}",  Bees.class);
    assertThat(bees.getBees(), is(notNullValue()));
    assertThat(bees.getBees().size(), is(0));
  }

  @Test
  @SuppressWarnings("java:S125")  // suppress false positive "block of commented-out lines of code should be removed."
  void skipEmptyHashOnWrite() {
    var bee = new Bee();
    var s = ObjectMapperTool.valueAsString(bee);
    assertThat(s, is("{}"));  // no empty object { "additionalProperties": {} }
  }

  @Test
  void readMissingObject() {
    var bees = ObjectMapperTool.readValue("{}",  Bee.class);
    assertThat(bees.getAdditionalProperties(), is(notNullValue()));
    assertThat(bees.getAdditionalProperties().size(), is(0));
  }
}
