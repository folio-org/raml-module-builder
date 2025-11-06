package org.folio.rest.persist.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import java.util.List;
import org.junit.jupiter.api.Test;

class LocalRowDescTest {

  @Test
  void test() {
    var localRowDesc = new LocalRowDesc(List.of("foo", "bar"));
    assertThat(localRowDesc.columnNames(), contains("foo", "bar"));
    var descriptor1 = localRowDesc.columnDescriptor();
    var descriptor2 = localRowDesc.columnDescriptor();
    assertThat(descriptor1.get(0).name(), is("foo"));
    assertThat(descriptor1.get(1).name(), is("bar"));
    assertThat(descriptor2, is(sameInstance(descriptor1)));
  }

}
