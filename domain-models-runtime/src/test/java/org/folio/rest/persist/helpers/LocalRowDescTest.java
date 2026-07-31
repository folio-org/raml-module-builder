package org.folio.rest.persist.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LocalRowDescTest {

  @Test
  void constructor() {
    var localRowDesc = new LocalRowDesc(List.of("foo", "bar"));
    assertThat(localRowDesc.columnNames(), contains("foo", "bar"));
    var descriptor1 = localRowDesc.columnDescriptor();
    var descriptor2 = localRowDesc.columnDescriptor();
    assertThat(descriptor1.get(0).name(), is("foo"));
    assertThat(descriptor1.get(1).name(), is("bar"));
    assertThat(descriptor2, is(sameInstance(descriptor1)));
  }

  @Test
  void columnIndexNull() {
    var localRowDesc = new LocalRowDesc(List.of("foo", "bar"));
    assertThrows(NullPointerException.class, () -> localRowDesc.columnIndex(null));
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      baz, 2
      foo, 0
      bar, 1
      """)
  void columnIndex(String columnName, int index) {
    var localRowDesc = new LocalRowDesc(List.of("foo", "bar", "baz"));
    assertThat(localRowDesc.columnIndex(columnName), is(index));
    // cached columnNames List
    assertThat(localRowDesc.columnIndex(columnName), is(index));
  }
}
