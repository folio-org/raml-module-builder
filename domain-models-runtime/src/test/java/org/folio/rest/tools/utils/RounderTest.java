package org.folio.rest.tools.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RounderTest {

  @ParameterizedTest
  @CsvSource(textBlock = """
      0, 0
      1, 1
      4, 4
      5, 5
      6, 6
      9, 9
      10, 10
      11, 10
      14, 10
      15, 20
      19, 20
      20, 20
      21, 20
      24, 20
      25, 30
      94, 90
      95, 100
      99, 100
      101, 100
      149, 100
      150, 200
      199, 200
      499, 500
      500, 500
      501, 500
      999, 1000
      8100, 8000
      89999, 90000
      950000, 1000000
       0x7fffffff,  2000000000
      -0x80000000, -2000000000
      """)
  void roundToLeftDigit(int i, int expected) {
    assertThat(Rounder.roundToLeftDigit(i), is(expected));
    if (i > 0) {
      assertThat(Rounder.roundToLeftDigit(-i), is(-expected));
    }
  }

}
