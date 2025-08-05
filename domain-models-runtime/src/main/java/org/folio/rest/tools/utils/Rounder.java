package org.folio.rest.tools.utils;

public final class Rounder {
  private Rounder() {
  }

  /**
   * i rounded to the left-most digit.
   */
  public static int roundToLeftDigit(int i) {
    if (i < -10) {
      if (i <= -1_500_000_000) {  // avoids overflow on -Integer.MIN_VALUE
        return -2_000_000_000;
      }
      return -roundToLeftDigit(-i);
    }
    if (i <= 10) {
      return i;
    }
    var factor = 1;
    while (i > 99) {
      i /= 10;
      factor *= 10;
    }
    var secondDigit = i % 10;
    i -= secondDigit;
    if (secondDigit >= 5) {
      i += 10;
    }
    return i * factor;
  }
}
