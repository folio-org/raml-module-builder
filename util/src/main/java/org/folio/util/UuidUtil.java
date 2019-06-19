package org.folio.util;

import java.util.regex.Pattern;

/**
 * Validates a UUID.
 */
public final class UuidUtil {
  private static final Pattern UUID_PATTERN = Pattern.compile(
      "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[1-5][a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$");
  private static final Pattern LOOSE_UUID_PATTERN = Pattern.compile(
      "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");

  private UuidUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Validates as a UUID of the form xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx where the version M
   * must be 1-5 and the variant N must be 8, 9, a, b, A or B and any x must be from
   * 0-9, a-f and A-F.
   *
   * <p>This includes all v1, v2, v3, v4, and v5 UUID and excludes the Nil UUID 00000000-0000-0000-0000-000000000000.
   *
   * @param input the String to validate as a UUID
   * @return true if valid, false otherwise or if input is null
   * @see <a href="https://dev.folio.org/guides/uuids/">https://dev.folio.org/guides/uuids/</a>
   * @see <a href="https://en.wikipedia.org/wiki/Universally_unique_identifier">https://en.wikipedia.org/wiki/Universally_unique_identifier</a>
   */
  public static boolean isUuid(CharSequence input) {
    if (input == null) {
      return false;
    }
    return UUID_PATTERN.matcher(input).find();
  }

  /**
   * Validates as a UUID of the form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx where x is from 0-9, a-f and A-F.
   *
   * <p>This includes the Nil UUID 00000000-0000-0000-0000-000000000000.
   *
   * <p>This also includes a UUID with invalid version digit M or invalid variant digit N, see the
   * {@link #isUuid(String) isUuid} method for details.
   *
   * @param input the String to validate as a loose UUID
   * @return true if valid, false otherwise or if input is null
   * @see <a href="https://dev.folio.org/guides/uuids/">https://dev.folio.org/guides/uuids/</a>
   * @see <a href="https://en.wikipedia.org/wiki/Universally_unique_identifier">https://en.wikipedia.org/wiki/Universally_unique_identifier</a>
   */
  public static boolean isLooseUuid(CharSequence input) {
    if (input == null) {
      return false;
    }
    return LOOSE_UUID_PATTERN.matcher(input).find();
  }
}
