package org.folio.rest.security;

import org.folio.rest.testing.UtilityClassTester;
import org.junit.Test;

import javax.crypto.SecretKey;

public final class AESTest {
  @Test
  public void UtilityClass() {
    UtilityClassTester.assertUtilityClass(AES.class);}
}
