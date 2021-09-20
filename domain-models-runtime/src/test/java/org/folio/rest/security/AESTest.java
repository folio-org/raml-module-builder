package org.folio.rest.security;

import org.folio.rest.testing.UtilityClassTester;
import static org.hamcrest.Matchers.is;
import org.junit.Test;

import javax.crypto.SecretKey;

import static org.hamcrest.MatcherAssert.assertThat;

public final class AESTest {
  public static final String SECRET_KEY = "b2+S+X4F/NFys/0jMaEG1A";  // hex: 6f6f92f97e05fcd172b3fd2331a106d4
  public static final String PASSWORD = "fooBarBazIsTopSecret$%&/()='\"";
  public static final String ENCRYPTED_PASSWORD_BASE64 = "1887iEdyijDrmybKzcDV4LbnVM7iqdimwGOoe1T3lVs=";

  @Test
  public void UtilityClass() throws Exception {
    UtilityClassTester.assertUtilityClass(AES.class);
    }
  @Test
  public void encryptDecryptBase64() throws Exception {
    SecretKey secretKey = AES.getSecretKeyObject(SECRET_KEY);
    String encrypted = AES.encryptPasswordAsBase64(PASSWORD, secretKey);
    assertThat(encrypted, is(ENCRYPTED_PASSWORD_BASE64));
    assertThat(AES.decryptPassword(encrypted, secretKey), is(PASSWORD));
  }
}
