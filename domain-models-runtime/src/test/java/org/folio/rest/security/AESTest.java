package org.folio.rest.security;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Base64;
import javax.crypto.SecretKey;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.Test;

public final class AESTest {
  public static final String SECRET_KEY = "b2+S+X4F/NFys/0jMaEG1A";  // hex: 6f6f92f97e05fcd172b3fd2331a106d4
  public static final String PASSWORD = "fooBarBazIsTopSecret$%&/()='\"";
  public static final String ENCRYPTED_PASSWORD_BASE64 = "1887iEdyijDrmybKzcDV4LbnVM7iqdimwGOoe1T3lVs=";
  public static final byte [] ENCRYPTED_PASSWORD = Base64.getDecoder().decode(ENCRYPTED_PASSWORD_BASE64);

  @Test
  public void UtilityClass() {
    UtilityClassTester.assertUtilityClass(AES.class);
  }

  @Test
  public void getSet() {
    AES.setSecretKey("theKey");
    assertThat(AES.getSecretKey(), is("theKey"));
    AES.setSecretKey(SECRET_KEY);
    assertThat(AES.getSecretKey(), is(SECRET_KEY));
    AES.setSecretKey(null);
    assertThat(AES.getSecretKey(), is(nullValue()));
  }

  @Test
  public void generateDifferentKeys() throws Exception {
    SecretKey key1 = AES.generateSecretKey();
    SecretKey key2 = AES.generateSecretKey();
    assertThat(AES.convertSecretKeyToString(key1), is(not(AES.convertSecretKeyToString(key2))));
  }

  @Test
  public void encryptDecrypt() throws Exception {
    SecretKey secretKey = AES.getSecretKeyObject(SECRET_KEY);
    byte [] encrypted = AES.encryptPassword(PASSWORD, secretKey);
    assertThat(encrypted, is(ENCRYPTED_PASSWORD));
    assertThat(AES.decryptPassword(encrypted, secretKey), is(PASSWORD));
  }

  @Test
  public void encryptDecryptBase64() throws Exception {
    SecretKey secretKey = AES.getSecretKeyObject(SECRET_KEY);
    String encrypted = AES.encryptPasswordAsBase64(PASSWORD, secretKey);
    assertThat(encrypted, is(ENCRYPTED_PASSWORD_BASE64));
    assertThat(AES.decryptPassword(encrypted, secretKey), is(PASSWORD));
  }
}
