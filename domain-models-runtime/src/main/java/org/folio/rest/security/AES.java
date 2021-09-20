package org.folio.rest.security;

import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public final class AES {

  private static String secretKey = null;

  private AES() {
    throw new UnsupportedOperationException("Cannot instantiate AES class");
  }

  /** get the secret key to use for decoding a password*/
  public static String getSecretKey() {
    return secretKey;
  }

  /** set the secret key to use for decoding a password*/
  public static void setSecretKey(String key) {
    secretKey = key;
  }

  /** encrypt a password with the secret key and get back a base64 representation of the password */
  @SuppressWarnings("all")
  public static String encryptPasswordAsBase64(String password, SecretKey secretKey) throws Exception {
    Cipher aesCipherForEncryption = Cipher.getInstance("AES");//using provider-specific default values for the mode and padding scheme.
    aesCipherForEncryption.init(Cipher.ENCRYPT_MODE, secretKey);
    byte[] byteDataToEncrypt = password.getBytes();
    byte[] byteCipherText = aesCipherForEncryption
        .doFinal(byteDataToEncrypt);
    return Base64.getEncoder().encodeToString(byteCipherText);
  }

  /** decode a base64 password with the secret key */
  @SuppressWarnings("all")
  public static String decryptPassword(String encryptedPasswordAsBase64, SecretKey secretKey) throws Exception {
    Cipher aesCipherForDecryption = Cipher.getInstance("AES");//using provider-specific default values for the mode and padding scheme.
    aesCipherForDecryption.init(Cipher.DECRYPT_MODE, secretKey);
    byte[] byteDecryptedText = aesCipherForDecryption
        .doFinal(Base64.getDecoder().decode(encryptedPasswordAsBase64));
    return new String(byteDecryptedText);
  }

  /** convert a secret key from a base64 representation to a secret key object */
  public static SecretKey getSecretKeyObject(String key) {
    byte[] encKey = Base64.getDecoder().decode(key);
    SecretKey aesKey = new SecretKeySpec(encKey, "AES");
    return aesKey;
  }
}
