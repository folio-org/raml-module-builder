package org.folio.rest.security;

import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
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

  /** generate a secret key to use for encrypting a password */
  public static SecretKey generateSecretKey() throws NoSuchAlgorithmException {
    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(128);
    SecretKey secretKey = keyGen.generateKey();
    return secretKey;
  }

  /** encrypt a password with the secret key object */
  public static byte[] encryptPassword(String password, SecretKey secretKey) throws Exception {
    Cipher aesCipherForEncryption = Cipher.getInstance("AES/GCM/NoPadding");
    aesCipherForEncryption.init(Cipher.ENCRYPT_MODE, secretKey);
    byte[] byteDataToEncrypt = password.getBytes();
    byte[] byteCipherText = aesCipherForEncryption
        .doFinal(byteDataToEncrypt);
    return byteCipherText;
  }

  /** encrypt a password with the secret key and get back a base64 representation of the password */
  public static String encryptPasswordAsBase64(String password, SecretKey secretKey) throws Exception {
    Cipher aesCipherForEncryption = Cipher.getInstance("AES/GCM/NoPadding");
    aesCipherForEncryption.init(Cipher.ENCRYPT_MODE, secretKey);
    byte[] byteDataToEncrypt = password.getBytes();
    byte[] byteCipherText = aesCipherForEncryption
        .doFinal(byteDataToEncrypt);
    return Base64.getEncoder().encodeToString(byteCipherText);
  }

  /** decode a password using the secret key */
  public static String decryptPassword(byte []encryptedPassword, SecretKey secretKey) throws Exception {
    Cipher aesCipherForDecryption = Cipher.getInstance("AES/GCM/NoPadding");
    aesCipherForDecryption.init(Cipher.DECRYPT_MODE, secretKey);
    byte[] byteDecryptedText = aesCipherForDecryption
        .doFinal(encryptedPassword);
    return new String(byteDecryptedText);
  }

  /** decode a base64 password with the secret key */
  public static String decryptPassword(String encryptedPasswordAsBase64, SecretKey secretKey) throws Exception {
    Cipher aesCipherForDecryption = Cipher.getInstance("AES/GCM/NoPadding");
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

  /** convert a secret key object to a string base64 representation */
  public static String convertSecretKeyToString(SecretKey secretKey) throws Exception {
    byte[] encoded = secretKey.getEncoded();
    String output = Base64.getEncoder().withoutPadding().encodeToString(encoded);
    return output;
  }

}
