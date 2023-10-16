package utility;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Base64;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;

/**
 * The Encryption class.
 * Manage encryption and hashing.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class Encryption {
    public static final String AES = "AES";
    public static final String HMAC_SHA3_256 = "HmacSHA3-256";
    public static final String SHA3_256 = "SHA3-256";
    public static final int AES_KEY_SIZE = 256;
    private static final int SHA3_KEY_SIZE = 256;
    public static final int AES_INIT_VECTOR_SIZE = 16;
    private static final String AES_CIPHER_ALGORITHM = "AES/CBC/PKCS5PADDING";

    private Encryption() {}

    public static SecretKey createAESKey() {
        return Encryption.createKey(AES, AES_KEY_SIZE);
    }

    public static SecretKey createSHA3Key() {
        return Encryption.createKey(HMAC_SHA3_256, SHA3_KEY_SIZE);
    }

    private static SecretKey createKey(String algorithm, int keySize) {
        try {
            SecureRandom securerandom = new SecureRandom();
            KeyGenerator keygenerator = KeyGenerator.getInstance(algorithm);
            keygenerator.init(keySize, securerandom);
            return keygenerator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            Logger.err(Encryption.class, e);
        }
        return null;
    }

    public static byte[] createInitializationVector() {
        byte[] initializationVector = new byte[AES_INIT_VECTOR_SIZE];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(initializationVector);
        return initializationVector;
    }

    public static String encrypt(String plainText, SecretKey key, byte[] initVector) {
        try {
            Cipher cipher = Cipher.getInstance(AES_CIPHER_ALGORITHM);
            IvParameterSpec ivParameterSpec = new IvParameterSpec(initVector);
            cipher.init(Cipher.ENCRYPT_MODE, key, ivParameterSpec);
            byte[] encryptedMessage = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encryptedMessage);
        } catch (Exception e) {
            Logger.err(Encryption.class, e);
        }
        return null;
    }

    public static String decrypt(String cipherText, SecretKey key, byte[] initVector) throws IllegalBlockSizeException, BadPaddingException, IllegalArgumentException {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance(AES_CIPHER_ALGORITHM);
            IvParameterSpec ivParameterSpec = new IvParameterSpec(initVector);
            cipher.init(Cipher.DECRYPT_MODE, key, ivParameterSpec);
            byte[] result = cipher.doFinal(Base64.getDecoder().decode(cipherText));
            return new String(result, StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            Logger.err(Encryption.class, e);
        }
        return null;
    }

    public static BigInteger hash(String plainText, SecretKey key) {
        byte[] encryptionBytes = ArrayUtils.addAll(key.getEncoded(), plainText.getBytes(StandardCharsets.UTF_8));
        try {
            byte[] hashBytes = MessageDigest.getInstance(Encryption.SHA3_256).digest(encryptionBytes);
            return new BigInteger(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            Logger.err(Encryption.class, e);
        }
        return null;
    }

    public static Triple<String, String, String> removeSalt(Triple<String, String, String> tuple) {
        if (tuple.getLeft().contains("-")) {
            return  Triple.of(tuple.getLeft().split("-")[0], tuple.getMiddle(), tuple.getRight()) ;
        }
        return tuple;
    }

    public static String removeSalt(String s) {
        if (s.contains("-")) {
            if (s.contains("|")) {
                return s.substring(0, s.indexOf("-")) + s.substring(s.indexOf("|"));
            } else {
                return Encryption.getPrefix(s, "-");
            }
        }
        return s;
    }

    public static String getPrefix(String s, String c) {
        if (s.contains(c)) {
            return s.substring(0, s.indexOf(c));
        }
        return s;
    }

    public static String getSuffix(String s,String c) {
        if (s.contains(c)) {
            return s.substring(s.lastIndexOf(c) + 1);
        }
        return s;
    }
}
