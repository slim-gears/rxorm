package com.slimgears.rxrepo.sql;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class DigestKeyEncoder implements KeyEncoder {
    private final MessageDigest digestProvider;
    private final Base64.Encoder base64;
    private final int length;

    private DigestKeyEncoder(String algorithm, int length) {
        try {
            this.digestProvider = MessageDigest.getInstance(algorithm);
            this.base64 = Base64.getEncoder();
            this.length = length;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public String encode(Object key) {
        if (key == null) {
            return null;
        }
        byte[] digest = digestProvider.digest(key.toString().getBytes(StandardCharsets.UTF_8));
        String encodedKey = new String(base64.encode(digest));
        return length > 0
                ? encodedKey.substring(0, length)
                : (length < 0)
                ? encodedKey.substring(0, encodedKey.length() + length)
                : encodedKey;
    }

    public static KeyEncoder create(String algorithm, int length) {
        return new DigestKeyEncoder(algorithm, length);
    }

    public static KeyEncoder create() {
        return create("SHA-1", 0);
    }
}
