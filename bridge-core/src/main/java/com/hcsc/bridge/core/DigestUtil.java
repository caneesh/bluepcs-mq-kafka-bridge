package com.hcsc.bridge.core;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The ONE SHA-256/hex implementation. Event IDs, HDFS write checksums, and HDFS file
 * checksums must all agree byte-for-byte — three private copies of this routine is how
 * an algorithm change gets applied to two of them.
 */
public final class DigestUtil {

    private DigestUtil() {
    }

    public static String sha256Hex(byte[] content) {
        return bytesToHex(newSha256().digest(content));
    }

    /** Streaming variant for file contents; does not close the stream. */
    public static String sha256Hex(InputStream in) throws IOException {
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
            digest.update(buffer, 0, bytesRead);
        }
        return bytesToHex(digest.digest());
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
