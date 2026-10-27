package com.workoss.boot.zenoh.cluster.internal;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Injective encoding of arbitrary strings into a single Zenoh key-expression chunk.
 * <p>
 * {@code [A-Za-z0-9_.-]} pass through; every other UTF-8 byte becomes {@code %XX}. The output
 * therefore never contains {@code /}, wildcards or the reserved {@code # ? $ @} characters, and two
 * different inputs never share a chunk.
 */
public final class KeyEscaper {

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private KeyEscaper() {
    }

    /**
     * @throws IllegalArgumentException for an empty input, which has no valid chunk
     */
    public static String escape(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Key component must not be empty");
        }
        return escapeAllowEmpty(value);
    }

    /**
     * Same as {@link #escape} but maps {@code ""} to {@code ""}; callers must prefix the result so
     * the final chunk is non-empty.
     */
    public static String escapeAllowEmpty(String value) {
        if (isPlain(value)) {
            return value;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            char c = (char) (b & 0xFF);
            if (isPlainChar(c)) {
                sb.append(c);
            } else {
                sb.append('%').append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
            }
        }
        return sb.toString();
    }

    /**
     * Escapes each {@code /}-separated segment on its own, keeping {@code /} as the chunk separator.
     *
     * @throws IllegalArgumentException for an empty input or an empty segment ({@code a//b}, {@code /a}, {@code a/})
     */
    public static String escapePath(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Key path must not be empty");
        }
        String[] segments = path.split("/", -1);
        StringBuilder sb = new StringBuilder(path.length());
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].isEmpty()) {
                throw new IllegalArgumentException("Key path must not contain empty segments: " + path);
            }
            if (i > 0) {
                sb.append('/');
            }
            sb.append(escapeAllowEmpty(segments[i]));
        }
        return sb.toString();
    }

    public static String unescapePath(String path) {
        if (path.indexOf('%') < 0) {
            return path;
        }
        String[] chunks = path.split("/", -1);
        StringBuilder sb = new StringBuilder(path.length());
        for (int i = 0; i < chunks.length; i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(unescape(chunks[i]));
        }
        return sb.toString();
    }

    public static String unescape(String chunk) {
        if (chunk.indexOf('%') < 0) {
            return chunk;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(chunk.length());
        for (int i = 0; i < chunk.length(); i++) {
            char c = chunk.charAt(i);
            if (c == '%' && i + 2 < chunk.length()) {
                out.write((Character.digit(chunk.charAt(i + 1), 16) << 4) | Character.digit(chunk.charAt(i + 2), 16));
                i += 2;
            } else {
                out.write(c);
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static boolean isPlain(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!isPlainChar(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPlainChar(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || c == '_' || c == '.' || c == '-';
    }
}
