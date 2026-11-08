package com.workoss.boot.vertx.contactcenter.uctp;

import java.security.SecureRandom;
import java.util.Base64;

public final class UctpIds {

    private static final SecureRandom RANDOM = new SecureRandom();
    private final SnowflakeGenerator generator;

    public UctpIds(SnowflakeGenerator generator) {
        this.generator = generator;
    }

    public long nextId() {
        return generator.nextId();
    }

    public String prefixed(String prefix) {
        return prefix + nextId();
    }

    public static String sessionToken() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return "tok_" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
