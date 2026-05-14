package com.vertx.spring.boot.web.server;

import org.springframework.http.server.reactive.SslInfo;

import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of SslInfo backed by Vert.x SSL session.
 */
public class VertxSslInfo implements SslInfo {

    private final SSLSession sslSession;

    public VertxSslInfo(SSLSession sslSession) {
        this.sslSession = sslSession;
    }

    @Override
    public String getSessionId() {
        if (sslSession != null && sslSession.getId() != null) {
            StringBuilder sb = new StringBuilder();
            for (byte b : sslSession.getId()) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
        return null;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        if (sslSession != null) {
            try {
                Certificate[] certs = sslSession.getPeerCertificates();
                if (certs != null) {
                    List<X509Certificate> x509Certs = new ArrayList<>();
                    for (Certificate cert : certs) {
                        if (cert instanceof X509Certificate x509) {
                            x509Certs.add(x509);
                        }
                    }
                    return x509Certs.toArray(new X509Certificate[0]);
                }
            } catch (Exception e) {
                // Peer certificates not available
            }
        }
        return null;
    }
}
