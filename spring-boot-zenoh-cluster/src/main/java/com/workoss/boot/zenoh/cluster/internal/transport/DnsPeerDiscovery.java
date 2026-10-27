package com.workoss.boot.zenoh.cluster.internal.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class DnsPeerDiscovery implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DnsPeerDiscovery.class);

    public record Settings(String name, int port, long refreshMillis, String selfAddress,
                           String server, long timeoutMillis) {
        public Settings {
            name = trim(name);
            selfAddress = trim(selfAddress);
            server = trim(server);
            if (!name.isEmpty()) {
                if (name.length() > 253 || !name.matches(
                        "[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\\.?")) {
                    throw new IllegalArgumentException("Expected a DNS hostname: " + name);
                }
                validatePort(port);
                if (refreshMillis <= 0) {
                    throw new IllegalArgumentException("DNS refreshMillis must be positive");
                }
                if (timeoutMillis <= 0) {
                    throw new IllegalArgumentException("DNS timeoutMillis must be positive");
                }
            }
            if (!selfAddress.isEmpty()) {
                numericAddress(selfAddress);
            }
            if (!server.isEmpty()) {
                serverUrl(server);
            }
        }

        public boolean enabled() {
            return !name.isEmpty();
        }
    }

    @FunctionalInterface
    interface Resolver {
        List<InetAddress> resolve(Settings settings) throws Exception;
    }

    private final Settings settings;
    private final List<String> staticEndpoints;
    private final Consumer<List<String>> updater;
    private final String nodeId;
    private final Resolver resolver;
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private ScheduledExecutorService scheduler;
    // Guarded by lifecycle; only a successful updater call advances the applied snapshot.
    private List<String> applied;

    public DnsPeerDiscovery(Settings settings, List<String> staticEndpoints,
                            Consumer<List<String>> updater, String nodeId) {
        this(settings, staticEndpoints, updater, nodeId, DnsPeerDiscovery::resolveDns);
    }

    DnsPeerDiscovery(Settings settings, List<String> staticEndpoints,
                     Consumer<List<String>> updater, String nodeId, Resolver resolver) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.staticEndpoints = staticEndpoints == null ? List.of()
                : List.copyOf(new TreeSet<>(staticEndpoints));
        this.updater = Objects.requireNonNull(updater, "updater");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.applied = this.staticEndpoints;
    }

    public void start() {
        synchronized (lifecycle) {
            if (closed || scheduler != null || !settings.enabled()) {
                return;
            }
            scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "zenoh-cluster-dns-" + nodeId);
                thread.setDaemon(true);
                return thread;
            });
            scheduler.scheduleWithFixedDelay(this::refresh, 0, settings.refreshMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    // DNS/network work intentionally runs outside lifecycle: close must not wait for a DNS timeout.
    void refresh() {
        if (closed || !settings.enabled()) {
            return;
        }
        try {
            List<InetAddress> addresses = resolver.resolve(settings);
            Set<InetAddress> selfAddresses = selfAddresses();
            TreeSet<String> endpoints = new TreeSet<>(staticEndpoints);
            for (InetAddress address : new HashSet<>(addresses)) {
                if (!selfAddresses.contains(address)) {
                    String ip = address.getHostAddress();
                    endpoints.add((ip.contains(":") ? "[" + ip + "]" : ip)
                            + ":" + settings.port());
                }
            }
            List<String> next = List.copyOf(endpoints);
            synchronized (lifecycle) {
                if (!closed && !next.equals(applied)) {
                    try {
                        updater.accept(next);
                        applied = next;
                    } catch (RuntimeException e) {
                        applied = null;
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            // Incomplete DNS answers must not withdraw peers that may still be reachable.
            if (!closed) {
                logger.warn("DNS peer refresh failed for {}; will retry", settings.name(), e);
            }
        }
    }

    private Set<InetAddress> selfAddresses() throws Exception {
        if (!settings.selfAddress().isEmpty()) {
            return Set.of(numericAddress(settings.selfAddress()));
        }
        Set<InetAddress> addresses = new HashSet<>();
        var interfaces = NetworkInterface.getNetworkInterfaces();
        if (interfaces != null) {
            for (NetworkInterface networkInterface : Collections.list(interfaces)) {
                addresses.addAll(Collections.list(networkInterface.getInetAddresses()));
            }
        }
        return addresses;
    }

    @Override
    public void close() {
        ScheduledExecutorService executor;
        synchronized (lifecycle) {
            closed = true;
            executor = scheduler;
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static List<InetAddress> resolveDns(Settings settings) throws Exception {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        if (!settings.server().isEmpty()) {
            environment.put(Context.PROVIDER_URL, serverUrl(settings.server()));
        }
        environment.put("com.sun.jndi.dns.timeout.initial",
                Long.toString(Math.min(settings.timeoutMillis(), Integer.MAX_VALUE)));
        environment.put("com.sun.jndi.dns.timeout.retries", "1");
        DirContext context = new InitialDirContext(environment);
        try {
            List<InetAddress> addresses = new ArrayList<>();
            // Never request ANY: Kubernetes/CoreDNS can refuse it or omit address records.
            for (String type : List.of("A", "AAAA")) {
                Attributes attributes = context.getAttributes(settings.name(), new String[]{type});
                Attribute attribute = attributes.get(type);
                if (attribute == null) {
                    continue; // Successful NOERROR/NODATA response, not a lookup failure.
                }
                NamingEnumeration<?> values = attribute.getAll();
                try {
                    while (values.hasMore()) {
                        addresses.add(numericAddress(values.next().toString()));
                    }
                } finally {
                    values.close();
                }
            }
            return addresses;
        } finally {
            context.close();
        }
    }

    private static String trim(String value) {
        return value == null ? "" : value.trim();
    }

    private static void validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("DNS port must be between 1 and 65535");
        }
    }

    /** Parse literals only; never pass a hostname to InetAddress.getByName. */
    private static InetAddress numericAddress(String value) {
        String ip = value;
        if (ip.startsWith("[") && ip.endsWith("]")) {
            ip = ip.substring(1, ip.length() - 1);
        }
        try {
            if (ip.contains(":")) {
                if (!ip.matches("[0-9a-fA-F:.]+")) {
                    throw new IllegalArgumentException("Expected a numeric IPv4/IPv6 address: " + value);
                }
                return InetAddress.getByName(ip);
            }
            String[] parts = ip.split("\\.", -1);
            if (parts.length != 4) {
                throw new IllegalArgumentException("Expected a numeric IPv4/IPv6 address: " + value);
            }
            byte[] bytes = new byte[4];
            for (int i = 0; i < parts.length; i++) {
                if (!parts[i].matches("[0-9]{1,3}") || Integer.parseInt(parts[i]) > 255) {
                    throw new IllegalArgumentException("Expected a numeric IPv4/IPv6 address: " + value);
                }
                bytes[i] = (byte) Integer.parseInt(parts[i]);
            }
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Expected a numeric IPv4/IPv6 address: " + value, e);
        }
    }

    private static String serverUrl(String server) {
        String host = server;
        int port = 53;
        if (server.startsWith("[")) {
            int end = server.indexOf(']');
            if (end < 0) {
                throw new IllegalArgumentException("Invalid numeric DNS server: " + server);
            }
            host = server.substring(1, end);
            if (end + 1 < server.length()) {
                if (server.charAt(end + 1) != ':') {
                    throw new IllegalArgumentException("Invalid numeric DNS server: " + server);
                }
                port = parsePort(server.substring(end + 2));
            }
        } else if (server.indexOf(':') == server.lastIndexOf(':') && server.contains(":")) {
            int separator = server.indexOf(':');
            host = server.substring(0, separator);
            port = parsePort(server.substring(separator + 1));
        }
        String ip = numericAddress(host).getHostAddress();
        return "dns://" + (ip.contains(":") ? "[" + ip + "]" : ip) + ":" + port + "/";
    }

    private static int parsePort(String value) {
        if (!value.matches("[0-9]{1,5}")) {
            throw new IllegalArgumentException("Invalid DNS server port: " + value);
        }
        int port = Integer.parseInt(value);
        validatePort(port);
        return port;
    }
}
