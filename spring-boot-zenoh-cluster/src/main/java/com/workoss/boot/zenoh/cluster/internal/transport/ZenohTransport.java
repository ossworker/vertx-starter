package com.workoss.boot.zenoh.cluster.internal.transport;

import com.workoss.zenoh.Config;
import com.workoss.zenoh.EmbeddedRouter;
import com.workoss.zenoh.Query;
import com.workoss.zenoh.Queryable;
import com.workoss.zenoh.Reply;
import com.workoss.zenoh.Sample;
import com.workoss.zenoh.Session;
import com.workoss.zenoh.Subscriber;
import com.workoss.zenoh.link.Endpoint;
import com.workoss.zenoh.link.LinkProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Owns one Zenoh session (and, in router mode, the embedded router behind it) together with every
 * subscriber and queryable declared on it.
 * <p>
 * zenoh-jni only offers blocking receive, so each subscription gets a dedicated daemon thread
 * parked in {@code recv()}; undeclaring wakes it up. Handlers run on that thread and must hand
 * slow work off.
 */
public final class ZenohTransport implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ZenohTransport.class);
    private static final long JOIN_TIMEOUT_MILLIS = 2_000L;

    public enum Mode { PEER, CLIENT, ROUTER }

    /** Protocols whose wildcard address {@code [::]:7447} is a meaningful router default. */
    private static final Set<LinkProtocol> IP_PROTOCOLS = EnumSet.of(
            LinkProtocol.TCP, LinkProtocol.UDP, LinkProtocol.TLS, LinkProtocol.QUIC, LinkProtocol.WS);
    private static final String DEFAULT_ROUTER_ADDRESS = "[::]:7447";

    /**
     * @param protocol   link protocol for endpoints written without one ({@code 10.0.0.1:7447});
     *                   endpoints with an explicit {@code <protocol>/} prefix keep theirs, so one
     *                   property switches the whole topology while mixing stays possible
     * @param configFile optional Zenoh JSON5 file; mode and endpoints are applied on top of it
     * @param config     arbitrary Zenoh configuration applied on top of {@code configFile} as
     *                   JSON5 values; explicit {@code mode}/{@code connect}/{@code listen} win
     *                   over entries targeting the same underlying keys. Use it for tuning that
     *                   has no dedicated property, e.g. {@code transport/link/tls/*} for mTLS
     *                   over QUIC/TLS or {@code transport/link/protocols} to restrict schemes.
     */
    public record Settings(Mode mode, LinkProtocol protocol, List<String> connect, List<String> listen,
                           String configFile, Map<String, String> config) {

        public Settings {
            mode = mode == null ? Mode.PEER : mode;
            protocol = protocol == null ? LinkProtocol.TCP : protocol;
            connect = connect == null ? List.of() : List.copyOf(connect);
            listen = listen == null ? List.of() : List.copyOf(listen);
            config = config == null ? Map.of() : Map.copyOf(config);
        }
    }

    private NativeSession nativeSession;
    private Settings settings;
    private final String threadSuffix;
    private final ExecutorService publisher;
    private final List<Registration> channels = new ArrayList<>();
    private final ReentrantReadWriteLock sessionLock = new ReentrantReadWriteLock(true);
    private volatile boolean closed;

    private ZenohTransport(Settings settings, String threadSuffix) {
        this.settings = settings;
        this.nativeSession = openSession(settings, false);
        this.threadSuffix = threadSuffix;
        // Single thread: replication writes for one key must leave in the order they were made.
        this.publisher = Executors.newSingleThreadExecutor(r -> daemon(r, "zenoh-cluster-publish-" + threadSuffix));
    }

    public static ZenohTransport open(Settings settings, String threadSuffix) {
        return new ZenohTransport(settings, threadSuffix);
    }

    private static NativeSession openSession(Settings settings, boolean replaceConnect) {
        // Fail before any native call: a typo'd endpoint otherwise surfaces as an opaque
        // Zenoh error at session open.
        List<Endpoint> connect = resolveEndpoints(settings.connect(), settings.protocol(), "connect");
        List<Endpoint> listen = resolveEndpoints(settings.listen(), settings.protocol(), "listen");
        if (settings.mode() == Mode.ROUTER && listen.isEmpty()) {
            listen = List.of(defaultRouterListen(settings.protocol()));
        }
        logger.info("Opening Zenoh session: mode={}, protocol={}, connect={}, listen={}, config={}",
                settings.mode(), settings.protocol().scheme(), connect, listen, settings.config().keySet());
        warnOnMissingTlsMaterial(settings, connect, listen);
        Config config = baseConfig(settings);
        try {
            config.setMode(settings.mode().name().toLowerCase(Locale.ROOT));
            if (replaceConnect || !connect.isEmpty()) {
                config.setConnectEndpoints(schemes(connect));
            }
            if (!listen.isEmpty()) {
                config.setListenEndpoints(schemes(listen));
            }
            if (settings.mode() == Mode.ROUTER) {
                return openWithRouter(config);
            }
            if (connect.isEmpty() && listen.isEmpty()) {
                logger.warn("No connect/listen endpoints configured; discovery relies on Zenoh "
                        + "scouting (multicast). Configure zenoh.cluster.connect/listen for a "
                        + "deterministic topology.");
            }
            return new NativeSession(new Session(config), null);
        } finally {
            config.close();
        }
    }

    /**
     * The node uses the router's own session: no loopback link, so the router's listeners can be
     * any protocol (including QUIC/TLS) without the node needing client TLS material for itself.
     */
    private static NativeSession openWithRouter(Config config) {
        EmbeddedRouter router = new EmbeddedRouter(config);
        try {
            return new NativeSession(new Session(router), router);
        } catch (RuntimeException e) {
            closeRouter(router);
            throw e;
        }
    }

    private static Config baseConfig(Settings settings) {
        Config config = settings.configFile() == null || settings.configFile().isBlank()
                ? Config.defaultConfig() : new Config(settings.configFile());
        try {
            applyConfig(config, settings.config());
            return config;
        } catch (RuntimeException e) {
            config.close();
            throw e;
        }
    }

    private static Endpoint defaultRouterListen(LinkProtocol protocol) {
        if (!IP_PROTOCOLS.contains(protocol)) {
            throw new IllegalArgumentException("zenoh.cluster.listen is required in router mode when "
                    + "zenoh.cluster.protocol is '" + protocol.scheme() + "'");
        }
        try {
            return Endpoint.resolve(DEFAULT_ROUTER_ADDRESS, protocol).requireSupported();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid zenoh.cluster.protocol: " + e.getMessage(), e);
        }
    }

    /**
     * Binds bare addresses to {@code protocol} and rejects malformed endpoints and protocols the
     * native library was built without. Address syntax (including endpoint parameters like
     * {@code quic/host:7447?rel=0#initial_mtu=1200}) is left to Zenoh, which reports it precisely.
     */
    static List<Endpoint> resolveEndpoints(List<String> endpoints, LinkProtocol protocol, String kind) {
        if (endpoints == null || endpoints.isEmpty()) {
            return List.of();
        }
        List<Endpoint> resolved = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints) {
            try {
                resolved.add(Endpoint.resolve(endpoint, protocol).requireSupported());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid zenoh.cluster." + kind + ": " + e.getMessage(), e);
            }
        }
        return List.copyOf(resolved);
    }

    private static List<String> schemes(List<Endpoint> endpoints) {
        return endpoints.stream().map(Endpoint::toString).toList();
    }

    /**
     * Inserts each entry into the Zenoh config as a JSON5 value. An entry whose value is not
     * valid JSON5 fails here with the offending key; entries Zenoh does not know are kept but
     * ignored. A value that parses but has the wrong type surfaces when the session opens.
     */
    private static void applyConfig(Config config, Map<String, String> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            try {
                config.insertJson5(entry.getKey(), entry.getValue());
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Invalid zenoh.cluster.config entry '"
                        + entry.getKey() + "': value must be valid JSON5: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Zenoh 1.x requires TLS material on QUIC/TLS links: a listener without a private key fails
     * with "Missing TLS private key" at session open, and a connector without a root CA cannot
     * validate the peer. A config file may carry the material, so only warn when neither the
     * config map nor a config file provides it.
     */
    private static void warnOnMissingTlsMaterial(Settings settings, List<Endpoint> connect,
                                                 List<Endpoint> listen) {
        if (settings.configFile() != null && !settings.configFile().isBlank()) {
            return;
        }
        Map<String, String> config = settings.config();
        if (usesTls(listen) && !config.containsKey("transport/link/tls/listen_certificate")) {
            logger.warn("Listening on QUIC/TLS without transport/link/tls/listen_certificate and "
                    + "listen_private_key; Zenoh will reject the listener. Set them via "
                    + "zenoh.cluster.tls or zenoh.cluster.config.");
        }
        if (usesTls(connect) && !config.containsKey("transport/link/tls/root_ca_certificate")) {
            logger.warn("Connecting over QUIC/TLS without transport/link/tls/root_ca_certificate; "
                    + "Zenoh will not be able to validate the peer. Set it via "
                    + "zenoh.cluster.tls or zenoh.cluster.config.");
        }
    }

    private static boolean usesTls(List<Endpoint> endpoints) {
        return endpoints.stream().anyMatch(e -> e.protocol().requiresTls());
    }

    public String sessionId() {
        sessionLock.readLock().lock();
        try {
            return session().zid();
        } finally {
            sessionLock.readLock().unlock();
        }
    }

    public void put(String key, byte[] payload) {
        sessionLock.readLock().lock();
        try {
            session().put(key, payload);
        } finally {
            sessionLock.readLock().unlock();
        }
    }

    public synchronized void updateConnectEndpoints(List<String> endpoints) {
        if (closed) {
            return;
        }
        List<String> resolved = schemes(resolveEndpoints(endpoints, settings.protocol(), "connect"))
                .stream().distinct().sorted().toList();
        List<String> previous = schemes(resolveEndpoints(settings.connect(), settings.protocol(), "connect"))
                .stream().distinct().sorted().toList();
        if (nativeSession != null && resolved.equals(previous)) {
            return;
        }
        Settings next = new Settings(settings.mode(), settings.protocol(), resolved, settings.listen(),
                settings.configFile(), settings.config());
        // Zenoh 1.10 only allows plugins/* hot writes; endpoints require reopening the native session.
        sessionLock.writeLock().lock();
        try {
            closeChannels();
            closeNativeSession();
            try {
                nativeSession = openSession(next, true);
                for (Registration registration : channels) {
                    registration.channel = registration.declare.apply(nativeSession.session());
                }
                settings = next;
                logger.info("Zenoh peer connections updated: {}", resolved);
            } catch (RuntimeException e) {
                closeChannels();
                closeNativeSession();
                throw e;
            }
        } finally {
            sessionLock.writeLock().unlock();
        }
    }

    private Session session() {
        if (closed || nativeSession == null) {
            throw new IllegalStateException("Zenoh transport is not connected");
        }
        return nativeSession.session();
    }

    /**
     * Ordered, fire-and-forget put; failures are logged.
     */
    public void putAsync(String key, byte[] payload) {
        try {
            publisher.execute(() -> {
                try {
                    put(key, payload);
                } catch (Exception e) {
                    logger.warn("Failed to publish '{}': {}", key, e.getMessage());
                }
            });
        } catch (RejectedExecutionException e) {
            logger.debug("Dropped publish of '{}' after transport close", key);
        }
    }

    public void delete(String key) {
        sessionLock.readLock().lock();
        try {
            session().delete(key);
        } finally {
            sessionLock.readLock().unlock();
        }
    }

    /**
     * Blocking query; returns once every matching queryable has answered or the Zenoh query
     * timeout elapses.
     */
    public List<Reply> get(String selector) {
        sessionLock.readLock().lock();
        try {
            return session().get(selector);
        } finally {
            sessionLock.readLock().unlock();
        }
    }

    public synchronized AutoCloseable subscribe(String keyExpr, Consumer<Sample> handler, String threadName) {
        return register(session -> {
            Subscriber subscriber = new Subscriber(session, keyExpr);
            Channel channel = new Channel(subscriber::undeclare, subscriber::close);
            channel.start(() -> {
                while (!channel.closed) {
                    Optional<Sample> sample;
                    try {
                        sample = subscriber.recv();
                    } catch (RuntimeException e) {
                        if (channel.closed) {
                            break;
                        }
                        logger.debug("recv on {} failed: {}", keyExpr, e.getMessage());
                        pause();
                        continue;
                    }
                    if (sample.isEmpty()) {
                        break;
                    }
                    try {
                        handler.accept(sample.get());
                    } catch (Exception e) {
                        logger.warn("Handler for {} failed", keyExpr, e);
                    }
                }
            }, threadName + "-" + threadSuffix);
            return channel;
        });
    }

    /**
     * Each query is closed after {@code handler} returns, which tells the requester no more
     * replies are coming; a handler that replies nothing therefore answers "no data".
     */
    public synchronized AutoCloseable serve(String keyExpr, Consumer<Query> handler, String threadName) {
        return register(session -> {
            Queryable queryable = new Queryable(session, keyExpr);
            Channel channel = new Channel(queryable::undeclare, queryable::close);
            channel.start(() -> {
                while (!channel.closed) {
                    Query query;
                    try {
                        query = new Query(queryable);
                    } catch (RuntimeException e) {
                        if (channel.closed || queryable.isUndeclared()) {
                            break;
                        }
                        logger.debug("Query receive on {} failed: {}", keyExpr, e.getMessage());
                        pause();
                        continue;
                    }
                    try (query) {
                        handler.accept(query);
                    } catch (Exception e) {
                        logger.warn("Query handler for {} failed", keyExpr, e);
                    }
                }
            }, threadName + "-" + threadSuffix);
            return channel;
        });
    }

    @Override
    public void close() {
        publisher.shutdown();
        try {
            if (!publisher.awaitTermination(JOIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                publisher.shutdownNow();
            }
        } catch (InterruptedException e) {
            publisher.shutdownNow();
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            sessionLock.writeLock().lock();
            try {
                closed = true;
                closeChannels();
                channels.clear();
                closeNativeSession();
            } finally {
                sessionLock.writeLock().unlock();
            }
        }
        logger.info("Zenoh session closed");
    }

    private AutoCloseable register(Function<Session, Channel> declare) {
        Registration registration = new Registration(declare, declare.apply(session()));
        channels.add(registration);
        return () -> {
            synchronized (this) {
                if (channels.remove(registration) && registration.channel != null) {
                    registration.channel.close();
                }
            }
        };
    }

    private void closeChannels() {
        for (Registration registration : channels) {
            if (registration.channel != null) {
                registration.channel.close();
                registration.channel = null;
            }
        }
    }

    private void closeNativeSession() {
        if (nativeSession == null) {
            return;
        }
        Session session = nativeSession.session();
        EmbeddedRouter router = nativeSession.router();
        nativeSession = null;
        // Router-owned sessions must release their handle before closing the router itself.
        if (router == null) {
            try {
                session.closeSession();
            } catch (Exception e) {
                logger.debug("Failed to close Zenoh session: {}", e.getMessage());
            }
        }
        session.close();
        if (router != null) {
            closeRouter(router);
        }
    }

    private record NativeSession(Session session, EmbeddedRouter router) {
    }

    private static final class Registration {
        private final Function<Session, Channel> declare;
        private Channel channel;

        private Registration(Function<Session, Channel> declare, Channel channel) {
            this.declare = declare;
            this.channel = channel;
        }
    }

    private static void closeRouter(EmbeddedRouter router) {
        try {
            router.closeRouter();
        } catch (Exception e) {
            logger.debug("Failed to close embedded router: {}", e.getMessage());
        }
        router.close();
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Thread daemon(Runnable runnable, String name) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }

    private static final class Channel {

        private final Runnable undeclare;
        private final Runnable release;
        private volatile boolean closed;
        private Thread thread;

        private Channel(Runnable undeclare, Runnable release) {
            this.undeclare = undeclare;
            this.release = release;
        }

        private void start(Runnable loop, String name) {
            thread = daemon(loop, name);
            thread.start();
        }

        private void close() {
            closed = true;
            try {
                undeclare.run();
            } catch (Exception e) {
                logger.debug("Undeclare failed: {}", e.getMessage());
            }
            try {
                thread.join(JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Freeing the native handle under a thread still inside recv() would be a use-after-free;
            // leak it instead.
            if (thread.isAlive()) {
                logger.warn("Receive thread {} did not stop; leaving its native handle open", thread.getName());
            } else {
                release.run();
            }
        }
    }
}
