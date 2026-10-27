package com.workoss.boot.zenoh.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Pure Spring Boot demo / integration harness for {@code spring-boot-zenoh-cluster}.
 * <p>
 * Starts a single cluster node. Run it three times with the {@code instance1}, {@code instance2}
 * and {@code instance3} profiles (different ports and {@code zenoh.cluster.node-id}) to get a
 * three-node mesh on one machine, then exercise the endpoints exposed by
 * {@link ClusterDemoController}. No Vert.x is involved.
 */
@SpringBootApplication
public class ZenohClusterExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ZenohClusterExampleApplication.class, args);
    }
}
