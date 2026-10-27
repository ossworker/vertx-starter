package com.workoss.boot.zenoh.cluster.autoconfigure;

import com.workoss.boot.zenoh.cluster.ClusterMessaging;
import com.workoss.boot.zenoh.cluster.ZenohCluster;
import com.workoss.boot.zenoh.cluster.internal.DefaultZenohCluster;
import com.workoss.boot.zenoh.cluster.internal.membership.ZenohMembershipProcessor;
import com.workoss.boot.zenoh.cluster.internal.messaging.ZenohSubscribeProcessor;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@AutoConfiguration
@ConditionalOnProperty(prefix = "zenoh.cluster", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(ZenohClusterProperties.class)
public class ZenohClusterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ZenohCluster.class)
    public DefaultZenohCluster zenohCluster(ZenohClusterProperties properties, Environment environment) {
        return new DefaultZenohCluster(properties.toSettings(environment));
    }

    @Bean
    @ConditionalOnMissingBean(ClusterMessaging.class)
    public ClusterMessaging zenohClusterMessaging(ZenohCluster cluster) {
        return cluster.messaging();
    }

    /**
     * Static so it registers as a {@code BeanPostProcessor} without eagerly initializing this
     * configuration's other beans.
     */
    @Bean
    static ZenohSubscribeProcessor zenohSubscribeProcessor() {
        return new ZenohSubscribeProcessor();
    }

    /**
     * Static for the same reason as {@link #zenohSubscribeProcessor()}.
     */
    @Bean
    static ZenohMembershipProcessor zenohMembershipProcessor() {
        return new ZenohMembershipProcessor();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(HealthIndicator.class)
    static class HealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "zenohClusterHealthIndicator")
        public ZenohClusterHealthIndicator zenohClusterHealthIndicator(ZenohCluster cluster) {
            return new ZenohClusterHealthIndicator(cluster);
        }
    }
}
