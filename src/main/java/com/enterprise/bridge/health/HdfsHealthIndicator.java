package com.enterprise.bridge.health;

import com.enterprise.bridge.hdfs.HdfsFileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!local")
public class HdfsHealthIndicator implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(HdfsHealthIndicator.class);

    private final HdfsFileOperations hdfsFileOperations;
    private final String basePath;

    public HdfsHealthIndicator(
            HdfsFileOperations hdfsFileOperations,
            @Value("${bridge.hdfs.base-path}") String basePath) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.basePath = basePath;
    }

    @Override
    public Health health() {
        try {
            boolean exists = hdfsFileOperations.exists(basePath);
            if (exists) {
                return Health.up()
                        .withDetail("basePath", basePath)
                        .withDetail("accessible", true)
                        .build();
            } else {
                return Health.down()
                        .withDetail("basePath", basePath)
                        .withDetail("error", "Base path does not exist")
                        .build();
            }
        } catch (Exception e) {
            logger.warn("HDFS health check failed", e);
            return Health.down()
                    .withDetail("basePath", basePath)
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}
