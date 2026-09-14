package com.hcsc.bridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * PMM (Product Message Model, canonical XML) bridge: MQ → XPath extract → XML request
 * template → web-service POST with STS token → raw XML response to a 4-hourly HDFS
 * folder → audit. Lives in {@code com.hcsc.bridge} so the default component scan
 * covers the shared bridge-core beans; everything PMM-specific is under
 * {@code com.hcsc.bridge.pmm}.
 */
@SpringBootApplication
@EnableJms
@EnableAsync
@EnableScheduling
public class PmmBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(PmmBridgeApplication.class, args);
    }
}
