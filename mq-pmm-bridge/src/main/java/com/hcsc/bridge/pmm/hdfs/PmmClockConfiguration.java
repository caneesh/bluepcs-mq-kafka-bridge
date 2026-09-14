package com.hcsc.bridge.pmm.hdfs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** Injectable clock so window arithmetic is testable with {@link Clock#fixed}. */
@Configuration
public class PmmClockConfiguration {

    @Bean
    public Clock pmmClock() {
        return Clock.systemUTC();
    }
}
