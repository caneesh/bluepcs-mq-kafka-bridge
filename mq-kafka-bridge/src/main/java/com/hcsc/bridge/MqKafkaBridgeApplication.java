package com.hcsc.bridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableJms
@EnableAsync
@EnableScheduling
public class MqKafkaBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqKafkaBridgeApplication.class, args);
    }
}
