package com.hcsc.bridge.pmm.local;

import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.pmm.orchestrator.PmmOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * Local profile only: with {@code bridge.pmm.local.sample-message=<path>} set, pushes that
 * XML file through the orchestrator once at startup so a developer can see the whole
 * pipeline (extract → template → stub API → windowed HDFS write → audit log) without MQ.
 */
@Component
@Profile("local")
public class PmmLocalSampleRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(PmmLocalSampleRunner.class);

    private final PmmOrchestrator orchestrator;
    private final String samplePath;

    public PmmLocalSampleRunner(PmmOrchestrator orchestrator,
                                @Value("${bridge.pmm.local.sample-message:}") String samplePath) {
        this.orchestrator = orchestrator;
        this.samplePath = samplePath;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (samplePath == null || samplePath.trim().isEmpty()) {
            return;
        }
        Path path = Paths.get(samplePath.trim());
        String xml = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        Instant now = Instant.now();
        MqMessage message = new MqMessage("LOCAL-SAMPLE-" + path.getFileName(), null, xml, now, "LOCAL.SAMPLE", now);
        logger.info("[LOCAL] Pushing sample message {} through the PMM pipeline", path.toAbsolutePath());
        ProcessingResult result = orchestrator.process(message);
        logger.info("[LOCAL] Sample result: status={} eventId={} hdfsPath={} errorCode={} errorMessage={}",
                result.getStatus(), result.getEventId(), result.getHdfsPath(),
                result.getErrorCode(), result.getErrorMessage());
    }
}
