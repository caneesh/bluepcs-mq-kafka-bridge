package com.hcsc.bridge.pmm.local;

import com.hcsc.bridge.pmm.api.PmmApiClient;
import com.hcsc.bridge.pmm.api.PmmApiException;
import com.hcsc.bridge.pmm.api.PmmApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Local-profile stand-in for the PMM web service: returns a deterministic canned XML
 * response (same eventId → same bytes) so the local run exercises the windowed HDFS
 * write and its idempotency without any network.
 */
@Component
@Profile("local")
public class LocalPmmApiClient implements PmmApiClient {

    private static final Logger logger = LoggerFactory.getLogger(LocalPmmApiClient.class);

    @Override
    public PmmApiResponse submit(String requestXml, String eventId) throws PmmApiException {
        logger.info("[LOCAL] PMM API stub called for eventId {} ({} request chars)", eventId, requestXml.length());
        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<PmmResponse>\n"
                + "  <Status>OK</Status>\n"
                + "  <EventId>" + eventId + "</EventId>\n"
                + "  <RequestChars>" + requestXml.length() + "</RequestChars>\n"
                + "  <Source>local-stub</Source>\n"
                + "</PmmResponse>\n";
        return new PmmApiResponse(200, body, 0L);
    }
}
