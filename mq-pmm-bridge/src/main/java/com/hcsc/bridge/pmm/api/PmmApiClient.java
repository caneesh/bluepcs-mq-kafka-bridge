package com.hcsc.bridge.pmm.api;

/** POSTs the rendered request and returns the raw response body. */
public interface PmmApiClient {

    PmmApiResponse submit(String requestXml, String eventId) throws PmmApiException;
}
