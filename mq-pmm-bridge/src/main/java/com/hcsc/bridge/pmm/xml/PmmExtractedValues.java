package com.hcsc.bridge.pmm.xml;

import java.util.Objects;

/**
 * The two values pulled from a PMM message for the request template. They are
 * business identifiers that may be sensitive: never put them in logs or audit events.
 */
public final class PmmExtractedValues {

    private final String value1;
    private final String value2;

    public PmmExtractedValues(String value1, String value2) {
        this.value1 = Objects.requireNonNull(value1, "value1 must not be null");
        this.value2 = Objects.requireNonNull(value2, "value2 must not be null");
    }

    public String getValue1() {
        return value1;
    }

    public String getValue2() {
        return value2;
    }

    @Override
    public String toString() {
        // Deliberately opaque: values may be PHI/PII
        return "PmmExtractedValues{value1Chars=" + value1.length() + ", value2Chars=" + value2.length() + '}';
    }
}
