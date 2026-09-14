package com.hcsc.bridge.pmm.template;

import com.hcsc.bridge.pmm.xml.PmmExtractedValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The (large) XML request body, loaded once at startup from
 * {@code bridge.pmm.template.location} and rendered per message by substituting the
 * two extracted values. Substitution is literal segment concatenation — never a regex
 * replace — so {@code $} or {@code \} inside a value cannot corrupt the request. Values
 * are XML-escaped before insertion.
 *
 * <p>Placeholder syntax is exactly {@code ${value1}} and {@code ${value2}} (case-sensitive,
 * no whitespace); each may appear any number of times. Any other {@code ${...}} token, an
 * unterminated {@code ${}, or a missing placeholder fails startup with a message naming
 * the token — the alternative is silently POSTing a literal "${Value1}" to the service.
 */
@Component
public class PmmRequestTemplate {

    private static final Logger logger = LoggerFactory.getLogger(PmmRequestTemplate.class);

    public static final String PLACEHOLDER_1 = "${value1}";
    public static final String PLACEHOLDER_2 = "${value2}";

    private enum Placeholder { VALUE1, VALUE2 }

    private final String location;
    /** Literal String segments interleaved with Placeholder markers, in document order. */
    private final List<Object> segments;
    private final int templateChars;

    @Autowired
    public PmmRequestTemplate(ResourceLoader resourceLoader,
                              @Value("${bridge.pmm.template.location}") String location,
                              @Value("${bridge.pmm.template.require-all-placeholders:true}")
                              boolean requireAllPlaceholders) {
        this(load(resourceLoader, location), location, requireAllPlaceholders);
    }

    /** Builds a template from literal text (tests, readiness probes). */
    public PmmRequestTemplate(String templateText, String describedLocation, boolean requireAllPlaceholders) {
        if (templateText == null || templateText.trim().isEmpty()) {
            throw new IllegalStateException("PMM request template is empty: " + describedLocation);
        }
        this.location = describedLocation;
        this.segments = Collections.unmodifiableList(split(templateText, describedLocation));
        this.templateChars = templateText.length();

        boolean has1 = segments.contains(Placeholder.VALUE1);
        boolean has2 = segments.contains(Placeholder.VALUE2);
        if (requireAllPlaceholders && !(has1 && has2)) {
            throw new IllegalStateException("PMM request template " + describedLocation
                    + " must contain both " + PLACEHOLDER_1 + " and " + PLACEHOLDER_2
                    + " (found value1=" + has1 + ", value2=" + has2
                    + "); set bridge.pmm.template.require-all-placeholders=false to allow a partial template");
        }
        logger.info("PMM request template loaded from {} ({} chars, value1 x{}, value2 x{})",
                describedLocation, templateChars,
                Collections.frequency(segments, Placeholder.VALUE1),
                Collections.frequency(segments, Placeholder.VALUE2));
    }

    public String render(PmmExtractedValues values) {
        String v1 = XmlEscaper.escape(values.getValue1());
        String v2 = XmlEscaper.escape(values.getValue2());
        StringBuilder out = new StringBuilder(templateChars + v1.length() + v2.length());
        for (Object segment : segments) {
            if (segment == Placeholder.VALUE1) {
                out.append(v1);
            } else if (segment == Placeholder.VALUE2) {
                out.append(v2);
            } else {
                out.append((String) segment);
            }
        }
        return out.toString();
    }

    public String getLocation() {
        return location;
    }

    private static String load(ResourceLoader resourceLoader, String location) {
        if (location == null || location.trim().isEmpty()) {
            throw new IllegalStateException("bridge.pmm.template.location is required");
        }
        Resource resource = resourceLoader.getResource(location.trim());
        if (!resource.exists()) {
            throw new IllegalStateException("PMM request template not found: " + location);
        }
        try (InputStream in = resource.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("PMM request template unreadable: " + location + " (" + e.getMessage() + ")", e);
        }
    }

    private static List<Object> split(String text, String location) {
        List<Object> out = new ArrayList<>();
        int pos = 0;
        while (true) {
            int start = text.indexOf("${", pos);
            if (start < 0) {
                if (pos < text.length()) {
                    out.add(text.substring(pos));
                }
                return out;
            }
            int end = text.indexOf('}', start);
            if (end < 0) {
                throw new IllegalStateException("PMM request template " + location
                        + " has an unterminated placeholder at offset " + start);
            }
            String token = text.substring(start, end + 1);
            Placeholder placeholder;
            if (PLACEHOLDER_1.equals(token)) {
                placeholder = Placeholder.VALUE1;
            } else if (PLACEHOLDER_2.equals(token)) {
                placeholder = Placeholder.VALUE2;
            } else {
                throw new IllegalStateException("PMM request template " + location
                        + " contains an unknown placeholder " + token + " (only "
                        + PLACEHOLDER_1 + " and " + PLACEHOLDER_2 + " are substituted)");
            }
            if (start > pos) {
                out.add(text.substring(pos, start));
            }
            out.add(placeholder);
            pos = end + 1;
        }
    }
}
