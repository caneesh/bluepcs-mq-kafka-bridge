package com.hcsc.bridge.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SecretMaskingUtil {

    private static final String MASK = "********";
    private static final Pattern SECRET_FIELD_PATTERN = Pattern.compile(
            "(?i)(password|secret|token|credential|key|apikey|api_key)"
    );

    private SecretMaskingUtil() {
    }

    public static String mask(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        return MASK;
    }

    public static String maskIfSecret(String fieldName, String value) {
        if (isSecretField(fieldName)) {
            return mask(value);
        }
        return value;
    }

    public static boolean isSecretField(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return false;
        }
        Matcher matcher = SECRET_FIELD_PATTERN.matcher(fieldName);
        return matcher.find();
    }

    // The payloads this bridge handles are JSON, where the field name is quoted:
    // "password":"x". The prose patterns below can never match that form (the
    // closing quote sits between the name and the colon), so JSON needs its own
    // rule. Field-name vocabulary must stay in sync with SECRET_FIELD_PATTERN.
    private static final String JSON_SECRET_KEYS =
            "(?:password|passwd|secret|token|credential|key|apikey|api_key)";

    public static String maskSecrets(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        String result = text;
        // JSON: "anyNameContainingSecretWord" : "value"  (also unquoted values)
        result = maskPattern(result,
                "(?i)(\"[^\"]*" + JSON_SECRET_KEYS + "[^\"]*\"\\s*:\\s*\")[^\"]*(\")",
                "$1" + MASK + "$2");
        result = maskPattern(result,
                "(?i)(\"[^\"]*" + JSON_SECRET_KEYS + "[^\"]*\"\\s*:\\s*)(?!\")[^\\s,}\\]]+",
                "$1" + MASK);
        // Prose / properties formats: name=value or name: value (before the generic
        // bearer rule so "Bearer token=x" is handled as key=value, not as a bearer token)
        result = maskPattern(result, "(?i)(password\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(secret\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(token\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(credential\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(apikey\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(api_key\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        // HTTP Authorization headers (Bearer/Basic); the bare-bearer rule skips
        // key=value forms already masked above
        result = maskPattern(result, "(?i)(authorization\\s*[=:]\\s*(?:bearer|basic)\\s+)\\S+", "$1" + MASK);
        result = maskPattern(result, "(?i)\\b(bearer\\s+)(?!\\S*=\\S)[A-Za-z0-9._~+/-]+", "$1" + MASK);

        return result;
    }

    private static String maskPattern(String text, String regex, String replacement) {
        return text.replaceAll(regex, replacement);
    }
}
