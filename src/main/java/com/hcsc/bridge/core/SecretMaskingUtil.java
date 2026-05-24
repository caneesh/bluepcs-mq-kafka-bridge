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

    public static String maskSecrets(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        String result = text;
        result = maskPattern(result, "(?i)(password\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(secret\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(token\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(credential\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(apikey\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);
        result = maskPattern(result, "(?i)(api_key\\s*[=:]\\s*)[^\\s,;\"']+", "$1" + MASK);

        return result;
    }

    private static String maskPattern(String text, String regex, String replacement) {
        return text.replaceAll(regex, replacement);
    }
}
