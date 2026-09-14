package com.hcsc.bridge.pmm.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Extracts the two template values from a PMM XML message with two configured XPath
 * expressions. Both expressions are compiled at construction, so a typo fails startup
 * rather than quarantining every message.
 *
 * <p>{@code extract} is synchronized: compiled {@link XPathExpression}s are not
 * thread-safe. The listener runs with concurrency 1, so this costs nothing.
 */
@Component
public class PmmXmlExtractor {

    private static final Logger logger = LoggerFactory.getLogger(PmmXmlExtractor.class);

    private final String xpath1Text;
    private final String xpath2Text;
    private final XPathExpression xpath1;
    private final XPathExpression xpath2;
    private final boolean namespaceAware;
    private final boolean allowEmpty;

    public PmmXmlExtractor(
            @Value("${bridge.pmm.xpath.value1}") String xpath1,
            @Value("${bridge.pmm.xpath.value2}") String xpath2,
            @Value("${bridge.pmm.xpath.namespace-aware:false}") boolean namespaceAware,
            @Value("${bridge.pmm.xpath.allow-empty:false}") boolean allowEmpty) {
        this.xpath1Text = requireText(xpath1, "bridge.pmm.xpath.value1");
        this.xpath2Text = requireText(xpath2, "bridge.pmm.xpath.value2");
        XPath xpath = XPathFactory.newInstance().newXPath();
        this.xpath1 = compile(xpath, this.xpath1Text, "bridge.pmm.xpath.value1");
        this.xpath2 = compile(xpath, this.xpath2Text, "bridge.pmm.xpath.value2");
        this.namespaceAware = namespaceAware;
        this.allowEmpty = allowEmpty;
        logger.info("PMM XPath extractor: value1='{}', value2='{}', namespaceAware={}, allowEmpty={}",
                this.xpath1Text, this.xpath2Text, namespaceAware, allowEmpty);
    }

    public synchronized PmmExtractedValues extract(String xml, String messageId) {
        if (xml == null || xml.trim().isEmpty()) {
            throw new PmmXmlException("Empty message body", messageId);
        }
        Document document;
        try {
            document = XmlSecureParser.parse(xml, namespaceAware);
        } catch (Exception e) {
            // SAXException (malformed / DOCTYPE rejected), IOException, ParserConfigurationException
            throw new PmmXmlException("Message is not well-formed XML: " + e.getMessage(), messageId, e);
        }
        String value1 = evaluate(xpath1, xpath1Text, document, messageId, "value1");
        String value2 = evaluate(xpath2, xpath2Text, document, messageId, "value2");
        return new PmmExtractedValues(value1, value2);
    }

    public String getXpath1() {
        return xpath1Text;
    }

    public String getXpath2() {
        return xpath2Text;
    }

    private String evaluate(XPathExpression expression, String expressionText, Document document,
                            String messageId, String label) {
        String raw;
        try {
            // NODESET first so "no match" and "ambiguous match" are both distinguishable from
            // "matched an empty element"; expressions that yield a string (concat(),
            // string()) fall back to STRING below.
            NodeList nodes = (NodeList) expression.evaluate(document, XPathConstants.NODESET);
            if (nodes == null || nodes.getLength() == 0) {
                throw new PmmXmlException("XPath for " + label + " matched nothing: " + expressionText, messageId);
            }
            if (nodes.getLength() > 1) {
                // Silently taking the first node would POST the wrong identifier with no
                // trail; the operator must disambiguate, e.g. (//Element)[1]
                throw new PmmXmlException("XPath for " + label + " matched " + nodes.getLength()
                        + " nodes, expected exactly one: " + expressionText, messageId);
            }
            raw = nodes.item(0).getTextContent();
        } catch (XPathExpressionException nodeFailure) {
            try {
                raw = (String) expression.evaluate(document, XPathConstants.STRING);
            } catch (XPathExpressionException e) {
                throw new PmmXmlException("XPath for " + label + " failed: " + e.getMessage(), messageId, e);
            }
            if (raw == null || raw.isEmpty()) {
                throw new PmmXmlException("XPath for " + label + " matched nothing: " + expressionText, messageId);
            }
        }
        String value = raw == null ? "" : raw.trim();
        if (value.isEmpty() && !allowEmpty) {
            throw new PmmXmlException("XPath for " + label + " matched a blank value: " + expressionText, messageId);
        }
        return value;
    }

    private static String requireText(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(key + " is required");
        }
        return value.trim();
    }

    private static XPathExpression compile(XPath xpath, String expression, String key) {
        try {
            return xpath.compile(expression);
        } catch (XPathExpressionException e) {
            throw new IllegalStateException(key + " is not a valid XPath expression: " + expression
                    + " (" + e.getMessage() + ")", e);
        }
    }
}
