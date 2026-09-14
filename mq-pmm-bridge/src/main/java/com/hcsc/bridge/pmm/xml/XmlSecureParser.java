package com.hcsc.bridge.pmm.xml;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * DOM parsing hardened against XXE and entity-expansion attacks. MQ payloads are
 * untrusted input: a DOCTYPE with an external entity could otherwise read files from
 * the edge node or stall the listener thread (billion laughs).
 */
public final class XmlSecureParser {

    private XmlSecureParser() {
    }

    public static DocumentBuilder newDocumentBuilder(boolean namespaceAware) throws ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        factory.setNamespaceAware(namespaceAware);
        return factory.newDocumentBuilder();
    }

    /** Parses {@code xml} with a fresh (non-thread-safe) hardened builder. */
    public static Document parse(String xml, boolean namespaceAware)
            throws ParserConfigurationException, SAXException, IOException {
        return newDocumentBuilder(namespaceAware).parse(new InputSource(new StringReader(xml)));
    }
}
