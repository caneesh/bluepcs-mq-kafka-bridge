package com.hcsc.bridge.pmm.xml;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("PmmXmlExtractor")
class PmmXmlExtractorTest {

    private static final String XML = "<?xml version=\"1.0\"?>"
            + "<PmmMessage><Product><ProductIdentifier> PRD-001 </ProductIdentifier>"
            + "<EffectiveDate>2026-09-13</EffectiveDate><Empty></Empty></Product></PmmMessage>";

    private PmmXmlExtractor extractor(String x1, String x2) {
        return new PmmXmlExtractor(x1, x2, false, false);
    }

    @Nested
    @DisplayName("extraction")
    class Extraction {

        @Test
        @DisplayName("returns both values, trimmed")
        void extractsBothValues() {
            PmmExtractedValues values = extractor("/PmmMessage/Product/ProductIdentifier",
                    "/PmmMessage/Product/EffectiveDate").extract(XML, "MSG-1");

            assertThat(values.getValue1()).isEqualTo("PRD-001");
            assertThat(values.getValue2()).isEqualTo("2026-09-13");
        }

        @Test
        @DisplayName("handles namespaced documents through local-name() when namespace-unaware")
        void namespacedDocument() {
            String xml = "<p:PmmMessage xmlns:p=\"urn:pmm\"><p:Product><p:Id>A</p:Id><p:Date>B</p:Date></p:Product></p:PmmMessage>";

            PmmExtractedValues values = extractor("//*[local-name()='Id']", "//*[local-name()='Date']")
                    .extract(xml, "MSG-1");

            assertThat(values.getValue1()).isEqualTo("A");
            assertThat(values.getValue2()).isEqualTo("B");
        }

        @Test
        @DisplayName("supports string-valued expressions such as concat()")
        void stringExpression() {
            PmmExtractedValues values = extractor("concat('X-', /PmmMessage/Product/ProductIdentifier)",
                    "/PmmMessage/Product/EffectiveDate").extract(XML, "MSG-1");

            assertThat(values.getValue1()).isEqualTo("X- PRD-001");
        }

        @Test
        @DisplayName("fails when an expression matches nothing")
        void missingNode() {
            assertThatThrownBy(() -> extractor("/PmmMessage/Nope", "/PmmMessage/Product/EffectiveDate")
                    .extract(XML, "MSG-1"))
                    .isInstanceOf(PmmXmlException.class)
                    .hasMessageContaining("value1").hasMessageContaining("matched nothing");
        }

        @Test
        @DisplayName("fails on a blank value unless allow-empty is set")
        void blankValue() {
            assertThatThrownBy(() -> extractor("/PmmMessage/Product/Empty", "/PmmMessage/Product/EffectiveDate")
                    .extract(XML, "MSG-1"))
                    .isInstanceOf(PmmXmlException.class)
                    .hasMessageContaining("blank");

            PmmExtractedValues values = new PmmXmlExtractor("/PmmMessage/Product/Empty",
                    "/PmmMessage/Product/EffectiveDate", false, true).extract(XML, "MSG-1");
            assertThat(values.getValue1()).isEmpty();
        }

        @Test
        @DisplayName("fails on malformed XML and on an empty body")
        void malformed() {
            PmmXmlExtractor ex = extractor("/a", "/b");
            assertThatThrownBy(() -> ex.extract("<PmmMessage><Product>", "MSG-1"))
                    .isInstanceOf(PmmXmlException.class).hasMessageContaining("not well-formed");
            assertThatThrownBy(() -> ex.extract("   ", "MSG-1"))
                    .isInstanceOf(PmmXmlException.class).hasMessageContaining("Empty");
            assertThatThrownBy(() -> ex.extract(null, "MSG-1"))
                    .isInstanceOf(PmmXmlException.class);
        }

        @Test
        @DisplayName("rejects a DOCTYPE so external entities can never be resolved")
        void rejectsDoctype() {
            String xxe = "<?xml version=\"1.0\"?><!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>"
                    + "<PmmMessage><Product><ProductIdentifier>&e;</ProductIdentifier>"
                    + "<EffectiveDate>d</EffectiveDate></Product></PmmMessage>";

            assertThatThrownBy(() -> extractor("/PmmMessage/Product/ProductIdentifier",
                    "/PmmMessage/Product/EffectiveDate").extract(xxe, "MSG-1"))
                    .isInstanceOf(PmmXmlException.class)
                    .hasMessageContaining("not well-formed");
        }
    }

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("fails fast on an invalid XPath")
        void invalidXpath() {
            assertThatThrownBy(() -> extractor("/a[", "/b"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("bridge.pmm.xpath.value1");
        }

        @Test
        @DisplayName("fails fast on a blank XPath")
        void blankXpath() {
            assertThatThrownBy(() -> extractor("/a", " "))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("bridge.pmm.xpath.value2");
        }
    }
}
