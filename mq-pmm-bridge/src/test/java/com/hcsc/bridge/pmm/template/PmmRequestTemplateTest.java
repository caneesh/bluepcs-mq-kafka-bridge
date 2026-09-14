package com.hcsc.bridge.pmm.template;

import com.hcsc.bridge.pmm.xml.PmmExtractedValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.DefaultResourceLoader;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("PmmRequestTemplate")
class PmmRequestTemplateTest {

    private static PmmRequestTemplate template(String text) {
        return new PmmRequestTemplate(text, "inline", true);
    }

    @Nested
    @DisplayName("rendering")
    class Rendering {

        @Test
        @DisplayName("substitutes both placeholders")
        void substitutes() {
            String out = template("<r><a>${value1}</a><b>${value2}</b></r>")
                    .render(new PmmExtractedValues("one", "two"));

            assertThat(out).isEqualTo("<r><a>one</a><b>two</b></r>");
        }

        @Test
        @DisplayName("XML-escapes the five special characters")
        void escapes() {
            String out = template("<r a=\"${value2}\">${value1}</r>")
                    .render(new PmmExtractedValues("a&b<c>d", "say \"hi\" 'now'"));

            assertThat(out).isEqualTo("<r a=\"say &quot;hi&quot; &apos;now&apos;\">a&amp;b&lt;c&gt;d</r>");
        }

        @Test
        @DisplayName("treats $ and backslash in values literally")
        void literalDollarAndBackslash() {
            String out = template("<r>${value1}|${value2}</r>")
                    .render(new PmmExtractedValues("$1$2\\d", "${value1}"));

            assertThat(out).isEqualTo("<r>$1$2\\d|${value1}</r>");
        }

        @Test
        @DisplayName("substitutes repeated placeholders everywhere they appear")
        void repeated() {
            String out = template("${value1}-${value2}-${value1}-${value2}")
                    .render(new PmmExtractedValues("A", "B"));

            assertThat(out).isEqualTo("A-B-A-B");
        }
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        @DisplayName("requires both placeholders by default")
        void missingPlaceholder() {
            assertThatThrownBy(() -> template("<r>${value1}</r>"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("${value2}").hasMessageContaining("value2=false");
        }

        @Test
        @DisplayName("allows a partial template when require-all-placeholders is off")
        void partialAllowed() {
            String out = new PmmRequestTemplate("<r>${value1}</r>", "inline", false)
                    .render(new PmmExtractedValues("A", "B"));

            assertThat(out).isEqualTo("<r>A</r>");
        }

        @Test
        @DisplayName("rejects an unknown placeholder, naming it")
        void unknownPlaceholder() {
            assertThatThrownBy(() -> template("<r>${value1}${Value2}</r>"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("${Value2}");
        }

        @Test
        @DisplayName("rejects an unterminated placeholder")
        void unterminated() {
            assertThatThrownBy(() -> template("<r>${value1</r>"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("unterminated");
        }

        @Test
        @DisplayName("rejects a placeholder inside a CDATA section, a comment or a processing instruction")
        void rejectsUnsafePlacement() {
            assertThatThrownBy(() -> template("<r><![CDATA[${value1}]]>${value2}</r>"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("CDATA");
            assertThatThrownBy(() -> template("<r><!-- ${value1} -->${value2}</r>"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("comment");
            assertThatThrownBy(() -> template("<?pi ${value1}?><r>${value2}</r>"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("processing instruction");
        }

        @Test
        @DisplayName("accepts placeholders after a closed comment, CDATA section or the XML declaration")
        void acceptsPlacementAfterClosedRegions() {
            String out = template("<?xml version=\"1.0\"?><!-- c --><r><![CDATA[x]]>${value1}|${value2}</r>")
                    .render(new PmmExtractedValues("A", "B"));

            assertThat(out).isEqualTo("<?xml version=\"1.0\"?><!-- c --><r><![CDATA[x]]>A|B</r>");
        }

        @Test
        @DisplayName("rejects an empty template")
        void empty() {
            assertThatThrownBy(() -> template("  "))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("empty");
        }
    }

    @Nested
    @DisplayName("loading")
    class Loading {

        @Test
        @DisplayName("loads the classpath sample template")
        void classpath() {
            PmmRequestTemplate t = new PmmRequestTemplate(new DefaultResourceLoader(),
                    "classpath:pmm/request-template.sample.xml", true);

            String out = t.render(new PmmExtractedValues("P1", "D1"));
            assertThat(out).contains("<ProductIdentifier>P1</ProductIdentifier>")
                    .contains("<EffectiveDate>D1</EffectiveDate>")
                    .doesNotContain("${");
            assertThat(t.getLocation()).isEqualTo("classpath:pmm/request-template.sample.xml");
        }

        @Test
        @DisplayName("loads a file: template")
        void file(@TempDir Path dir) throws Exception {
            Path f = dir.resolve("t.xml");
            Files.write(f, "<r>${value1}${value2}</r>".getBytes(StandardCharsets.UTF_8));

            PmmRequestTemplate t = new PmmRequestTemplate(new DefaultResourceLoader(),
                    "file:" + f.toAbsolutePath(), true);

            assertThat(t.render(new PmmExtractedValues("a", "b"))).isEqualTo("<r>ab</r>");
        }

        @Test
        @DisplayName("fails fast when the template is missing or the location blank")
        void missing() {
            assertThatThrownBy(() -> new PmmRequestTemplate(new DefaultResourceLoader(), "file:/nope/none.xml", true))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not found");
            assertThatThrownBy(() -> new PmmRequestTemplate(new DefaultResourceLoader(), " ", true))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("bridge.pmm.template.location");
        }
    }
}
