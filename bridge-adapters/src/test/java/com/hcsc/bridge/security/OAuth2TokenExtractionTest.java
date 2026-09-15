package com.hcsc.bridge.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OAuth2JwtTokenProvider.tokenFrom")
class OAuth2TokenExtractionTest {

    @Test
    @DisplayName("accepts the STS field and the standard OAuth2 fallbacks, stripping a Bearer prefix")
    void acceptsKnownFields() {
        assertThat(OAuth2JwtTokenProvider.tokenFrom("{\"jwt_token\":\"a.b.c\"}")).isEqualTo("a.b.c");
        assertThat(OAuth2JwtTokenProvider.tokenFrom("{\"access_token\":\"Bearer x.y.z\"}")).isEqualTo("x.y.z");
        assertThat(OAuth2JwtTokenProvider.tokenFrom("{\"other\":1,\"id_token\":\"t\"}")).isEqualTo("t");
    }

    @Test
    @DisplayName("returns null for an empty object, an empty token, a non-JSON body or an empty body")
    void rejectsTokenlessBodies() {
        assertThat(OAuth2JwtTokenProvider.tokenFrom("{}")).isNull();
        assertThat(OAuth2JwtTokenProvider.tokenFrom("{\"jwt_token\":\"\"}")).isNull();
        assertThat(OAuth2JwtTokenProvider.tokenFrom("{\"jwt_token\":42}")).isNull();
        assertThat(OAuth2JwtTokenProvider.tokenFrom("<html/>")).isNull();
        assertThat(OAuth2JwtTokenProvider.tokenFrom("")).isNull();
        assertThat(OAuth2JwtTokenProvider.tokenFrom(null)).isNull();
    }
}
