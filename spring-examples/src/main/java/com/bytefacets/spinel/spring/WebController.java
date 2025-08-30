// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.spring;

import java.nio.charset.StandardCharsets;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class WebController {
    private static final String INDEX_HTML = toString("public/index.html");
    private static final String PROTO = toString("/com/bytefacets/spinel/grpc/data-service.proto");

    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    public Mono<String> index() {
        return Mono.just(INDEX_HTML);
    }

    @GetMapping(value = "/data-service.proto", produces = MediaType.TEXT_PLAIN_VALUE)
    public Mono<String> spinelProtobuf() {
        return Mono.just(PROTO);
    }

    private static String toString(final String resourcePath) {
        try {
            final ClassPathResource resource = new ClassPathResource(resourcePath);
            return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load " + resourcePath, ex);
        }
    }
}
