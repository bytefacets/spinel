// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SuppressWarnings("FinalClass")
@SpringBootApplication
public class Application {
    private Application() {}

    public static void main(final String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
