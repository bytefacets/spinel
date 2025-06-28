// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
plugins {
    id("com.tddworks.central-portal-publisher")
}

sonatypePortalPublisher {
    settings {
        autoPublish = false
        aggregation = true
    }
}
