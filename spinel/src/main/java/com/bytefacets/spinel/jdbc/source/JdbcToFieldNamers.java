// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

public final class JdbcToFieldNamers {
    private JdbcToFieldNamers() {}

    public static final JdbcToFieldNamer Same = jdbcName -> jdbcName;

    public static final JdbcToFieldNamer TitleCase =
            new JdbcToFieldNamer() {
                private final StringBuilder sb = new StringBuilder();

                @Override
                public String jdbcToFieldName(final String jdbcName) {
                    sb.setLength(0);
                    boolean capitalize = true;
                    for (int i = 0, len = jdbcName.length(); i < len; i++) {
                        final char ch = jdbcName.charAt(i);
                        if (Character.isLetter(ch) || Character.isDigit(ch)) {
                            if (capitalize) {
                                sb.append(Character.toUpperCase(ch));
                                capitalize = false;
                            } else {
                                sb.append(ch);
                            }
                        } else {
                            capitalize = true;
                        }
                    }
                    return sb.toString();
                }
            };
}
