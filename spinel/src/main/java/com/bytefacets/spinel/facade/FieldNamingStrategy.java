// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

public interface FieldNamingStrategy {
    String formulateName(String nameFromMethod);

    FieldNamingStrategy Identity = name -> name;

    FieldNamingStrategy SnakeCase =
            nameFromMethod -> {
                final StringBuilder sb = new StringBuilder(nameFromMethod.length() + 4);
                int elideCount = 0;
                for (int i = 0, len = nameFromMethod.length(); i < len; i++) {
                    final char c = nameFromMethod.charAt(i);
                    if (Character.isUpperCase(c) || Character.isDigit(c)) {
                        if (i != 0 && elideCount == 0) {
                            sb.append('_');
                        }
                        sb.append(Character.toLowerCase(c));
                        elideCount++;
                    } else {
                        // we've combined several uppercase
                        // jump back and separate the last one
                        if (elideCount > 1) {
                            elideCount = 0;
                            i -= 2;
                            sb.setLength(sb.length() - 1);
                        } else {
                            sb.append(c);
                            elideCount = 0;
                        }
                    }
                }
                return sb.toString();
            };
}
