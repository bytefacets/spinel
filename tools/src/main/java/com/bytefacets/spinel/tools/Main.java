// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools;

import com.bytefacets.spinel.tools.console.ConsoleCommand;
import picocli.CommandLine;

@CommandLine.Command(
        name = "",
        subcommands = {ConsoleCommand.class})
public final class Main {
    private Main() {}

    public static void main(final String[] args) {
        new CommandLine(new Main()).execute(args);
    }
}
