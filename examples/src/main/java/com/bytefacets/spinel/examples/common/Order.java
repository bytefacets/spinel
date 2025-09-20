// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.common;

/** A simplified model of an Order which will get inspected at turned into a table structure. */
public interface Order {
    // formatting:off
    int getOrderId(); // getter only bc it's the key field
    int getQty();           Order setQty(int value);
    double getPrice();      Order setPrice(double value);
    int getInstrumentId();  Order setInstrumentId(int value);
    // formatting:on
}
