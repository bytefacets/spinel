package com.bytefacets.diaspore.comms.send;

public interface OutputRegistryFactory {
    OutputRegistry createOutputRegistry(ConnectedSessionInfo sessionInfo);
}
