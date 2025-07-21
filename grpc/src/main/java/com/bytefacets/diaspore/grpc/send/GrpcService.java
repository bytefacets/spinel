package com.bytefacets.diaspore.grpc.send;

import static com.bytefacets.diaspore.grpc.send.GrpcSession.createSession;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.send.OutputRegistry;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcService extends DataServiceGrpc.DataServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcSession.class);
    private final OutputRegistry registry;
    private final Executor dataExecutor;

    GrpcService(final OutputRegistry registry, final Executor dataExecutor) {
        this.registry = requireNonNull(registry, "registry");
        this.dataExecutor = requireNonNull(dataExecutor, "dataExecutor");
    }

    public OutputRegistry registry() {
        return registry;
    }

    @Override
    public StreamObserver<SubscriptionRequest> subscribe(
            final StreamObserver<SubscriptionResponse> responseObserver) {
        final var sessionStream = (ServerCallStreamObserver<SubscriptionResponse>) responseObserver;
        log.info("New session established");
        return createSession(registry, sessionStream, dataExecutor, this::sessionCompleted)
                .requestHandler();
    }

    private void sessionCompleted(final GrpcSession session) {
        session.close();
    }
}
