package com.bytefacets.diaspore.grpc.send;

import static com.bytefacets.diaspore.grpc.send.GrpcSession.createSession;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.send.OutputRegistry;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service which can be added to a gRPC Server to expose remote client subscriptions to registered
 * outputs.
 *
 * <p>
 *
 * <pre>
 * RegisteredOutputs registry = new RegisteredOutputs();
 * // ... register outputs in the registry
 * EventLoop dataEventLoop = new DefaultEventLoop();
 * GrpcService service = GrpcServiceBuilder.grpcService(registry, dataEventLoop).build();
 * Server server = ServerBuilder.forPort(15000).addService(service).executor(eventLoop).build();
 * server.start();
 * </pre>
 */
public final class GrpcService extends DataServiceGrpc.DataServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcSession.class);
    private final OutputRegistry registry;
    private final EventLoop dataEventLoop;

    GrpcService(final OutputRegistry registry, final EventLoop dataEventLoop) {
        this.registry = requireNonNull(registry, "registry");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
    }

    public OutputRegistry registry() {
        return registry;
    }

    @Override
    public StreamObserver<SubscriptionRequest> subscribe(
            final StreamObserver<SubscriptionResponse> responseObserver) {
        final var sessionStream = (ServerCallStreamObserver<SubscriptionResponse>) responseObserver;
        log.info("New session established");
        return createSession(registry, sessionStream, dataEventLoop, this::sessionCompleted)
                .requestHandler();
    }

    private void sessionCompleted(final GrpcSession session) {
        session.close();
    }
}
