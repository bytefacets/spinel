package com.bytefacets.diaspore.grpc.send;

import static com.bytefacets.diaspore.grpc.send.GrpcSession.createSession;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.diaspore.comms.send.ConnectedSessionInfo;
import com.bytefacets.diaspore.comms.send.OutputRegistryFactory;
import com.bytefacets.diaspore.comms.send.SubscriptionProvider;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.bytefacets.diaspore.grpc.send.auth.GrpcConnectedSessionInfo;
import io.grpc.Context;
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
 *
 * <p>The service will create a GrpcSession with the registry and the session info it can pull from
 * the gRPC context in the "connected-session" context key. If you're using your own auth, you can
 * put an instance of ConnectedSessionInfo in that context key, and it will be passed on to the
 * session.
 *
 * @see ConnectedSessionInfo
 */
public final class GrpcService extends DataServiceGrpc.DataServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(GrpcService.class);

    /**
     * The context key value is "connected-session" and expects an instance of ConnectedSessionInfo.
     */
    public static final Context.Key<ConnectedSessionInfo> CONNECTED_SESSION_KEY =
            Context.key("connected-session");

    private static final ConnectedSessionInfo EMPTY = GrpcConnectedSessionInfo.EMPTY;
    private final SubscriptionProvider subscriptionProvider;
    private final EventLoop dataEventLoop;

    GrpcService(final SubscriptionProvider subscriptionProvider, final EventLoop dataEventLoop) {
        this.subscriptionProvider = requireNonNull(subscriptionProvider, "subscriptionProvider");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
    }

    /**
     * Extracts the ConnectedSessionInfo from the "connected-session" Context.key, calls the
     * OutputRegistryFactory to get an OutputRegistry for the session, and instantiates a
     * GrpcSession.
     *
     * @see GrpcService#CONNECTED_SESSION_KEY
     * @see OutputRegistryFactory
     * @see ConnectedSessionInfo
     */
    @Override
    public StreamObserver<SubscriptionRequest> subscribe(
            final StreamObserver<SubscriptionResponse> responseObserver) {
        final ConnectedSessionInfo sessionInfo =
                requireNonNullElse(CONNECTED_SESSION_KEY.get(), EMPTY);
        final var sessionStream = (ServerCallStreamObserver<SubscriptionResponse>) responseObserver;
        log.info("New session established: {}", sessionInfo);
        return createSession(
                        sessionInfo,
                        subscriptionProvider,
                        sessionStream,
                        dataEventLoop,
                        this::sessionCompleted)
                .requestHandler();
    }

    private void sessionCompleted(final GrpcSession session) {
        session.close();
    }
}
