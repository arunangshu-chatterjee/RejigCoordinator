package edu.usc.cs550.rejig.coordinator;

import edu.usc.cs550.rejig.coordinator.config.Config;
import edu.usc.cs550.rejig.interfaces.RejigConfig;
import edu.usc.cs550.rejig.interfaces.RejigReaderGrpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

/**
 * A GRPC service to read the coordinator config.
 */
public class RejigCoordinatorReader extends RejigReaderGrpc.RejigReaderImplBase {

  private Config config;

  RejigCoordinatorReader(Config config) {
    this.config = config;
  }

  @Override
  public void getConfig(Empty req, StreamObserver<RejigConfig> responseObserver) {
    responseObserver.onNext(config.get());
    responseObserver.onCompleted();
  }
}