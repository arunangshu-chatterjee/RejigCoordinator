package edu.usc.cs550.rejig.coordinator;

import edu.usc.cs550.rejig.coordinator.config.InMemoryConfig;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

/**
 * Implementation of a coordinator using the InMemoryConfig.
 * Since the InMemoryConfig exists on only one process, both
 * the reader, and writers have to be on the same process too.
 */
public class InMemoryCoordinator {
  private InMemoryConfig config;

  private Server reader;

  private Server writer;

  /** The reader server. */
  public Server reader() {
    return reader;
  }

  /** The writer server. */
  public Server writer() {
    return writer;
  }

  /** Start a reader, and writer server at the specified ports. */
  public InMemoryCoordinator(int readerPort, int writerPort) {
    config = new InMemoryConfig();
    reader = ServerBuilder.forPort(readerPort)
      .addService(new RejigCoordinatorReader(config))
      .build();

    writer = ServerBuilder.forPort(writerPort)
      .addService(new RejigCoordinatorWriter(config))
      .build();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("Shutting down servers since JVM is shutting down.");
        InMemoryCoordinator.this.stop();
        System.err.println("Servers shut down.");
      }
    });
  }

  /** Start both servers. */
  public void start() throws IOException {
    reader.start();
    writer.start();
  }

  /** Stop both servers. */
  public void stop() {
    if (reader != null) {
      reader.shutdown();
    }
    if (writer != null) {
      writer.shutdown();
    }
  }

  /** Wait for shutdown of both servers. */
  public void blockUntilShutdowna() throws InterruptedException {
    if (reader != null) {
      reader.awaitTermination();
    }
    if (writer != null) {
      writer.awaitTermination();
    }
  }
}