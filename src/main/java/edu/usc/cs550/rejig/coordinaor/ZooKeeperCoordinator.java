package edu.usc.cs550.rejig.coordinator;

import edu.usc.cs550.rejig.coordinator.config.ZooKeeperConfig;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;

/**
 * Implementation of a coordinator using the ZooKeeperConfig.
 * The reader, and writer servers need to instantiated
 * seperately.
 */
public class ZooKeeperCoordinator {
  private ZooKeeperConfig config;

  private CuratorFramework client;

  private Server server;

  private boolean isWriter;

  private int port;

  /** The server. */
  public Server server() {
    return server;
  }

  /** Start a server at the specified ports. */
  public ZooKeeperCoordinator(boolean isWriter, String zookeeperAddress, int port) {
    this.isWriter = isWriter;
    this.port = port;
    ExponentialBackoffRetry policy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory
      .newClient(zookeeperAddress, policy);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("Shutting down servers since JVM is shutting down.");
        ZooKeeperCoordinator.this.stop();
        System.err.println("Servers shut down.");
      }
    });
  }

  /** Start both servers. */
  public void start() throws IOException, Exception {
    client.start();
    config = new ZooKeeperConfig(client);
    if (isWriter) {
      server = ServerBuilder.forPort(port)
        .addService(new RejigCoordinatorWriter(config))
        .build();
    } else {
      server = ServerBuilder.forPort(port)
        .addService(new RejigCoordinatorReader(config))
        .build();
    }
    server.start();
  }

  /** Stop both servers. */
  public void stop() {
    if (server != null) {
      server.shutdown();
    }
    if (client != null) {
      client.close();
    }
  }

  /** Wait for shutdown of both servers. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}