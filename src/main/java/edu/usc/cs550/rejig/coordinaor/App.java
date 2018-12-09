package edu.usc.cs550.rejig.coordinator;

import java.io.IOException;

public class App {
  public static void main(String[] args) throws IOException, InterruptedException, Exception {
    if (args[0].equals("in-memory")) {
      int readerPort;
      int writerPort;
      try {
        readerPort = Integer.parseInt(args[1]);
        writerPort = Integer.parseInt(args[2]);
        System.out.println(readerPort);
        System.out.println(writerPort);
      } catch (Exception e) {
        System.out.println("Please pass parameters readerPort, and writerPort");
        throw e;
      }
      inMemoryMain(readerPort, writerPort);
    } else if (args[0].equals("zookeeper-reader")
        || args[0].equals("zookeeper-writer")) {
      boolean isWriter = args[0].equals("zookeeper-writer");
      String zookeeperAddress;
      int port;
      try {
        zookeeperAddress = args[1];
        port = Integer.parseInt(args[2]);
      } catch (Exception e) {
        System.out.println("Please pass parameters port, and zookeeperAddress");
        throw e;
      }
      zooKeeperMain(isWriter, zookeeperAddress, port);
    } else {
      System.out.println("The coordinator value (first argument) can be in-memory, zookeeper-reader, or zookeeper-writer");
    }
  }

  public static void zooKeeperMain(boolean isWriter, String zookeeperAddress, int port) throws IOException, InterruptedException, Exception {
    final ZooKeeperCoordinator coordinator =
      new ZooKeeperCoordinator(isWriter, zookeeperAddress, port);
    coordinator.start();
    coordinator.blockUntilShutdown();
  }

  public static void inMemoryMain(int readerPort, int writerPort) throws IOException, InterruptedException {
    final InMemoryCoordinator coordinator = new InMemoryCoordinator(readerPort, writerPort);
    coordinator.start();
    coordinator.blockUntilShutdown();
  }
}