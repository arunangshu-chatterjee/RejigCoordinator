package edu.usc.cs550.rejig.coordinator;

import edu.usc.cs550.rejig.coordinator.config.Config;
import edu.usc.cs550.rejig.coordinator.config.InMemoryConfig;
import edu.usc.cs550.rejig.interfaces.FragmentList;
import edu.usc.cs550.rejig.interfaces.RejigConfig;
import edu.usc.cs550.rejig.interfaces.RejigWriterGrpc;

import static org.junit.Assert.assertEquals;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

@RunWith(JUnit4.class)
public class RejigCoordinatorWriterTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /** Test the constructor. */
  @Test
  public void rejigCoordinatorWriter_init() {
    InMemoryConfig config = new InMemoryConfig();
    RejigCoordinatorWriter writer = new RejigCoordinatorWriter(config);
  }

  /**
   * If non empty config is passed during initialization, then throw
   * an exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void rejigCoordinatorWriter_initWrong() {
    InMemoryConfig config = new InMemoryConfig();
    config.beginUpdate()
      .addFragment("server1:port1")
      .endUpdate();
    RejigCoordinatorWriter writer = new RejigCoordinatorWriter(config);
  }

  /**
   * Test if setting the config for the first time updates the
   * memcached clients properly.
   */
  @Test
  public void rejigCoordinatorWriter_setConfigFirstTime() throws Exception {
    RejigWriterGrpc.RejigWriterBlockingStub stub = createStub();
    FragmentList assignments = FragmentList.newBuilder()
      .addAddress("server1:port1").build();
    RejigConfig reply = stub.setConfig(assignments);

    // Ensure reply is correct.
    assertEquals(reply.getId(), 1);
    assertEquals(reply.getFragmentCount(), 1);
    assertEquals(reply.getFragment(0).getAddress(), "server1:port1");
  }

  /**
   * Test addition of new CMIs.
   */
  @Test
  public void rejigCoordinatorWriter_setConfigAdd() throws Exception {
    RejigWriterGrpc.RejigWriterBlockingStub stub = createStub();
    FragmentList assignments = FragmentList.newBuilder()
      .addAddress("server1:port1").build();
    RejigConfig reply = stub.setConfig(assignments);

    assertEquals(reply.getId(), 1);
    assertEquals(reply.getFragmentCount(), 1);
    assertEquals(reply.getFragment(0).getAddress(), "server1:port1");

    assignments = FragmentList.newBuilder()
      .addAddress("server1:port1")
      .addAddress("server2:port1").build();
    reply = stub.setConfig(assignments);

    // Ensure reply is correct.
    assertEquals(reply.getId(), 2);
    assertEquals(reply.getFragmentCount(), 2);
    assertEquals(reply.getFragment(0).getAddress(), "server1:port1");
    assertEquals(reply.getFragment(1).getAddress(), "server2:port1");
  }

  /**
   * Test deletion of a fragment.
   */
  @Test
  public void rejigCoordinatorWriter_setConfigDelete() throws Exception {
    RejigWriterGrpc.RejigWriterBlockingStub stub = createStub();
    FragmentList assignments = FragmentList.newBuilder()
      .addAddress("server1:port1")
      .addAddress("server2:port1").build();
    RejigConfig reply = stub.setConfig(assignments);

    assertEquals(reply.getId(), 1);
    assertEquals(reply.getFragmentCount(), 2);
    assertEquals(reply.getFragment(0).getAddress(), "server1:port1");
    assertEquals(reply.getFragment(1).getAddress(), "server2:port1");

    assignments = FragmentList.newBuilder()
      .addAddress("server1:port1").build();
    reply = stub.setConfig(assignments);

    // Ensure reply is correct.
    assertEquals(reply.getId(), 2);
    assertEquals(reply.getFragmentCount(), 1);
    assertEquals(reply.getFragment(0).getAddress(), "server1:port1");
  }

  /**
   * Test modification of fragment.
   */
  @Test
  public void rejigCoordinatorWriter_setConfigModify() throws Exception {
    RejigWriterGrpc.RejigWriterBlockingStub stub = createStub();
    FragmentList assignments = FragmentList.newBuilder()
      .addAddress("server1:port1")
      .addAddress("server2:port1")
      .addAddress("server1:port1")
      .build();
    RejigConfig reply = stub.setConfig(assignments);

    assertEquals(reply.getId(), 1);
    assertEquals(reply.getFragmentCount(), 3);
    assertEquals(reply.getFragment(0).getAddress(), "server1:port1");
    assertEquals(reply.getFragment(1).getAddress(), "server2:port1");
    assertEquals(reply.getFragment(2).getAddress(), "server1:port1");

    assignments = FragmentList.newBuilder()
      .addAddress("server2:port1")
      .addAddress("server2:port1")
      .addAddress("server1:port1")
      .build();
    reply = stub.setConfig(assignments);

    // Ensure reply is correct.
    assertEquals(reply.getId(), 2);
    assertEquals(reply.getFragmentCount(), 3);
    assertEquals(reply.getFragment(0).getAddress(), "server2:port1");
    assertEquals(reply.getFragment(1).getAddress(), "server2:port1");
    assertEquals(reply.getFragment(2).getAddress(), "server1:port1");
  }

  private RejigWriterGrpc.RejigWriterBlockingStub createStub() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    // Create a server, add service, start, and register for automatic
    // graceful shutdown.
    InMemoryConfig config = new InMemoryConfig();
    grpcCleanup.register(InProcessServerBuilder
      .forName(serverName).directExecutor()
      .addService(new RejigCoordinatorWriter(config))
      .build().start());

    RejigWriterGrpc.RejigWriterBlockingStub blockingStub = RejigWriterGrpc
      .newBlockingStub(grpcCleanup
        .register(InProcessChannelBuilder.forName(serverName)
          .directExecutor().build()));

    return blockingStub;
  }
}