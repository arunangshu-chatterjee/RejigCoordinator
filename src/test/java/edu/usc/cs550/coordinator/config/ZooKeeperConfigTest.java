package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.FragmentList;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import static org.junit.Assert.assertEquals;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class ZooKeeperConfigTest {

  /**
   * On creation of the an ZooKeeperConfig, the internal config
   * should be empty.
   */
  @Test
  public void ZooKeeperConfig_init() {
    TestingServer server = createServer();
    CuratorFramework client = createClient(server);
    ZooKeeperConfig config = new ZooKeeperConfig(client);
    RejigConfig defaultConfig = config.get();
    assertEquals(defaultConfig.getId(), 0);
    assertEquals(defaultConfig.getFragmentCount(), 0);
    cleanUp(server, client);
  }

  /**
   * Test that addFragment adds fragments correctly.
   */
  @Test
  public void ZooKeeperConfig_addFragment() {
    TestingServer server = createServer();
    CuratorFramework client = createClient(server);
    ZooKeeperConfig config = new ZooKeeperConfig(client);
    RejigConfig conf = config.beginUpdate()
      .addFragment("server1:port1")
      .addFragment("server2:port2")
      .addFragment("server3:port3")
      .endUpdate().get();

    assertEquals(conf.getFragmentCount(), 3);
    assertEquals(conf.getFragment(0).getId(), 1);
    assertEquals(conf.getFragment(0).getAddress(), "server1:port1");
    assertEquals(conf.getFragment(1).getId(), 1);
    assertEquals(conf.getFragment(1).getAddress(), "server2:port2");
    assertEquals(conf.getFragment(2).getId(), 1);
    assertEquals(conf.getFragment(2).getAddress(), "server3:port3");
    cleanUp(server, client);
  }

  /**
   * Test that setFragment sets fragments correctly.
   */
  @Test
  public void ZooKeeperConfig_setFragment() {
    TestingServer server = createServer();
    CuratorFramework client = createClient(server);
    ZooKeeperConfig config = new ZooKeeperConfig(client);
    RejigConfig conf = config.beginUpdate()
      .addFragment("server1:port1")
      .addFragment("server2:port2")
      .endUpdate().get();

    assertEquals(conf.getFragmentCount(), 2);
    assertEquals(conf.getFragment(0).getId(), 1);
    assertEquals(conf.getFragment(0).getAddress(), "server1:port1");
    assertEquals(conf.getFragment(1).getId(), 1);
    assertEquals(conf.getFragment(1).getAddress(), "server2:port2");

    conf = config.beginUpdate()
      .setFragment(1, "server3:port3")
      .endUpdate().get();

    assertEquals(conf.getFragmentCount(), 2);
    assertEquals(conf.getId(), 2);
    assertEquals(conf.getFragment(0).getId(), 1);
    assertEquals(conf.getFragment(0).getAddress(), "server1:port1");
    assertEquals(conf.getFragment(1).getId(), 2);
    assertEquals(conf.getFragment(1).getAddress(), "server3:port3");
    cleanUp(server, client);
  }

  /**
   * Test that endUpdate increments the config id.
   */
  @Test
  public void ZooKeeperConfig_endUpdateIncrementsId() {
    TestingServer server = createServer();
    CuratorFramework client = createClient(server);
    ZooKeeperConfig config = new ZooKeeperConfig(client);

    assertEquals(config.get().getId(), 0);
    RejigConfig conf = config.beginUpdate()
      .endUpdate().get();

    assertEquals(conf.getId(), 1);
    assertEquals(conf.getFragmentCount(), 0);
    cleanUp(server, client);
  }

  /**
   * Test that any setter called before beginUpdate throws an error.
   */
  @Test(expected = IllegalStateException.class)
  public void ZooKeeperConfig_noSetterBeforeBeginUpdate() {
    TestingServer server = createServer();
    CuratorFramework client = createClient(server);
    ZooKeeperConfig config = new ZooKeeperConfig(client);

    try {
      RejigConfig conf = config.addFragment("server1:port1")
        .endUpdate().get();
    } finally {
      cleanUp(server, client);
    }
  }

  /** Test the getters. */
  @Test
  public void ZooKeeperConfig_getters() {
    TestingServer server = createServer();
    CuratorFramework client = createClient(server);
    ZooKeeperConfig config = new ZooKeeperConfig(client);
    config.beginUpdate().addFragment( "server1:port1").endUpdate();

    assertEquals(config.getConfigId(), 1);
    assertEquals(config.getFragment(0).getAddress(), "server1:port1");
    cleanUp(server, client);
  }

  /** Test that beginUpdate locks the object. And endlock unlocks. */
  @Test
  public void ZooKeeperConfig_beginUpdateLock() throws InterruptedException {
    final TestingServer server = createServer();
    final CuratorFramework client = createClient(server);
    final ZooKeeperConfig config = new ZooKeeperConfig(client);
    config.beginUpdate();

    final boolean[] lockResult = new boolean[1];
    lockResult[0] = true;
    Runnable task = new Runnable() {
      @Override
      public void run() {
        CuratorFramework client2 = createClient(server);
        ZooKeeperConfig config2 = new ZooKeeperConfig(client2);
        InterProcessMutex lock = config2.lock.readLock();
        try {
          lockResult[0] = lock.acquire(5, TimeUnit.SECONDS);
          if (lockResult[0]) {
            lock.release();
          }
          client2.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    Thread thread = new Thread(task);
    thread.start();
    thread.join();
    assertEquals(lockResult[0], false);

    config.endUpdate();
    lockResult[0] = false;
    thread = new Thread(task);
    thread.start();
    thread.join();
    assertEquals(lockResult[0], true);
    cleanUp(server, client);
  }

  /** Creates a mock zookeeper server */
  private TestingServer createServer() {
    try {
      return new TestingServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates a zookeeper client. */
  private CuratorFramework createClient(TestingServer server) {
    try {
      ExponentialBackoffRetry policy = new ExponentialBackoffRetry(1000, 3);
      CuratorFramework client = CuratorFrameworkFactory
        .newClient(server.getConnectString(), policy);
      client.start();
      return client;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Closes the zookeeper server, and client. */
  private void cleanUp(TestingServer server, CuratorFramework client) {
    try {
      client.close();
      server.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}