package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.FragmentList;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InMemoryConfigTest {

  /**
   * On creation of the an InMemoryConfig, the internal config
   * should be empty.
   */
  @Test
  public void inMemoryConfig_init() {
    InMemoryConfig config = new InMemoryConfig();
    RejigConfig defaultConfig = config.get();
    assertEquals(defaultConfig.getId(), 0);
    assertEquals(defaultConfig.getFragmentCount(), 0);
  }

  /**
   * Test that addFragment adds fragments correctly.
   */
  @Test
  public void inMemoryConfig_addFragment() {
    InMemoryConfig config = new InMemoryConfig();
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
  }

  /**
   * Test that setFragment sets fragments correctly.
   */
  @Test
  public void inMemoryConfig_setFragment() {
    InMemoryConfig config = new InMemoryConfig();
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
  }

  /**
   * Test that endUpdate increments the config id.
   */
  @Test
  public void inMemoryConfig_endUpdateIncrementsId() {
    InMemoryConfig config = new InMemoryConfig();

    assertEquals(config.get().getId(), 0);
    RejigConfig conf = config.beginUpdate()
      .endUpdate().get();

    assertEquals(conf.getId(), 1);
    assertEquals(conf.getFragmentCount(), 0);
  }

  /**
   * Test that any setter called before beginUpdate throws an error.
   */
  @Test(expected = IllegalStateException.class)
  public void inMemoryConfig_noSetterBeforeBeginUpdate() {
    InMemoryConfig config = new InMemoryConfig();

    RejigConfig conf = config.addFragment("server1:port1")
      .endUpdate().get();
  }

  /** Test the getters. */
  @Test
  public void inMemoryConfig_getters() {
    InMemoryConfig config = new InMemoryConfig();
    config.beginUpdate().addFragment( "server1:port1").endUpdate();

    assertEquals(config.getConfigId(), 1);
    assertEquals(config.getFragment(0).getAddress(), "server1:port1");
  }

  /** Test that beginUpdate locks the object. And endlock unlocks. */
  @Test
  public void inMemoryConfig_beginUpdateLock() throws InterruptedException {
    InMemoryConfig config = new InMemoryConfig();
    config.beginUpdate();

    final boolean[] lockResult = new boolean[1];
    lockResult[0] = true;
    Runnable task = new Runnable() {
      @Override
      public void run() {
        lockResult[0] = config.lock.readLock().tryLock();
        if (lockResult[0]) {
          config.lock.readLock().unlock();
        }
      }
    };
    Thread thread = new Thread(task);
    thread.start();
    thread.join();
    assertEquals(config.lock.isWriteLocked(), true);
    assertEquals(lockResult[0], false);

    config.endUpdate();
    lockResult[0] = false;
    thread = new Thread(task);
    thread.start();
    thread.join();
    assertEquals(config.lock.isWriteLocked(), false);
    assertEquals(lockResult[0], true);
  }
}