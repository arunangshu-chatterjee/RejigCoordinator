package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.FragmentAssignments;
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
    assertEquals(defaultConfig.getMapping().getFragmentToCMICount(), 0);
  }

  /**
   * Test that setFragment adds fragments correctly.
   */
  @Test
  public void inMemoryConfig_setFragment() {
    InMemoryConfig config = new InMemoryConfig();
    FragmentAssignments assignments = config.beginUpdate()
      .setFragment(1, "server1:port1")
      .setFragment(2, "server2:port2")
      .setFragment(4, "server3:port3")
      .endUpdate().get().getMapping();

    assertEquals(assignments.getFragmentToCMICount(), 3);
    assertEquals(assignments.getFragmentToCMIOrDefault(1, null), "server1:port1");
    assertEquals(assignments.getFragmentToCMIOrDefault(2, null), "server2:port2");
    assertEquals(assignments.getFragmentToCMIOrDefault(3, null), null);
    assertEquals(assignments.getFragmentToCMIOrDefault(4, null), "server3:port3");
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
    FragmentAssignments assignments = conf.getMapping();

    assertEquals(conf.getId(), 1);
    assertEquals(assignments.getFragmentToCMICount(), 0);
  }

  /**
   * Test that deleteFragment removes fragments correctly.
   */
  @Test
  public void inMemoryConfig_deleteFragment() {
    InMemoryConfig config = new InMemoryConfig();

    FragmentAssignments assignments = config.beginUpdate()
      .setFragment(1, "server1:port1")
      .setFragment(2, "server2:port2")
      .setFragment(4, "server3:port3")
      .endUpdate().get().getMapping();

    assertEquals(assignments.getFragmentToCMICount(), 3);
    assignments = config.beginUpdate()
      .deleteFragment(2)
      .deleteFragment(1)
      .endUpdate().get().getMapping();


    assertEquals(assignments.getFragmentToCMICount(), 1);
    assertEquals(assignments.getFragmentToCMIOrDefault(1, null), null);
    assertEquals(assignments.getFragmentToCMIOrDefault(2, null), null);
    assertEquals(assignments.getFragmentToCMIOrDefault(3, null), null);
    assertEquals(assignments.getFragmentToCMIOrDefault(4, null), "server3:port3");
  }

  /**
   * Test that any setter called before beginUpdate throws an error.
   */
  @Test(expected = IllegalStateException.class)
  public void inMemoryConfig_noSetterBeforeBeginUpdate() {
    InMemoryConfig config = new InMemoryConfig();

    RejigConfig conf = config.setFragment(1, "server1:port1")
      .endUpdate().get();
  }

  /** Test the getters. */
  @Test
  public void inMemoryConfig_getters() {
    InMemoryConfig config = new InMemoryConfig();
    config.beginUpdate().setFragment(1, "server1:port1").endUpdate();

    assertEquals(config.getConfigId(), 1);
    assertEquals(config.getFragment(1), "server1:port1");
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