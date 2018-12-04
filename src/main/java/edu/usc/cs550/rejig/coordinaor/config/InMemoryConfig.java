package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An in-memory implementation of Config.
 * Stores the server configuratin in a HashMap.
 */
public class InMemoryConfig implements Config {
  private RejigConfig.Builder configBuilder;

  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private boolean isUpdating = false;

  public InMemoryConfig() {
    configBuilder = RejigConfig.newBuilder();
    configBuilder.setId(0);
  }

  @Override
  public RejigConfig get() {
    lock.readLock().lock();
    try {
      return configBuilder.build();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public RejigConfig getCleaned() {
    RejigConfig conf = get();
    RejigConfig.Builder builder = conf.toBuilder();
    for (int i = conf.getFragmentCount() - 1; i >= 0; i--) {
      Fragment f = conf.getFragment(i);
      if (f.getAddress() == null || f.getAddress().equals("")) {
        builder.removeFragment(i);
      }
    }
    return builder.build();
  }

  @Override
  public int getConfigId() {
    lock.readLock().lock();
    try {
      return configBuilder.getId();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Fragment getFragment(int fragmentNum) {
    lock.readLock().lock();
    try {
      if (fragmentNum < configBuilder.getFragmentCount()) {
        return configBuilder.getFragment(fragmentNum);
      }
      return null;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public InMemoryConfig beginUpdate() {
    lock.writeLock().lock();
    isUpdating = true;
    return this;
  }

  @Override
  public InMemoryConfig setFragment(int fragmentNum, String newAddr) {
    throwIfSetterInvalidUsage();
    if (fragmentNum < configBuilder.getFragmentCount()) {
      Fragment frag = configBuilder.getFragment(fragmentNum);
      configBuilder.setFragment(fragmentNum, frag.toBuilder()
        .setId(frag.getId() + 1)
        .setAddress(newAddr)
        .build()
      );
    } else {
      String message = String.format(
        "Fragment number is larger than the size of the fragment list. Fragment number passed: %d, list size: %d",
        fragmentNum,
        configBuilder.getFragmentCount()
      );
      throw new IndexOutOfBoundsException(message);
    }
    return this;
  }

  @Override
  public InMemoryConfig addFragment(String newAddr) {
    throwIfSetterInvalidUsage();
    configBuilder.addFragment(Fragment.newBuilder()
      .setId(1)
      .setAddress(newAddr)
      .build()
    );
    return this;
  }

  @Override
  public InMemoryConfig endUpdate() {
    throwIfSetterInvalidUsage();
    configBuilder.setId(configBuilder.getId() + 1);
    isUpdating = false;
    lock.writeLock().unlock();
    return this;
  }

  /**
   * Throws an exception if a setter is called without first
   * calling beiginUpdate.
   */
  private void throwIfSetterInvalidUsage() {
    if (!isUpdating) {
      throw new IllegalStateException("Calling a setter before calling beginUpdate!");
    }
  }
}
