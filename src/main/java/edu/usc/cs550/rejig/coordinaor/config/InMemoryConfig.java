package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.RejigConfig;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An in-memory implementation of Config.
 * Stores the server configuratin in a HashMap.
 */
public class InMemoryConfig implements Config {
  private RejigConfig.Builder configBuilder;

  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public boolean isUpdating = false;

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
  public int getConfigId() {
    lock.readLock().lock();
    try {
      return configBuilder.getId();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getFragment(int fragmentNum) {
    lock.readLock().lock();
    try {
      return configBuilder.getMapping()
        .getFragmentToCMIOrDefault(fragmentNum, null);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void beginUpdate() {
    lock.writeLock().lock();
    isUpdating = true;
  }

  @Override
  public void setFragment(int fragmentNum, String newAddr) {
    throwIfSetterInvalidUsage();
    configBuilder.getMappingBuilder()
      .putFragmentToCMI(fragmentNum, newAddr);
  }

  @Override
  public void deleteFragment(int fragmentNum) {
    throwIfSetterInvalidUsage();
    configBuilder.getMappingBuilder()
      .removeFragmentToCMI(fragmentNum);
  }

  @Override
  public void endUpdate() {
    throwIfSetterInvalidUsage();
    configBuilder.setId(configBuilder.getId() + 1);
    lock.writeLock().unlock();
  }

  /**
   * Throws an exception if a setter is called without first
   * calling beiginUpdate.
   */
  private void throwIfSetterInvalidUsage() {
    if (!isUpdating) {
      throw new IllegalStateException("Calling a setter before callign beginUpdate!");
    }
  }
}
