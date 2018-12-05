package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.HashMap;
import java.util.function.IntConsumer;

/**
 * An zookeeper implementation of Config.
 */
public class ZooKeeperConfig implements Config {
  private static final int sleepBetweenRetries = 1000;
  private static final int maxRetries = 3;

  private CuratorFramework client;

  String rootPath;

  InterProcessReadWriteLock lock;

  private InterProcessMutex tempLock;

  public ZooKeeperConfig(CuratorFramework client) {
    rootPath = "/rejig_config";
    this.client = client;
    lock = new InterProcessReadWriteLock(client, getLockPath());

    try {
      if (client.checkExists().forPath(rootPath) == null) {
        client.create().forPath(rootPath);
        client.create().forPath(getConfigIdPath(), "0".getBytes());
        client.create().forPath(getFragmentCountPath(), "0".getBytes());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RejigConfig get() {
    InterProcessMutex lock = readLockAcquire();
    try {
      RejigConfig.Builder builder = RejigConfig.newBuilder();
      builder.setId(getConfigId());
      Integer fragCount = Integer.parseInt(new String(client.getData().forPath(getFragmentCountPath())));
      for (int i = 0; i < fragCount; i++) {
        builder.addFragment(getFragment(i));
      }
      return builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      readLockRelease(lock);
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
    InterProcessMutex lock = readLockAcquire();
    try {
      String str = new String(client.getData().forPath(getConfigIdPath()));
      return Integer.parseInt(str);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      readLockRelease(lock);
    }
  }

  @Override
  public Fragment getFragment(int fragmentNum) {
    InterProcessMutex lock = readLockAcquire();
    try {
      FragmentPath path = getFragmentPath(fragmentNum);
      if (client.checkExists().forPath(path.root) == null) {
        return null;
      }
      String fragIdStr = new String(client.getData().forPath(path.id));
      String fragAddress = new String(client.getData().forPath(path.address));
      return Fragment.newBuilder()
        .setId(Integer.parseInt(fragIdStr))
        .setAddress(fragAddress)
        .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      readLockRelease(lock);
    }
  }

  @Override
  public ZooKeeperConfig beginUpdate() {
    tempLock = writeLockAcquire();
    return this;
  }

  @Override
  public ZooKeeperConfig setFragment(int fragmentNum, String newAddr) {
    throwIfSetterInvalidUsage();
    try {
      FragmentPath path = getFragmentPath(fragmentNum);
      if (client.checkExists().forPath(path.root) != null) {
        Integer fragId = Integer.parseInt(new String(client.getData().forPath(path.id)));
        client.setData().forPath(path.address, newAddr.getBytes());
        fragId += 1;
        client.setData().forPath(path.id, fragId.toString().getBytes());
      } else {
        String message = String.format(
          "Trying to set a fragment that doesn't already exist. Fragment number passed: %d",
          fragmentNum
        );
        throw new IndexOutOfBoundsException(message);
      }
    } catch (Exception e) {
      writeLockRelease(tempLock);
      tempLock = null;
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public ZooKeeperConfig addFragment(String newAddr) {
    throwIfSetterInvalidUsage();
    try {
      String fragCountPath = getFragmentCountPath();
      Integer fragNum = Integer.parseInt(new String(client.getData().forPath(fragCountPath)));
      FragmentPath path = getFragmentPath(fragNum);
      client.create().forPath(path.root);
      client.create().forPath(path.address, newAddr.getBytes());
      client.create().forPath(path.id, "1".getBytes());
      fragNum += 1;
      client.setData().forPath(fragCountPath, fragNum.toString().getBytes());
    } catch (Exception e) {
      writeLockRelease(tempLock);
      tempLock = null;
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public ZooKeeperConfig endUpdate() {
    throwIfSetterInvalidUsage();
    try {
      String path = getConfigIdPath();
      Integer id = Integer.parseInt(new String(client.getData().forPath(path))) + 1;
      client.setData().forPath(path, id.toString().getBytes());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      writeLockRelease(tempLock);
      tempLock = null;
    }
    return this;
  }

  /**
   * Throws an exception if a setter is called without first
   * calling beiginUpdate.
   */
  private void throwIfSetterInvalidUsage() {
    if (tempLock == null) {
      throw new IllegalStateException("Calling a setter before calling beginUpdate!");
    }
  }

  private InterProcessMutex readLockAcquire() {
    try {
      InterProcessMutex ret = lock.readLock();
      ret.acquire();
      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void readLockRelease(InterProcessMutex lock) {
    try {
      lock.release();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private InterProcessMutex writeLockAcquire() {
    try {
      InterProcessMutex ret = lock.writeLock();
      ret.acquire();
      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void writeLockRelease(InterProcessMutex lock) {
    try {
      lock.release();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getConfigIdPath() {
    return rootPath + "/id";
  }

  private String getLockPath() {
    return rootPath + "/lock";
  }

  private String getFragmentCountPath() {
    return rootPath + "/fragmentCount";
  }

  private FragmentPath getFragmentPath(int fragmentNum) {
    return new FragmentPath(rootPath, fragmentNum);
  }
}

class FragmentPath {
  public String root;
  public String id;
  public String address;

  public FragmentPath(String root, int fragmentNum) {
    this.root = root + "/fragment_" + fragmentNum;
    this.id = this.root + "/id";
    this.address = this.root + "/address";
  }
}
