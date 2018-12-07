package edu.usc.cs550.rejig.coordinator;

import edu.usc.cs550.rejig.client.MemcachedClient;
import edu.usc.cs550.rejig.client.configreader.RejigConfigReader;
import edu.usc.cs550.rejig.client.SockIOPool;
import edu.usc.cs550.rejig.coordinator.config.Config;
import edu.usc.cs550.rejig.interfaces.FragmentList;
import edu.usc.cs550.rejig.interfaces.RejigConfig;
import edu.usc.cs550.rejig.interfaces.RejigWriterGrpc;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicReference;
import java.util.*;

/**
 * A GRPC service to write/modify the coordinator config.
 * This class manages the passed Config object.
 */
public class RejigCoordinatorWriter extends RejigWriterGrpc.RejigWriterImplBase {

  protected Config config;

  protected AtomicReference<MemcachedClient> mc;

  /**
   * Creates a writer to RejigCoordinator.
   * The passed config contain an empty fragment assignments,
   * and setConfig should be called with the initializing fragment assignments.
   */
  RejigCoordinatorWriter(Config config) {
    this.config = config;
    mc = new AtomicReference<>();
    if (config.get().getFragmentCount() > 0) {
      throw new IllegalArgumentException("Initial config object's fragment assignments should be empty.");
    }
  }

  @Override
  public void setConfig(FragmentList newAssignments, StreamObserver<RejigConfig> responseObserver) {
    RejigConfig conf = updateConfig(newAssignments);
    responseObserver.onNext(conf);
    responseObserver.onCompleted();
  }

  /** Create a memcached client to the specified address. */
  private MemcachedClient createMemcachedClient(RejigConfig config) {
    ConfigReader configReader = new ConfigReader();
    configReader.setConfig(config);
    SockIOPool.SockIOPoolOptions options = new SockIOPool.SockIOPoolOptions();
    options.initConn = 1;
    options.minConn = 1;
    options.maxConn = 1;
    options.maintSleep = 20;
    options.nagle = false;

    MemcachedClient client = new MemcachedClient(configReader, options);
    return client;
  }

  /**
   * Updates the current config to match the new config.
   * Also updates memcached clients map.
   */
  private RejigConfig updateConfig(FragmentList assignments) {
    HashMap<String, ArrayList<Integer>> impactedCMIs = new HashMap<>();
    HashMap<String, ArrayList<Integer>> toLease = new HashMap<>();
    RejigConfig oldConfig = config.get();
    int oldFragmentCount = oldConfig.getFragmentCount();
    int minIndex = Math.min(oldFragmentCount, assignments.getAddressCount());

    config.beginUpdate();
    for (int i = 0; i < minIndex; i++) {
      String oldAddr = oldConfig.getFragment(i).getAddress();
      String newAddr = assignments.getAddress(i);
      if (!oldAddr.equals(newAddr)) {
        if (!impactedCMIs.containsKey(oldAddr)) {
          impactedCMIs.put(oldAddr, new ArrayList<>());
        }
        impactedCMIs.get(oldAddr).add(i + 1);
        if (!toLease.containsKey(newAddr)) {
          toLease.put(newAddr, new ArrayList<>());
        }
        toLease.get(newAddr).add(i + 1);
        config.setFragment(i, newAddr);
      }
    }

    for (int i = minIndex; i < assignments.getAddressCount(); i++) {
      String newAddr = assignments.getAddress(i);
      if (!toLease.containsKey(newAddr)) {
        toLease.put(newAddr, new ArrayList<>());
      }
      toLease.get(newAddr).add(i + 1);
      config.addFragment(newAddr);
    }

    for (int i = minIndex; i < oldFragmentCount; i++) {
      String oldAddr = config.getFragment(i).getAddress();
      if (!impactedCMIs.containsKey(oldAddr)) {
        impactedCMIs.put(oldAddr, new ArrayList<>());
      }
      impactedCMIs.get(oldAddr).add(i + 1);
      config.setFragment(i, "");
    }
    config.endUpdate();

    RejigConfig newConfig = config.getCleaned();
    MemcachedClient newClient = createMemcachedClient(newConfig);
    // Grant leases on new fragments.
    for (String cmi : toLease.keySet()) {
      newClient.setConfig(newConfig, null, cmi);
      Date expiry = new Date(System.currentTimeMillis() + 30 * 24 * 60 * 60 * 1000);
      for (int fragmentNum : toLease.get(cmi)) {
        newClient.grantLease(fragmentNum, expiry, cmi);
      }
    }

    MemcachedClient oldClient = mc.get();
    if (oldClient != null) {
      // Revoke leases for impacted fragments.
      // Update config on impacted cmi.
      for (String impactedCMI : impactedCMIs.keySet()) {
        for (int fragmentNum : impactedCMIs.get(impactedCMI)) {
          oldClient.revokeLease(fragmentNum, impactedCMI);
        }
        oldClient.setConfig(newConfig, null, impactedCMI);
      }
    }

    // Swap memcached clients.
    mc.getAndSet(newClient);
    return newConfig;
  }
}

class ConfigReader implements RejigConfigReader {
  private RejigConfig config;

  @Override
  public RejigConfig getConfig() {
    return config;
  }

  public void setConfig(RejigConfig config) {
    this.config = config;
  }
}
