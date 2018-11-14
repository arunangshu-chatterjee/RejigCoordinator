package edu.usc.cs550.rejig.coordinator;

import edu.usc.cs550.rejig.coordinator.config.Config;
import edu.usc.cs550.rejig.interfaces.FragmentAssignments;
import edu.usc.cs550.rejig.interfaces.RejigConfig;
import edu.usc.cs550.rejig.interfaces.RejigWriterGrpc;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashSet;

/**
 * A GRPC service to write/modify the coordinator config.
 * This class manages the passed Config object.
 */
public class RejigCoordinatorWriter extends RejigWriterGrpc.RejigWriterImplBase {

  private static final String CONFIG_ID_MEMCACHED_KEY = "$$CONFIG_ID_KEY$$";

  protected Config config;

  protected Map<String, MemCachedClient> memCachedClients = new HashMap<String, MemCachedClient>();

  /**
   * Creates a writer to RejigCoordinator.
   * The passed config should be contain an empty fragment assignment,
   * and setConfig should be called with the initializing fragment assignment.
   */
  RejigCoordinatorWriter(Config config) {
    this.config = config;
    if (config.get().getMapping().getFragmentToCMICount() > 0) {
      throw new IllegalArgumentException("Initial config object's fragment assignments should be empty.");
    }
  }

  @Override
  public void setConfig(FragmentAssignments newAssignments, StreamObserver<RejigConfig> responseObserver) {
    updateConfig(newAssignments);
    responseObserver.onNext(config.get());
    responseObserver.onCompleted();
  }

  /** Create a memcached client to the specified address. */
  private void createMemcachedClient(String cmiAddr) {
    if (!memCachedClients.containsKey(cmiAddr)) {
      SockIOPool pool = SockIOPool.getInstance(cmiAddr);
      pool.setServers(new String[] { cmiAddr });
      memCachedClients.put(cmiAddr, new MemCachedClient(cmiAddr));
    }
  }

  /** Update the config object in specified CMI. */
  private void updateImpactedCMI(String oldAddr) {
    MemCachedClient mcc = memCachedClients.get(oldAddr);
    mcc.set(CONFIG_ID_MEMCACHED_KEY, config.getConfigId());
  }

  /**
   * Updates the current config to match the new config.
   * Also updates memcached clients map.
   */
  private void updateConfig(FragmentAssignments assignments) {
    Map<Integer, String> oldAssignments = config.get().getMapping().getFragmentToCMIMap();
    Map<Integer, String> newAssignments = assignments.getFragmentToCMIMap();

    HashSet<String> impactedCMIs = new HashSet<>();
    HashSet<String> removedCMIs  = new HashSet<>();

    config.beginUpdate();
    // Get deleted fragments.
    for (int fragmentNum : oldAssignments.keySet()) {
      if (!newAssignments.containsKey(fragmentNum)) {
        String cmiAddr = oldAssignments.get(fragmentNum);
        impactedCMIs.add(cmiAddr);
        removedCMIs.add(cmiAddr);
        config.deleteFragment(fragmentNum);
      }
    }

    // Get newly added, and modified fragments.
    for (int fragmentNum: newAssignments.keySet()) {
      String cmiAddr = newAssignments.get(fragmentNum);
      // Create a client for all cmi addrs if it doesn't already exist.
      createMemcachedClient(cmiAddr);
      if (removedCMIs.contains(cmiAddr)) {
        removedCMIs.remove(cmiAddr);
      }
      if (!oldAssignments.containsKey(fragmentNum)) {
        // New fragments.
        impactedCMIs.add(cmiAddr);
        config.setFragment(fragmentNum, cmiAddr);
      } else {
        String oldAddr = oldAssignments.get(fragmentNum);
        if (!oldAddr.equals(cmiAddr)) {
          // Modified fragments.
          impactedCMIs.add(oldAddr);
          config.setFragment(fragmentNum, cmiAddr);
        }
      }
    }
    config.endUpdate();

    int newConfigId = config.getConfigId();
    for (String cmiAddr : impactedCMIs) {
      MemCachedClient client = memCachedClients.get(cmiAddr);
      client.set(CONFIG_ID_MEMCACHED_KEY, newConfigId);
    }

    for (String cmiAddr : removedCMIs) {
      deleteMemcachedClient(cmiAddr);
    }
  }

  /** Deletes clients from the existing map. */
  private void deleteMemcachedClient(String cmiAddr) {
     memCachedClients.remove(cmiAddr);
  }
}
