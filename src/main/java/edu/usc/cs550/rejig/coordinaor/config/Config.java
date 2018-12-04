package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.Fragment;
import edu.usc.cs550.rejig.interfaces.RejigConfig;

/**
 * An interface to access, and modify the current fragment
 * to CMI assignments.
 */
public interface Config {
  /** Returns a serialized version of the config. */
  RejigConfig get();

  /**
   * Returns a serialized version of the config.
   * But removes all fragments from the end which have a
   * null or empty server address.
   */
  RejigConfig getCleaned();

  /** Get the current config id. */
  int getConfigId();

  /**
   * Returns the Fragment at the specified index.
   * Returns null if fragment does not exist.
   */
  Fragment getFragment(int fragmentNum);

  /**
   * To call before calling any of the setter methods.
   * Manage any necessary locking here.
   */
  Config beginUpdate();

  /**
   * Sets a new CMI address for the given fragment number.
   * Overrides existing fragments. Should update the fragment id.
   */
  Config setFragment(int fragmentNum, String newAddr);

  /**
   * Add a new CMI address to the end of the fragment list.
   */
  Config addFragment(String newAddr);

  /**
   * To call after calling all of the setter methods.
   * Manage any necessary locking here.
   * This method should also increment the config id.
   */
  Config endUpdate();
}