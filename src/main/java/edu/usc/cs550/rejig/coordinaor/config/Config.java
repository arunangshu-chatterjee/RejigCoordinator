package edu.usc.cs550.rejig.coordinator.config;

import edu.usc.cs550.rejig.interfaces.RejigConfig;

/**
 * An interface to access, and modify the current fragment
 * to CMI assignments.
 */
public interface Config {
  /** Returns a serialized version of the config. */
  RejigConfig get();

  /** Get the current config id. */
  int getConfigId();

  /**
   * Returns the CMI address assigned to the given fragment.
   * Returns null if fragment does not exist.
   */
  String getFragment(int fragmentNum);

  /**
   * To call before calling any of the setter methods.
   * Manage any necessary locking here.
   */
  Config beginUpdate();

  /**
   * Sets a new CMI address for the given fragment number.
   * Overrides existing fragments.
   */
  Config setFragment(int fragmentNum, String newAddr);

  /**
   * Deletes a fragment from the config.
   */
  Config deleteFragment(int fragmentNum);

  /**
   * To call after calling all of the setter methods.
   * Manage any necessary locking here.
   * This method should also increment the config id.
   */
  Config endUpdate();
}