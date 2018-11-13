package com.extendedrejig.model;

import java.util.HashMap;

public class InMemoryConfig implements Config<String> {
  private ConfigModel<String> config = new ConfigModel<>();

  public InMemoryConfig() {
    config.globalConfigId = 0;
    config.fragmentToCMIMap = new HashMap<>();
  }

  public ConfigModel<String> get() {
    return this.config;
  }

  public void setFragment(int fragmentNum, String newAddr) {
    config.fragmentToCMIMap.put(fragmentNum, newAddr);
  }

  public void deleteFragment(int fragmentNum) {
    config.fragmentToCMIMap.remove(fragmentNum);
  }

  public String getFragment(int fragmentNum) {
    return config.fragmentToCMIMap.get(fragmentNum);
  }
}
