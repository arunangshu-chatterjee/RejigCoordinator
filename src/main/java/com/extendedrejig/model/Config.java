package com.extendedrejig.model;

public interface Config<T> {
  ConfigModel<T> get();

  void setFragment(int fragmentNum, T newAddr);

  void deleteFragment(int fragmentNum);

  T getFragment(int fragmentNum);
}