package com.extendedrejig.model;

import java.util.Map;

public class ConfigModel<T> {
	public int globalConfigId;
	public Map<Integer, T> fragmentToCMIMap;
}
