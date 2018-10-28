package com.extendedrejig.coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.extendedrejig.model.Config;
import com.extendedrejig.model.ConfigModel;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class RejigCoordinator<T> {
	
	private static final String CONFIG_ID_MEMCACHED_KEY = "$$CONFIG_ID_KEY$$";
	
	private Config<T> config;

	private Map<String, MemCachedClient> memCachedClients = new HashMap<String, MemCachedClient>();

	RejigCoordinator(Config<String> config) {
		for(Entry<Integer, String> entry : config.get().fragmentToCMIMap.entrySet()) {
			String cmiAddr = entry.getValue();
			SockIOPool pool = SockIOPool.getInstance(cmiAddr);
            pool.setServers(new String[] { cmiAddr });
			this.memCachedClients.put(cmiAddr, new MemCachedClient(cmiAddr));
            updateImpactedCMI(cmiAddr);
		}
    }

	public void updateImpactedCMI(String oldAddr) { // throw exception //Sets the globalConfigId of the impacted CMI
		MemCachedClient mcc = this.memCachedClients.get(oldAddr);
		mcc.set(CONFIG_ID_MEMCACHED_KEY, config.get().globalConfigId);
	}
	
	Config<T> getConfig() {
		return config;
	}

	void setConfig(Config<T> newConfig) throws Exception {
		/*
         * setConfig happens when there are 3 scenarios:
         * 1. Addition of fragments
         * 2. Deletion of fragments
         * 3. Movement of fragments from 1 CMI to another
         * 
         * Algorithm to setConfig for addition or deletion of fragments:
         * 1. Get old and new configs and store them in 2 sets
         * 2. Get the result of the intersection of these 2 sets. X
         * 3. if length of old config != new config: 
         *    	update new_config in the intersection list CMIs
         *    else:
         *    	for each fragmentNo:
         *    		if IP:Port different in old config vs. new config
         *    			update fragment IP:Port to new config
         */
		
		Map<Integer, T> oldConfigMap = config.get().fragmentToCMIMap;
		Map<Integer, T> newConfigMap = newConfig.get().fragmentToCMIMap;
		
		if (oldConfigMap.size() < newConfigMap.size()) {
			Map<Integer, T> result = new HashMap<Integer, T>(oldConfigMap);
			result.keySet().retainAll(newConfigMap.keySet());
			for(Entry<Integer, T> entry : result.entrySet()) {
				String cmiAddr = (String) entry.getValue();
				this.memCachedClients.put(cmiAddr, new MemCachedClient(cmiAddr));
	            updateImpactedCMI(cmiAddr); //Add new Fragment details
			}
		}
		else if(oldConfigMap.size() > newConfigMap.size()) {
			Map<Integer, T> result = new HashMap<Integer, T>(oldConfigMap);
			result.keySet().retainAll(newConfigMap.keySet());
			for(Entry<Integer, T> entry : result.entrySet()) {
				String cmiAddr = (String) entry.getValue();
				this.memCachedClients.put(cmiAddr, new MemCachedClient(cmiAddr));
	            updateImpactedCMI(cmiAddr); //Remove new Fragment details
			}
		}
		else {
			for(Entry<Integer, T> entry : oldConfigMap.entrySet()) {
				String oldCMIAddr = (String) entry.getValue();
				if (oldCMIAddr != newConfig) 
			}
		}
		
		
		
		
        config.get().globalConfigId++;  
        //Different for  loop for checking cases where fragments doesn't exist in new(removal of fragments.)
        //Check case where new frag is added while one is deleted
        for(Integer fragmentNo : newConfig.get().fragmentToCMIMap.keySet()) {
        	String oldAddr = (String) this.config.getFragment(fragmentNo);
        	this.config.setFragment(fragmentNo, newConfig.get().fragmentToCMIMap.get(fragmentNo));
        	updateImpactedCMI(oldAddr);
        }
    }
}
