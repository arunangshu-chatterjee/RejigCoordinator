package com.extendedrejig.coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.extendedrejig.model.Config;
import com.extendedrejig.model.ConfigModel;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class RejigCoordinator<T> {
	
	private static final String CONFIG_ID_MEMCACHED_KEY = "$$CONFIG_ID_KEY$$";
	
	private Config<T> config;

	private Map<String, MemCachedClient> memCachedClients = new HashMap<String, MemCachedClient>();

	public void createPool(String cmiAddr) {
		SockIOPool pool = SockIOPool.getInstance(cmiAddr);
        pool.setServers(new String[] { cmiAddr });
		this.memCachedClients.put(cmiAddr, new MemCachedClient(cmiAddr));
	}
	
	RejigCoordinator(Config<String> config) {
		for(Entry<Integer, String> entry : config.get().fragmentToCMIMap.entrySet()) {
			String cmiAddr = entry.getValue();
			createPool(cmiAddr);
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
         * 2. For each item in old_config:
         * 		if item present in both old_config and new_config and IP:Port of old_config != IP:Port of new_config 
         *    		update old_config to new_config
         *    	else
         *    		remove item from the old_config (update old_config to new_config)
         * 3. For each item in new_config minus old_config:
         *    		update fragment IP:Port to new config
         */
		
		Map<Integer, T> oldConfigMap = config.get().fragmentToCMIMap;
		Map<Integer, T> newConfigMap = newConfig.get().fragmentToCMIMap;
		
		for (int fragmentNum : oldConfigMap.keySet()) {
			if (newConfigMap.keySet().contains(fragmentNum) && oldConfigMap.get(fragmentNum) != newConfigMap.get(fragmentNum)) {
				String cmiAddr = (String) newConfigMap.get(fragmentNum);
				createPool(cmiAddr); //Check if required
				updateImpactedCMI(cmiAddr);
			}
			else if (!newConfigMap.keySet().contains(fragmentNum)){
				String cmiAddr = (String) newConfigMap.get(fragmentNum);
				createPool(cmiAddr); //Check if required
				updateImpactedCMI(cmiAddr);
			}
		}
		
		Set<Map.Entry<Integer, T>> filter = oldConfigMap.entrySet();
		for( Map.Entry<Integer, T> entry : newConfigMap.entrySet() )
		{
			 if( !filter.contains( entry ))
			 { 
				 String cmiAddr = (String) entry.getValue();
					createPool(cmiAddr); //Check if required
					updateImpactedCMI(cmiAddr);
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
