package com.github.sponge;

import com.github.sponge.persist.Persistence;

/**
 * User: benjamin.wuhaixu
 * Date: 2018-01-02
 * Time: 9:23 pm
 */
public class SpongeService {
	// 持久化服务
	private Persistence thePersistence;
	
	public SpongeService(Persistence thePersistenceParm) {
		thePersistence = thePersistenceParm;
	}

	public Persistence getThePersistence() {
		return thePersistence;
	}
}
