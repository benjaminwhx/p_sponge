package com.github.sponge.persist;

import com.github.sponge.exception.SpongeException;

public interface Persistence {
	/**
	 * 保存一次数据到持久化介质
	 * @param bytes
	 * @return
	 */
	boolean persistData(byte[] bytes);
	
	/**
	 * 从持久化介质获取一批任务
	 * @return
	 * @throws SpongeException
	 */
	byte[] fetchDataFromPersistence() throws SpongeException;
	
	/**
	 * 启动的时候，该存储介质是否有没被消费的任务,如果有返回true,如果没有返回false;
	 * @return
	 */
	boolean haveDataInPersistence();
}
