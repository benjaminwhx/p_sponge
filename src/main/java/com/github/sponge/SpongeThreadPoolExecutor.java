package com.github.sponge;

import com.github.sponge.exception.SpongeException;
import com.github.sponge.persist.FilePersistence;
import com.github.sponge.persist.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: benjamin.wuhaixu
 * Date: 2018-01-02
 * Time: 9:23 pm
 */
public class SpongeThreadPoolExecutor {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpongeThreadPoolExecutor.class);
	/**
	 * 配置的key name
	 */
	public final static String FILE_PERSISTENCE_DIR = "dir";
	public final static String BLOCKING_QUEUE_CAPACITY = "capacity";
	public final static String BLOCKING_QUEUE_ONCE_PERSIST_LIMIT_SIZE = "oncePersistLimit";
	public final static String MAX_BYTE_ARRAY_SIZE = "maxByteArraySize";
	public final static String ONE_BATCH_WRITE_CNT = "oneBatchWriteCnt";
	public final static String CAN_RELEASE_RES_MAX_TIME = "canReleaseResMaxTime";
	private final static int DEFAULT_CAPACITY = 500;
	private final static int DEFAULT_ONCE_PERSIST_LIMIT_SIZE = 100;
	private final static long DEFAULT_MAX_BYTE_ARRAY_SIZE = 50 * 1024 * 1024;
	private final static int DEFAULT_ONE_BATCH_WRITE_CNT = 20;
	private final static int DEFAULT_RELEASE_RES_MAX_TIME = 60 * 1000;

    private SpongeThreadPoolExecutor() {}

    /**
     * 创建一个默认以文件为持久化缓冲的线程池
     * @param corePoolSize     同java自带ThreadPoolExecutor初始化参数
     * @param maximumPoolSize  同java自带ThreadPoolExecutor初始化参数
     * @param keepAliveTime    同java自带ThreadPoolExecutor初始化参数
     * @param timeUnit         同java自带ThreadPoolExecutor初始化参数
     * @param configMap         key-value方式的参数:
     *      1.FILE_PERSISTENCE_DIR: 持久化文件目录,如 d:/testThread、/root/netcomm;
     *      2.BLOCKING_QUEUE_CAPACITY：队列大小,默认500;
     *      3.BLOCKING_QUEUE_ONCE_PERSIST_LIMIT_SIZE：队列一次持久化的任务数上限,默认100;
     *      4.MAX_BYTE_ARRAY_SIZE：最大允许的内存数,单位byte,默认50 * 1024 * 1024(50M)
	 *      5.ONE_BATCH_WRITE_CNT：进行一次持久化从内存队列中一批最多可以处理的个数,默认20
	 *      6.CAN_RELEASE_RES_MAX_TIME：如果连续等待这么长时间还没有任何持久化的读、写操作，
	 *                             则删除相关资源，如删除序列化的文件,默认60s。
     * @return
     * @throws SpongeException
     */
    public static ThreadPoolExecutor generateThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit timeUnit, Map configMap) {
    	if (configMap == null) {
			throw new SpongeException("hashMap 不能为null");
		}
		if (configMap.get(FILE_PERSISTENCE_DIR) == null) {
			throw new SpongeException("configMap里缺少 FILE_PERSISTENCE_DIR 设置");
		}

    	ThreadPoolExecutor tmpThreadPool;
    	try {
			String directory = (String)configMap.get(FILE_PERSISTENCE_DIR);

			int queueCapacity = configMap.get(BLOCKING_QUEUE_CAPACITY) == null ? DEFAULT_CAPACITY :
					Integer.parseInt(configMap.get(BLOCKING_QUEUE_CAPACITY).toString());

			int persistLimitSize = configMap.get(BLOCKING_QUEUE_ONCE_PERSIST_LIMIT_SIZE) == null ? DEFAULT_ONCE_PERSIST_LIMIT_SIZE :
					Integer.parseInt(configMap.get(BLOCKING_QUEUE_ONCE_PERSIST_LIMIT_SIZE).toString());

			long maxByteArraySize = configMap.get(MAX_BYTE_ARRAY_SIZE) == null ? DEFAULT_MAX_BYTE_ARRAY_SIZE :
					Long.parseLong(configMap.get(MAX_BYTE_ARRAY_SIZE).toString());

			int oneBatchWriteCnt = configMap.get(ONE_BATCH_WRITE_CNT) == null ? DEFAULT_ONE_BATCH_WRITE_CNT :
					Integer.parseInt(configMap.get(ONE_BATCH_WRITE_CNT).toString());

			int releaseResMaxTime = configMap.get(CAN_RELEASE_RES_MAX_TIME) == null ? DEFAULT_RELEASE_RES_MAX_TIME :
					Integer.parseInt(configMap.get(CAN_RELEASE_RES_MAX_TIME).toString());

    		FilePersistence filePersistence = new FilePersistence(maxByteArraySize, oneBatchWriteCnt, releaseResMaxTime, directory);
    		SpongeArrayBlockingQueue tmpMyArrayBlockingQueue =
					new SpongeArrayBlockingQueue(queueCapacity, persistLimitSize, new SpongeService(filePersistence));
    		// construct jdk ThreadPoolExecutor
    		tmpThreadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, timeUnit,
    					tmpMyArrayBlockingQueue);
    		tmpMyArrayBlockingQueue.initFetchData(tmpThreadPool);
    	} catch (Exception ex) {
			LOGGER.error("Construct SpongeThreadPoolExecutor error!", ex);
    		throw new SpongeException(ex.getLocalizedMessage());
    	}

    	return tmpThreadPool;
    }
    
    public static ThreadPoolExecutor generateThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        	TimeUnit timeUnit, HashMap configMap, Persistence persistence) {
    	int tmpCapacity = 500;
    	int tmpOnePersistLimit = 100;

    	ThreadPoolExecutor tmpThreadPool = null;
    	try {
    		if (configMap != null) {
        		String tmpCapStr = (String)configMap.get(BLOCKING_QUEUE_CAPACITY);
        		if (tmpCapStr != null) {
        			tmpCapacity = Integer.parseInt(tmpCapStr);
        		}

        		String tmpLimit = (String)configMap.get(BLOCKING_QUEUE_ONCE_PERSIST_LIMIT_SIZE);
        		if (tmpLimit != null) {
        			tmpOnePersistLimit = Integer.parseInt(tmpLimit);
        		}
        	}

    		if (persistence == null) {
    			throw new SpongeException("持久化插件不能为null");
    		}

    		SpongeService tmpSpongeService = new SpongeService(persistence);
    		SpongeArrayBlockingQueue tmpMyArrayBlockingQueue =
    				new SpongeArrayBlockingQueue(tmpCapacity, tmpOnePersistLimit, tmpSpongeService);
    		tmpThreadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, timeUnit,
    					tmpMyArrayBlockingQueue);
    		tmpMyArrayBlockingQueue.initFetchData(tmpThreadPool);
    	} catch (Exception ex) {
			LOGGER.error("Construct SpongeThreadPoolExecutor error!", ex);
    		throw new SpongeException(ex.getLocalizedMessage());
    	}

    	return tmpThreadPool;
    }
}
