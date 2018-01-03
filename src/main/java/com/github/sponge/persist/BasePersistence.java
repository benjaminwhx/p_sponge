package com.github.sponge.persist;

import java.util.ArrayList;

import com.github.sponge.exception.SpongeException;
import com.github.sponge.util.DataByteArrayOutputStream;
import com.github.sponge.util.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: benjamin.wuhaixu
 * Date: 2018-01-02
 * Time: 9:23 pm
 */
public abstract class BasePersistence implements Persistence {
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	/**
	 * 最大允许的内存数,单位byte 默认50M
	 */
	private long maxByteArraySz = 50 * 1024 * 1024;
	private ArrayList<byte[]> inMemoryDataList = new ArrayList();
	private int curInMemorySz = 0;
	private final Object listMutex = new Object();
	private final Object writeAndReadMutex = new Object();
	private Thread thread;
	private boolean haveDataInPersistence = false;
	private DataByteArrayOutputStream theOutBytes = null;
	private int oneBatchWriteCnt = 20;
	private long canReleaseResTime = -1;
	private long canReleaseResMaxTime = 60 * 1000;
	private int writeOffset = 0;
	
	/**
	 * 
	 * @param maxByteArraySz         最大允许的内存数,单位byte
	 * @param oneBatchWriteCnt        一次序列化批量处理的byte[]的个数
	 * @param canReleaseResMaxTime  如果连续等待这么长时间还没有任何持久化的读、写操作，
	 *                                    则删除相关资源，如保存序列化的文件。
	 * @throws Exception
	 */
	public BasePersistence(long maxByteArraySz,
			int oneBatchWriteCnt, int canReleaseResMaxTime) throws Exception {
		this.maxByteArraySz = maxByteArraySz;
		this.oneBatchWriteCnt = oneBatchWriteCnt;
		this.canReleaseResMaxTime = canReleaseResMaxTime;
		theOutBytes = new DataByteArrayOutputStream(1 * 1024 * 1024);
		
		thread = new Thread() {
            public void run() {
                processQueue();
            }
        };
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.setDaemon(true);
        thread.setName("Sponge Data File Writer");
        thread.start();
	}
	
	@Override
	public boolean persistData(byte[] bytes) {
		boolean retBool = false;

		if (canReleaseResTime != -1) {
        	if ((System.currentTimeMillis() - canReleaseResTime) > canReleaseResMaxTime) {
        		try {
        			releaseResource();
        		} catch (Exception ex) {
        			LOGGER.error("persist Data release res failed.");
        		}
        		canReleaseResTime = -1;
        	}
        }
		
		if (curInMemorySz + bytes.length <= maxByteArraySz) {
			retBool = true;
			synchronized (listMutex) {
				inMemoryDataList.add(bytes);
				listMutex.notifyAll();
			}
			
			curInMemorySz += bytes.length;
		}
		else {
			System.out.println("已经达到缓冲器系统处理上线,丢弃此次数据,数据大小 "+ bytes.length
					+"。原因是磁盘IO资源不足，请确认!!!");
		}
		
		return retBool;
	}

	protected void processQueue() {
		byte[] tmpBytes = null;
        try {
            for (;;) {
                // Block till we get a command.
                synchronized (listMutex) {
                    for (;;) {
                    	int tmpListSz = inMemoryDataList.size();
                        if (tmpListSz > 0) {
                        	int tmpThisTimeSaveCnt = oneBatchWriteCnt;
                        	if (tmpListSz < oneBatchWriteCnt) {
                        		tmpThisTimeSaveCnt = tmpListSz;
                        	}
                        	
                        	for (int i = 0; i < tmpThisTimeSaveCnt; i++) {
                        		tmpBytes = inMemoryDataList.remove(0);
                        		curInMemorySz -= tmpBytes.length;
                        		theOutBytes.write(tmpBytes);
                        	}
                            break;
                        }
                        listMutex.wait();
                    }
                    listMutex.notifyAll();
                }
                
                if (theOutBytes.size() > 0) {
                	synchronized(writeAndReadMutex) {
                		doWriteOneBatchBytes(theOutBytes.getData(), writeOffset, theOutBytes.size());
                		theOutBytes.reset();
                	}
                }
            }
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
            try {
            	destroy();
            } catch (Throwable ignore) {
            }
        }
    }
	
	@Override
	public byte[] fetchDataFromPersistence() throws SpongeException {
		byte[] retBytes = null;
		try {
			synchronized(writeAndReadMutex) {
				retBytes = doFetchOneBatchBytes();
				
				if (retBytes == null) {
					canReleaseResTime = System.currentTimeMillis();
				} else {
					canReleaseResTime = -1;
				}
				
				if (retBytes == null) {
					if (theOutBytes.size() > 0) {
						int tmpByteLength = Utilities.getIntFromBytes(theOutBytes.getData(), writeOffset + 2);
						byte[] tmpReadBytes = new byte[tmpByteLength];
						
						System.arraycopy(theOutBytes.getData(), writeOffset, tmpReadBytes, 0, tmpByteLength);
						retBytes = tmpReadBytes;
						
						writeOffset += tmpByteLength;
					}
				}
				
				if (retBytes == null) {
					synchronized (listMutex) {
						if (inMemoryDataList.size() > 0) {
							retBytes = inMemoryDataList.remove(0);
							curInMemorySz -= retBytes.length;
						}
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new SpongeException(ex.getMessage());
		}
		
		return retBytes;
	}

	public abstract byte[] doFetchOneBatchBytes() throws Exception;
	public abstract void doWriteOneBatchBytes(byte[] writeBytes, int offset, int length) throws Exception;
	public abstract void doWriteOneBatchBytes(byte[] writeBytes) throws Exception;
	public abstract void destroy() throws Exception;
	public abstract void releaseResource() throws Exception;

	@Override
	public boolean haveDataInPersistence() {
		return haveDataInPersistence;
	}

	public void setHaveDataInPersistence(boolean haveDataInPersistence) {
		this.haveDataInPersistence = haveDataInPersistence;
	}
}
