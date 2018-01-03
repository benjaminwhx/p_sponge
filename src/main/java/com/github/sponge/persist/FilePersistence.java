package com.github.sponge.persist;

import java.io.File;
import java.io.RandomAccessFile;
import com.github.sponge.util.RAcsFile;
import com.github.sponge.util.Utilities;

public class FilePersistence extends BasePersistence {
	private String directory;
	private RAcsFile writeDataFile;
	private RAcsFile readDataFile;
	private RAcsFile fetchPositionFile;
	private boolean forceToDisk = true;
	private long curFetchPosition;
	private final static String DATA_FILE_NAME = "dataFile.data";
	private final static String FETCH_POSITION_FILE_DATA = "fetchPositionFile.data";
	
	public FilePersistence(long maxByteArraySize,
			int oneBatchWriteCnt, int canReleaseResMaxTime,
			String directory) throws Exception {
		super(maxByteArraySize, oneBatchWriteCnt, canReleaseResMaxTime);
		this.directory = directory;
		if (!directory.endsWith("/")) {
			this.directory = this.directory + "/";
			//throw new SpongeException("文件目录设置不对,必须以'/'结尾 " + directory);
		}
		writeDataFile = new RAcsFile(this.directory + DATA_FILE_NAME);
		readDataFile = new RAcsFile(this.directory + DATA_FILE_NAME, "r");
		fetchPositionFile = new RAcsFile(this.directory + FETCH_POSITION_FILE_DATA);
		initCurFetchPosi();
		writeDataFile.getDataFile().seek(writeDataFile.getFileLength());
		//readOneBatch_MaxBytes = new byte[readOneBatch_MaxByteSz];
	}
	
	private void initCurFetchPosi() {
		try {
			if (fetchPositionFile.getFileLength() == 8) {
				curFetchPosition = fetchPositionFile.getDataFile().readLong();
			}
			
			readDataFile.getDataFile().seek(curFetchPosition);
			
			if (curFetchPosition < readDataFile.getFileLength()) {
				setHaveDataInPersistence(true);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	@Override
	public byte[] doFetchOneBatchBytes() throws Exception {
		byte[] tmpLengthByte = new byte[6];
		int tmpReadCnt = readDataFile.getDataFile().read(tmpLengthByte);
		if (tmpReadCnt == -1) {
			return null;
		}
		
		int tmpByteLength = Utilities.getIntFromBytes(tmpLengthByte, 2);
		byte[] tmpReadBytes = new byte[tmpByteLength];
		
		tmpReadCnt = readDataFile.getDataFile().read(tmpReadBytes, 6, tmpByteLength - 6);
		if (tmpReadCnt == -1) {
			return null;
		} else {
			curFetchPosition += tmpByteLength;
			byte[] tmpBytes = Utilities.getBytesFromLong(curFetchPosition);
			fetchPositionFile.getDataFile().seek(0);
			fetchPositionFile.getDataFile().write(tmpBytes);
			fetchPositionFile.getDataFile().getFD().sync();
			return tmpReadBytes;
		}
	}

	@Override
	public void doWriteOneBatchBytes(byte[] writeBytes, int offset, int length) throws Exception {
		long tmpStartTime = System.currentTimeMillis();
		
		RandomAccessFile file = writeDataFile.getDataFile();
		file.write(writeBytes, offset, length);
        
        if (forceToDisk) {
            file.getFD().sync();
        }
        
        LOGGER.info("一次写入耗时{}ms", System.currentTimeMillis() - tmpStartTime);
	}

	@Override
	public void destroy() throws Exception {
		writeDataFile.close();
		readDataFile.close();
	}

	@Override
	public void doWriteOneBatchBytes(byte[] writeBytes) throws Exception {
		doWriteOneBatchBytes(writeBytes, 0, writeBytes.length);
	}

	@Override
	public void releaseResource() throws Exception {
		if (curFetchPosition != 0) {
			writeDataFile.destroy();
			readDataFile.destroy();
			
			writeDataFile = null;
			readDataFile = null;
			
			File tmpFile = new File(directory + DATA_FILE_NAME);
			deleteFile(tmpFile);
			
			curFetchPosition = 0;
			fetchPositionFile.getDataFile().seek(0);
			fetchPositionFile.getDataFile().write(Utilities.getBytesFromLong(curFetchPosition));
			fetchPositionFile.getDataFile().getFD().sync();
			
			writeDataFile = new RAcsFile(directory + DATA_FILE_NAME);
			readDataFile = new RAcsFile(directory + DATA_FILE_NAME, "r");
		}
	}
	
	private boolean deleteFile(File fileToDelete) {
        if (fileToDelete == null || !fileToDelete.exists()) {
            return true;
        }
        return fileToDelete.delete();
    }
}
