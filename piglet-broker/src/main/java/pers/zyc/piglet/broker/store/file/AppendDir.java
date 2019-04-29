package pers.zyc.piglet.broker.store.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * 顺序写目录，包含多个顺序写文件
 *
 * @author zhangyancheng
 */
@Slf4j
public class AppendDir implements Closeable {
	private static final Pattern DIGIT_REGEX = Pattern.compile("\\d+");
	
	@Getter
	private final File directory;

	/**
	 * 文件长度
	 */
	private final int fileLength;

	/**
	 * 顺序写文件集合
	 */
	private final TreeMap<Long, AppendFile> appendFileMap = new TreeMap<>();
	
	public AppendDir(File directory, int fileLength) {
		this.directory = directory;
		this.fileLength = fileLength;
		if (!directory.exists() && !directory.mkdir()) {
			throw new IllegalStateException("Create append directory failed, directory: " + directory);
		}
		File[] appendFiles = directory.listFiles(f -> DIGIT_REGEX.matcher(f.getName()).matches());
		int files = appendFiles == null ? 0 : appendFiles.length;
		if (files > 0) {
			// 按照id大小升序排列，保证文件顺序创建
			Arrays.sort(appendFiles, (f1, f2) -> Long.parseLong(f1.getName()) >= Long.parseLong(f2.getName()) ? 1 : -1);

			for (File file : appendFiles) {
				if (file.length() != fileLength + AppendFile.HEAD_LENGTH) {
					throw new SystemException(SystemCode.STORE_BAD_FILE.getCode(), "Incorrect data length, file: " +
							file.getPath());
				}
				createFile(Long.parseLong(file.getName()));
			}
		} else {
			createFile(0);// 初始化第一个文件
		}
		log.info("Open {}, load {} append file.", directory.getPath(), files);
	}

	@Override
	public void close() {
		appendFileMap.values().forEach(this::closeFile);
		appendFileMap.clear();
	}

	private void closeFile(AppendFile appendFile) {
		try {
			appendFile.close();
		} catch (IOException e) {
			log.warn("Append file close error: {}, file: {}", e.getMessage(), appendFile.getFile());
		}
	}

	/**
	 * @return 目下所有的顺序写文件
	 */
	public AppendFile[] getAllFiles() {
		AppendFile[] files = new AppendFile[appendFileMap.size()];
		appendFileMap.values().toArray(files);
		return files;
	}

	/**
	 * 根据id创建顺序写文件
	 *
	 * @param fileId 文件id
	 * @return 新创建的文件
	 */
	private AppendFile createFile(long fileId) {
		try {
			AppendFile af = new AppendFile(fileId, fileLength, this);
			appendFileMap.put(fileId, af);
			return af;
		} catch (IOException e) {
			throw new SystemException(SystemCode.IO_EXCEPTION, e);
		}
	}

	/**
	 * 创建下一个新文件
	 *
	 * @return 新创建的文件
	 */
	public AppendFile createNewFile() {
		return createFile(appendFileMap.isEmpty() ? 0 : appendFileMap.lastKey() + fileLength);
	}

	/**
	 * @return 获取最后一个文件
	 */
	public AppendFile getLastFile() {
		if (appendFileMap.isEmpty()) {
			return createFile(0);
		}
		return appendFileMap.lastEntry().getValue();
	}

	/**
	 * 整个目录按照指定偏移量截断
	 *
	 * @param offset 截断位置
	 */
	public void truncate(long offset) {
		for (AppendFile file : appendFileMap.values()) {
			if (file.getMaxOffset() > offset) {
				offset = file.getId() >= offset ? file.getId() : offset;
				try {
					file.truncate(offset);
				} catch (IOException e) {
					throw new SystemException(SystemCode.IO_EXCEPTION, e);
				}
			}
		}
	}
}
