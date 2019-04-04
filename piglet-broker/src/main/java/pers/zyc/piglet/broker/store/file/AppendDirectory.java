package pers.zyc.piglet.broker.store.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import pers.zyc.piglet.SystemCode;
import pers.zyc.piglet.SystemException;
import pers.zyc.tools.utils.lifecycle.Service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * @author zhangyancheng
 */
@Slf4j
public class AppendDirectory extends Service {
	private static final Pattern DIGIT_REGEX = Pattern.compile("\\d+");
	
	@Getter
	private final File directory;
	
	private final int fileLength;
	
	private final Map<Long, AppendFile> appendFileMap = new TreeMap<>();
	
	public AppendDirectory(File directory, int fileLength) {
		this.directory = directory;
		this.fileLength = fileLength;
	}
	
	@Override
	protected void beforeStart() throws Exception {
		if (!directory.exists() && !directory.mkdirs() && !directory.exists()) {
			throw new IllegalStateException("Create append directory failed, file: " + directory);
		}
	}
	
	@Override
	protected void doStart() throws Exception {
		File[] appendFiles = directory.listFiles(f -> DIGIT_REGEX.matcher(f.getName()).matches());
		int files = appendFiles == null ? 0 : appendFiles.length;
		if (files > 0) {
			// 按照id大小升序排列，保证文件顺序创建
			Arrays.sort(appendFiles, (f1, f2) -> Long.parseLong(f1.getName()) >= Long.parseLong(f2.getName()) ? 1 : -1);
			
			for (File file : appendFiles) {
				if (file.length() != fileLength + AppendFile.HEAD_LENGTH) {
					throw new SystemException("Incorrect data length, file: " + file.getPath(),
							SystemCode.STORE_BAD_FILE.getCode());
				}
				long fileId = Long.parseLong(file.getName());
				appendFileMap.put(fileId, createFile(fileId));
			}
		}
		log.info("Open {}, load {} append file.", directory.getPath(), files);
	}
	
	@Override
	protected void doStop() throws Exception {
		appendFileMap.values().forEach(AppendFile::close);
		appendFileMap.clear();
	}
	
	private AppendFile createFile(long id) throws IOException {
		return new AppendFile(id, fileLength, this);
	}
}
