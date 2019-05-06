package pers.zyc.piglet.admin.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import pers.zyc.piglet.IOExecutor;
import pers.zyc.tools.utils.IPUtil;
import pers.zyc.tools.utils.lifecycle.Service;

import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.SocketException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author zhangyancheng
 */
@Slf4j
public class EmbeddedZKServer extends Service {

	private static final String DEFAULT_DATA_DIR = System.getProperty("user.home") + "/zkData";
	private long myId;
	private QuorumPeerConfig config;
	private Thread starter;

	@Override
	protected void beforeStart() throws Exception {
		InputStream confInputStream = getClass().getClassLoader().getResourceAsStream("/conf/zoo.cfg");
		if (confInputStream == null) {
			throw new NullPointerException();
		}

		Properties properties = new Properties();
		properties.load(confInputStream);
		config = new QuorumPeerConfig();
		config.parseProperties(properties);

		myId = getMyId(properties);
		// 根据本机ip识别出server id后写入myId文件
		if (myId > 0) {
			File dataDir = new File(properties.getProperty("dataDir", DEFAULT_DATA_DIR));
			if (!dataDir.exists() && !dataDir.mkdirs()) {
				throw new IllegalStateException("Create dataDir failed, file: " + dataDir);
			}
			try (RandomAccessFile raf = new RandomAccessFile(new File(dataDir, "myId"), "rw")) {
				raf.setLength(0);// 清除原数据
				raf.write(String.valueOf(myId).getBytes());
				raf.getChannel().force(false);
			}
		}
	}

	private static long getMyId(Properties zkProperties) throws SocketException {
		long myId = 0;
		Set<String> allLocalIP = IPUtil.getAllLocalIP();
		for (Map.Entry<Object, Object> entry : zkProperties.entrySet()) {
			String key = entry.getKey().toString().trim();
			String value = entry.getValue().toString().trim();
			if (key.startsWith("server.") && allLocalIP.contains(value.substring(0, value.indexOf(":")))) {
				myId = Long.parseLong(key.substring(7));
				break;
			}
		}
		return myId;
	}

	private static void startPurgeMgr(QuorumPeerConfig config) {
		// Start and schedule the the purge task
		DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
				.getDataDir(), config.getDataLogDir(), config
				.getSnapRetainCount(), config.getPurgeInterval());
		purgeMgr.start();
	}

	@Override
	protected void doStart() throws Exception {
		starter = new Thread(() -> {
			startPurgeMgr(config);
			IOExecutor.execute(() -> {
				if (myId > 0) {
					new QuorumPeerMain().runFromConfig(config);
				} else {
					ServerConfig serverConfig = new ServerConfig();
					serverConfig.readFrom(config);
					new ZooKeeperServerMain().runFromConfig(serverConfig);
				}
			});
		});
		starter.setName("Embedded ZK Starter");
		starter.start();
	}

	@Override
	protected void doStop() throws Exception {
		starter.interrupt();
	}
}
