/**Server class to create a server with cache size, and displacement strategy that runs on a specific port
 * @author shifuddin, Ilya*/
package app_kvServer;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVConsistentHash;
import common.messages.KVLevel;
import logger.LogSetup;

public class KVServer {

	private int port;
	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;
	private boolean running;
	private KVConsistentHash<String> metadata;
	private CacheInterface cache;
	private boolean serverState = false;
	private Socket client = null;
	private static String jarDir;

	public KVServer(int port) {
		this.port = port;
		run();
	}

	public void start() {

		serverState = true;
	}

	public void stop() {
		serverState = false;
	}

	public void setCache(CacheInterface cache) {
		this.cache = cache;
	}

	public CacheInterface getCache() {
		return cache;
	}

	public void setMetaData(KVConsistentHash<String> metadata) {
		this.metadata = metadata;
	}

	public KVConsistentHash<String> getMetaData() {
		return metadata;
	}
	
	/**Initialize KVServer
	 * Instantiate persistent storage
	 * Waits for client connection
	 * After accepting a client connection, runs each connection in different thread with persistent storage, null cache and metadata 
	 * @return Void
	 * */
	public void run() {
		running = initializeServer();

		cache = null;
		metadata = null;
		
		// Persistent storage
		DBFileInterface store = new DBFile(jarDir, "kv");
		DBFileInterface suc1 = new DBFile(jarDir, "suc1");
		DBFileInterface suc2 = new DBFile(jarDir, "suc2");
		DBFileInterface client_level = new DBFile(jarDir, "client_level");
		DBFileInterface notificationBuffer = new DBFile(jarDir, "notification_buffer");
		
		
		if (serverSocket != null) {

			while (isRunning()) {
				try {

					client = serverSocket.accept();

					// create and start connection
					ClientConnection connection = new ClientConnection(metadata, client, cache, this, store, suc1, suc2,
							client_level, new KVLevel(), notificationBuffer);
					new Thread(connection).start();

					logger.info(
							"Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	/**
	 * Check server running or not
	 * 
	 * @return running
	 */
	private boolean isRunning() {
		return this.running;
	}

	/**
	 * Create server socket on given port
	 * 
	 * @return true or false
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " + serverSocket.getLocalPort() + " address "
					+ serverSocket.getLocalSocketAddress());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	public boolean getServerState() {
		return serverState;
	}

	/**
	 * Close client connection and stop server being running
	 * */
	public void shutdown() {
		try {
			if (client != null)
				client.close();
			running = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		/* Invoke jar directory where it runs */
		CodeSource codeSource = KVServer.class.getProtectionDomain().getCodeSource();
		File jarFile = new File(codeSource.getLocation().toURI().getPath());
		jarDir = jarFile.getParentFile().getPath();

		/* Create log file in the jar directory */
		new LogSetup(jarDir + "/logs/server/server.log", Level.ALL);
		if (args.length == 1) {
			try {

				int port = Integer.parseInt(args[0]);
				new KVServer(port);

			} catch (NumberFormatException nfe) {
				logger.error("Invalid parameter " + nfe);
			}
		} else {
			logger.error("Invalid number of argument");
		}
	}
}
