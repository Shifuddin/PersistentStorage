/**
 * Front-end for ECS server administrator
 * @author shifuddin, Ilya*/
package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

public class ECSClient {

	private Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	private boolean stop = false;
	private int numberOfNodes;
	private int cacheSize;
	private String displacementStrategy;
	private ECS ecs;
	private int invokeResult;
	private String invokeMessage;

	/**
	 * Loop for reading/writing shell commands Exits loop when quitting or
	 * having errors thrown
	 * 
	 * @throws InterruptedException
	 */
	public void run() throws InterruptedException {
		try {
			ecs = new ECS("ecs.config");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			logger.error("Config file not found");
			return;
		}
		while (!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	/**
	 * Handles shell input, dividing it into tokens, called from the run() loop.
	 * 
	 * @param cmdLine
	 *            command input
	 * @throws IOException
	 *             any System IO exceptions
	 * @throws InterruptedException
	 */
	private void handleCommand(String cmdLine) throws IOException, InterruptedException {
		String[] tokens = cmdLine.split("\\s+");

		if (tokens[0].equals("shutdown")) {
			if (tokens.length == 1) {
				logger.info(ecs.shutdown());
				System.exit(0);
			} else
				logger.info("Too many parameters");

		} else if (tokens[0].equals("init")) {
			if (tokens.length == 4) {
				try {
					numberOfNodes = Integer.parseInt(tokens[1]);
					cacheSize = Integer.parseInt(tokens[2]);
					displacementStrategy = tokens[3];
					invokeResult = ecs.initService(numberOfNodes, cacheSize, displacementStrategy);
					if (invokeResult == 0)
						logger.info("Service initialized");
					else if (invokeResult == 1)
						logger.info("Service already initialized. Type start");
					else if (invokeResult == 2)
						logger.info("Service started. Stop first");
					else if (invokeResult == -2)
						logger.info("Not supported displacement Strategy");
					else
						logger.info("Could not initialize service");
				} catch (NumberFormatException nfe) {
					printError("Not valid numberofnodes or cache size!");
					logger.info("Unable to parse argument <numberofnodes> <cachesize>", nfe);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					printError(e.getMessage());
					logger.warn("Error: " + e.getMessage());
				}
			} else {
				printError("Invalid number of parameters!");
				logger.warn("Invalid number of parameters!");
			}

		} else if (tokens[0].equals("start")) {

			if (tokens.length == 1) {
				logger.info(ecs.start());
			} else {
				printError("More parameters");
			}

		} else if (tokens[0].equals("stop")) {

			if (tokens.length == 1) {
				logger.info(ecs.stop());
			} else {
				printError("More parameters");
			}
		} else if (tokens[0].equals("add")) {

			if (tokens.length == 3) {
				try {

					invokeMessage = ecs.addNode(Integer.parseInt(tokens[1]), tokens[2]);
					logger.info(invokeMessage);

				} catch (NumberFormatException nfe) {
					printError("Not valid cache size!");
					logger.info("Unable to parse argument <cachesize>", nfe);
				} catch (FileNotFoundException fne) {
					printError("File not found!");
					logger.info("Config file not found", fne);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					printError(e.getMessage());
					logger.warn("Error: " + e.getMessage());
				}
			} else {
				printError("Invalid Number of Parametes");
			}
		}

		else if (tokens[0].equals("remove")) {

			if (tokens.length == 1) {
				invokeMessage = ecs.removeNode();
				logger.info(invokeMessage);
			} else {
				printError("Invalid Number of Parametes");
			}
		} else if (tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			logger.error("Error: \t Unknown command!");
			printHelp();
		}
	}

	/**
	 * Error output with shell prompt
	 * 
	 * @param error
	 *            thrown message
	 */
	private void printError(String error) {
		System.out.println(PROMPT + "Error! " + error);
	}

	/**
	 * Prints all commands and their respective variables, called is command was
	 * unknown, or if it was "help"
	 */
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("init <numberOfNodes> <cacheSize> <displacementStrategy>\n");
		sb.append(PROMPT).append(
				"\t\t\t\t This call launches the server with the specified cache size and displacement strategy.\n");
		sb.append(PROMPT).append("start");
		sb.append(
				"\t\t\t Starts the storage service by calling start() on all KVServer instances that participate in the service \n");
		sb.append(PROMPT).append("stop");
		sb.append(
				"\t\t\t\t Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running. \n");
		sb.append(PROMPT).append("add");
		sb.append(PROMPT).append("add <cacheSize> <displacementStrategy>\n");
		sb.append(PROMPT).append(
				"\t\t\t\t Create a new KVServer with the specified cache size and displacement strategy and add it to the storage service at an arbitrary position \n");
		sb.append(PROMPT).append("remove");
		sb.append("\t\t\t Remove a node from the storage service at an arbitrary position\n");
		sb.append(PROMPT).append("shutdown");
		sb.append("\t\t\t Stops all server instances and exits the remote processes. \n");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		new LogSetup("logs/ecs/ecs.log", Level.ALL);
		ECSClient client = new ECSClient();
		client.run();

	}

}
