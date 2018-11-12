package de.hpi.akka_assignment;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

public class AkkaAssignmentApp {

	public static final String ACTOR_SYSTEM_NAME = "akka_assignment";
	
	public static void main(String[] args) {

    	MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
        	.addCommand(AkkaAssignmentMaster.MASTER_ROLE, masterCommand)
            .addCommand(AkkaAssignmentSlave.SLAVE_ROLE, slaveCommand)
            .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            switch (jCommander.getParsedCommand()) {
                case AkkaAssignmentMaster.MASTER_ROLE:
                    AkkaAssignmentMaster.start(ACTOR_SYSTEM_NAME, masterCommand.workers, masterCommand.host, masterCommand.port, masterCommand.slaves, masterCommand.filePath);
                    break;
                case AkkaAssignmentSlave.SLAVE_ROLE:
                    AkkaAssignmentSlave.start(ACTOR_SYSTEM_NAME, slaveCommand.workers, slaveCommand.host, slaveCommand.port, slaveCommand.masterHost, slaveCommand.masterPort);
                    break;
                default:
                    throw new AssertionError();
            }

        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (jCommander.getParsedCommand() == null) {
                jCommander.usage();
            } else {
                jCommander.usage(jCommander.getParsedCommand());
            }
            System.exit(1);
        }
	}

    abstract static class CommandBase {

    	public static final int DEFAULT_MASTER_PORT = 7877;
    	public static final int DEFAULT_SLAVE_PORT = 7879;
        public static final int DEFAULT_WORKERS = 4;
        public static final int DEFAULT_SLAVES = 4;
        public static final String DEFAULT_FILE_PATH = "src/resources/students.csv";

        String host = this.getDefaultHost();

        String getDefaultHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }

        int port = this.getDefaultPort();

        abstract int getDefaultPort();

    	@Parameter(names = {"-w", "--workers"}, description = "number of workers to start locally", required = false)
        int workers = DEFAULT_WORKERS;
	}

    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {
	    @Parameter(names = {"-i", "--input"}, description = "input file location", required = true)
        String filePath = DEFAULT_FILE_PATH;

        @Parameter(names = {"-s", "--slaves"}, description = "number of slaves to wait for", required = true)
        int slaves = DEFAULT_SLAVES;

        @Override
        int getDefaultPort() {
            return DEFAULT_MASTER_PORT;
        }
    }

    @Parameters(commandDescription = "start a slave actor system")
    static class SlaveCommand extends CommandBase {

        @Override
        int getDefaultPort() {
            return DEFAULT_SLAVE_PORT;
        }
        
        @Parameter(names = {"-p", "--port"}, description = "port of the master", required = false)
        int masterPort = DEFAULT_MASTER_PORT;

        @Parameter(names = {"-h", "--host"}, description = "host name or IP of the master", required = true)
        String masterHost;
    }
}
