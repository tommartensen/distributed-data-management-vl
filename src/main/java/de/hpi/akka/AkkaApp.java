package de.hpi.akka;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

public class AkkaApp {

	public static final String ACTOR_SYSTEM_NAME = "akka";
	
	public static void main(String[] args) {

    	MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
        	.addCommand(AkkaMasterSystem.MASTER_ROLE, masterCommand)
            .addCommand(AkkaSlaveSystem.SLAVE_ROLE, slaveCommand)
            .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            switch (jCommander.getParsedCommand()) {
                case AkkaMasterSystem.MASTER_ROLE:
                    AkkaMasterSystem.start(ACTOR_SYSTEM_NAME, masterCommand.host, masterCommand.port, masterCommand.workers, masterCommand.slaves, masterCommand.filePath);
                    break;
                case AkkaSlaveSystem.SLAVE_ROLE:
                    AkkaSlaveSystem.start(ACTOR_SYSTEM_NAME, slaveCommand.workers, slaveCommand.host, slaveCommand.port, slaveCommand.masterHost, slaveCommand.masterPort);
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
    	
    	String getDefaultHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }

        String host = this.getDefaultHost();

    	@Parameter(names = {"-p", "--port"}, description = "port to start system on", required = false)
        int port = this.getDefaultPort();

        abstract int getDefaultPort();

    	@Parameter(names = {"-w", "--workers"}, description = "number of workers to start locally", required = false)
        int workers = DEFAULT_WORKERS;
    }

    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {
	    public static final int DEFAULT_SLAVES = 4;

        @Parameter(names = {"--slaves"}, description = "number of slaves to wait for", required = false)
        int slaves = DEFAULT_SLAVES;

        @Parameter(names = {"--input"}, description = "location of input file", required = false)
        String filePath;

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

        String host = this.getDefaultHost();
        
        int masterPort = DEFAULT_MASTER_PORT;

        @Parameter(names = {"-h", "--host"}, description = "this machine's host name or IP to bind against", required = true)
        String masterHost;
    }
}
