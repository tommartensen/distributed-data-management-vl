package de.hpi.akka;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.akka.actors.*;
import de.hpi.akka.actors.listeners.ClusterListener;

/*
* Taken from Octopus example. Added reaper and own workers as well as adapted TaskMessage.
 */
public class AkkaMasterSystem extends AkkaBaseSystem {
	
	public static final String MASTER_ROLE = "master";
	public static void start(String actorSystemName, String host, int port, int workers, int slaves, String filePath) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
				system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
				system.actorOf(Master.props(), Master.DEFAULT_NAME);

				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);

				system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.TaskMessage(slaves, filePath), ActorRef.noSender());
			}
		});
	}
}
