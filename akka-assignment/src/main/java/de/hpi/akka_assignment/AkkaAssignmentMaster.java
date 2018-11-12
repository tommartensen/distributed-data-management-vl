package de.hpi.akka_assignment;

import java.util.Scanner;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.akka_assignment.actors.Master;
import de.hpi.akka_assignment.actors.Worker;
import de.hpi.akka_assignment.actors.listeners.ClusterListener;

public class AkkaAssignmentMaster extends AkkaAssignmentSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port, int slaves, String filePath) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				system.actorOf(Master.props(slaves, filePath), Master.DEFAULT_NAME);
				
				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
			}
		});
		

		system.actorSelection("/user/" + Master.DEFAULT_NAME).tell(new Master.TaskMessage(1), ActorRef.noSender());
	}
}
