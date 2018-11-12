package de.hpi.akka_assignment.actors;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.akka_assignment.AkkaAssignmentMaster;
import de.hpi.akka_assignment.actors.Master.CompletionMessage;
import de.hpi.akka_assignment.actors.Master.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @SuppressWarnings("unused")
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = -7643194361868862395L;
		public WorkMessage(type workType, Object payload) {
			this.messageType = workType;
			this.payload = payload;
		}
		public Object payload;
		public enum type {PASSWORD_HASH}
		public type messageType;

		public type getMessageType() {
			return messageType;
		}
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(WorkMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if (member.hasRole(AkkaAssignmentMaster.MASTER_ROLE))
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new RegistrationMessage(), this.self());
	}

	private void handle(WorkMessage message) {
		
		this.log.info("work message received");

		switch (message.getMessageType()) {
			case PASSWORD_HASH:
				this.log.info("Do a password hash" + message.payload.toString());
				break;
		}
		
		this.sender().tell(new CompletionMessage(CompletionMessage.status.EXTENDABLE), this.self());
	}
}