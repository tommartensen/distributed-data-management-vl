package de.hpi.akka_assignment.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.akka_assignment.actors.Worker.WorkMessage;
import de.hpi.akka_assignment.util.DataLoader;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Master extends AbstractActor {

	private int numSlaves;
	private Map<String, List<Object>> students;

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(int numSlaves, String filePath) {
		return Props.create(Master.class, () -> new Master(numSlaves, filePath));
	}

	public Master(int numSlaves, String filePath) {
		this.numSlaves = numSlaves;
		this.students = DataLoader.loadTable(filePath);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 4545299661052078209L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = -8330958742629706627L;
		private TaskMessage() {}
		private int attributes;

		public int getAttributes() {
			return attributes;
		}
	}
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		public enum status {MINIMAL, EXTENDABLE, FALSE, FAILED}
		private CompletionMessage() {}
		private status result;

		public status getResult() {
			return result;
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

	private TaskMessage task;

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(TaskMessage.class, this::handle)
				.match(CompletionMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		
		this.assign(this.sender());
		this.log.info("Registered {}", this.sender());
	}

	private void handle(TaskMessage message) {
		this.log.info("Received taskmessage");
		this.assign(new WorkMessage(WorkMessage.type.PASSWORD_HASH, students.get("passwordHashes").get(0)));
	}
	
	private void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			WorkMessage work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}		
		this.log.info("Unregistered {}", message.getActor());
	}
	
	private void handle(CompletionMessage message) {
		ActorRef worker = this.sender();
		WorkMessage work = this.busyWorkers.remove(worker);

		this.log.info(work.toString());
		
		this.log.info("completed" + message.getResult());
		
		this.assign(worker);
	}
	
	private void assign(WorkMessage work) {
		ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	private void assign(ActorRef worker) {
		WorkMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
}