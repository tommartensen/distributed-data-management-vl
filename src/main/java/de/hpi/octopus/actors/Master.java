package de.hpi.octopus.actors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Master extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	private List<String> names = new ArrayList<>();
	private List<String> passwordHashes = new ArrayList<>();
	private List<String> sequencedGenes = new ArrayList<>();

	public static Props props() {
		return Props.create(Master.class);
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
		public int slaves;
		public String filePath;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class RegisterSlaveMessage implements Serializable {
		private static final long serialVersionUID = -7472726661953028641L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PasswordCompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		public enum status {DONE, FAILED}
		private PasswordCompletionMessage() {}
		public PasswordCompletionMessage(status result) {
			this.result = result;
		}
		public status result;
		public int id;
		public int password;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PrefixCompletionMessage implements Serializable {
		private static final long serialVersionUID = 5933119973166777442L;
		public enum status {DONE, FAILED}
		private PrefixCompletionMessage() {}
		public PrefixCompletionMessage(status result) {
			this.result = result;
		}
		public status result;
		public List<Integer> prefixes;
	}

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PartnerCompletionMessage implements Serializable {
        private static final long serialVersionUID = 8423520414177733108L;
        public enum status {DONE, FAILED}
        private PartnerCompletionMessage() {}
        public PartnerCompletionMessage(status result) {
            this.result = result;
        }
        public status result;
        public int id;
        public int partner;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class HashMiningCompletionMessage implements Serializable {
        private static final long serialVersionUID = 6969909201709811241L;
        public enum status {DONE, FAILED}
        private HashMiningCompletionMessage() {}
        public HashMiningCompletionMessage(status result) {
            this.result = result;
        }
        public status result;
        public int id;
        public String hash;
    }
	
	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private final Queue<Worker.WorkMessage> unassignedWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, Worker.WorkMessage> busyWorkers = new HashMap<>();

	private TaskMessage task;
	private int numberSlaves = 0;
	private int desiredNumberSlaves;
	private boolean areDonePrefixes = false;
	private boolean areDonePartners = false;
	private boolean isStartedHashing = false;
	private boolean doneWithAllTasks = false;
	private String filePath;

	/////////////
    // Results //
    /////////////
	private Map<Integer, Integer> crackedPasswords = new HashMap<>();
	private List<Integer> prefixes;
    private Map<Integer, Integer> partners = new HashMap<>();
    private Map<Integer, String> partnerHashes = new HashMap<>();
    long startTime;

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(TaskMessage.class, this::handle)
				.match(RegisterSlaveMessage.class, this::handle)
				.match(PasswordCompletionMessage.class, this::handle)
				.match(PrefixCompletionMessage.class, this::handle)
                .match(PartnerCompletionMessage.class, this::handle)
                .match(HashMiningCompletionMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

    private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		
		this.assign(this.sender());
		this.log.info("Registered {}", this.sender());
	}

	private void handle(RegisterSlaveMessage message) {
		this.numberSlaves++;
		if (this.numberSlaves == this.desiredNumberSlaves) {
			kickOff();
		}
	}
	
	private void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			Worker.WorkMessage work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}		
		this.log.info("Unregistered {}", message.getActor());
	}
	
	private void handle(TaskMessage message) {
		if (this.task != null)
			this.log.error("The profiler actor can process only one task in its current implementation!");

		this.desiredNumberSlaves = message.slaves;
		this.filePath = message.filePath;
		this.task = message;
		if (this.desiredNumberSlaves == 0) {
			kickOff();
		}
	}
	
	private void handle(PasswordCompletionMessage message) {
		ActorRef worker = this.sender();
		Worker.WorkMessage work = this.busyWorkers.remove(worker);

		switch (message.result) {
			case DONE:
				this.crackedPasswords.put(message.id, message.password);
				break;
			case FAILED:
				this.assign(work);
				break;
		}
		if (this.crackedPasswords.size() < 42) {
            this.assign(worker);
        } else {
		    this.idleWorkers.add(worker);
            this.startPrefixes();
        }
	}

	private void handle(PrefixCompletionMessage message) {
		ActorRef worker = this.sender();
		this.busyWorkers.remove(worker);

		switch (message.result) {
			case DONE:
			    if (!this.areDonePrefixes) {
                    this.areDonePrefixes = true;
                    this.idleWorkers.add(worker);
                    this.prefixes = message.prefixes;
                    if (this.areDonePartners && !this.isStartedHashing) {
                        this.startHashing();
                    }
                }
                this.assign(worker);
				break;
			case FAILED:
                this.assign(worker);
				break;
		}
	}

	private void handle(PartnerCompletionMessage message) {
        ActorRef worker = this.sender();
        this.busyWorkers.remove(worker);

        switch (message.result) {
            case DONE:
                this.partners.put(message.id, message.partner);
                break;
        }

        if (this.partners.size() < 42) {
            this.assign(worker);
        } else {
            this.areDonePartners = true;
            if (this.areDonePrefixes && !this.isStartedHashing) {
                this.idleWorkers.add(worker);
                this.startHashing();
            } else {
                this.assign(worker);
            }
        }
    }

    private void handle(HashMiningCompletionMessage message) {
        ActorRef worker = this.sender();
        this.busyWorkers.remove(worker);

        switch (message.result) {
            case DONE:
                this.partnerHashes.put(message.id, message.hash);
                break;
        }
        if (this.partnerHashes.size() < 42) {
            this.assign(worker);
        } else {
            this.idleWorkers.add(worker);
            if (!this.doneWithAllTasks) {
                this.doneWithAllTasks = true;
                long endTime = System.currentTimeMillis();
                this.log.info("ID,Name,Password,Prefix,Partner,Hash");
                for (int i = 0; i < 42; i++) {
                    this.log.info((i + 1) + "," + this.names.get(i) + "," + this.crackedPasswords.get(i) + "," + this.prefixes.get(i) + "," + this.partners.get(i) + "," + this.partnerHashes.get(i));
                }
                this.log.info("Calculation Time: " + (endTime - this.startTime));
                this.log.info("STOP SYSTEM!");


                Set<ActorRef> workers = new HashSet<>();
                for (ActorRef wor : this.idleWorkers)
                	workers.add(wor);

				for (ActorRef wor : this.busyWorkers.keySet())
					workers.add(wor);

				for (ActorRef wor : workers)
					wor.tell(PoisonPill.getInstance(), this.getSelf());

				this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
            }
        }
    }

	private void assign(Worker.WorkMessage work) {
	    ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	private void assign(ActorRef worker) {
		Worker.WorkMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}

    private void kickOff() {
        startTime = System.currentTimeMillis();
        this.log.info("Start processing");

        loadFile(this.filePath);

        for (int i = 0; i < 42; i++) {
            this.assign(new Worker.PasswordMessage(i, passwordHashes.get(i)));
            this.assign(new Worker.PartnerMessage(i, sequencedGenes));
        }
    }

	private void startPrefixes() {
        int shardCount = this.busyWorkers.size() + this.idleWorkers.size();
        long maxValue = (long) Math.pow((double) 2, (double) 42);
        long shardSize = maxValue / shardCount;
        for (int i = 0; i < shardCount; i++) {
           long end = (i + 1) * shardSize;
           this.assign(new Worker.PrefixMessage(i*shardSize, end, this.crackedPasswords));
        }
    }

    private void startHashing() {
        this.isStartedHashing = true;
        for (int i = 0; i < 42; i++) {
            this.assign(new Worker.HashMiningMessage(i, partners.get(i), prefixes.get(i)));
        }
    }

	private void loadFile(String filePath) {
		String line = "";
		String cvsSplitBy = ";";

		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			br.readLine();
			while ((line = br.readLine()) != null) {
				if (!line.equals("")) {
					String[] parts = line.split(cvsSplitBy);
					this.names.add(parts[1]);
					this.passwordHashes.add(parts[2]);
					this.sequencedGenes.add(parts[3]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}