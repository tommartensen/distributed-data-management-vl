package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Master.RegistrationMessage;
import de.hpi.octopus.util.Solver;
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
    public abstract static class WorkMessage implements Serializable {
        public static final long serialVersionUID = 7741343650855817071L;
        private WorkMessage() {}
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PasswordMessage extends WorkMessage implements Serializable {
        private static final long serialVersionUID = 651145820504746184L;
        private PasswordMessage() {}
        public int id;
        public String hash;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PrefixMessage extends WorkMessage implements Serializable {
        private static final long serialVersionUID = 1746256255673692918L;
        private PrefixMessage() {}
        public long start;
        public long end;
        public Map<Integer, Integer> passwords;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PartnerMessage extends WorkMessage  implements Serializable {
        private static final long serialVersionUID = -2148611780905300325L;
        private PartnerMessage() {}
        public int id;
        public List<String> sequences;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class HashMiningMessage extends WorkMessage  implements Serializable {
        private static final long serialVersionUID = -7850632418688845902L;
        private HashMiningMessage() {}
        public int id;
        public int partner;
        public int prefix;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class TerminationMessage implements Serializable {
        private static final long serialVersionUID = 1859755325220694017L;
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
        Reaper.watchWithDefaultReaper(this);
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
                .match(PasswordMessage.class, this::handle)
                .match(PrefixMessage.class, this::handle)
                .match(PartnerMessage.class, this::handle)
                .match(HashMiningMessage.class, this::handle)
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
        if (member.hasRole(OctopusMaster.MASTER_ROLE))
            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new RegistrationMessage(), this.self());
    }

    private void handle(PasswordMessage message) {
        this.log.debug("I am " + this.self().path() + " and do password finding for" + message.id);
        try {
            int password = Solver.unHash(message.hash);
            this.sender().tell(new Master.PasswordCompletionMessage(Master.PasswordCompletionMessage.status.DONE, message.id, password), this.self());
        } catch (RuntimeException e) {
            this.sender().tell(new Master.PasswordCompletionMessage(Master.PasswordCompletionMessage.status.FAILED), this.self());
        }
    }

    private void handle(PrefixMessage message) {
        this.log.debug("I am " + this.self().path() + " and do prefix finding for [" + message.start + ", " + message.end + "]");
        List<Integer> passwords = new ArrayList<>();
        for (int i = 0; i < message.passwords.size(); i++)
            passwords.add(message.passwords.get(i));
        try {
            List<Integer> prefixes = Solver.solve(message.start, message.end, passwords);
            this.sender().tell(new Master.PrefixCompletionMessage(Master.PrefixCompletionMessage.status.DONE, prefixes), this.self());
        } catch (RuntimeException e) {
            this.sender().tell(new Master.PrefixCompletionMessage(Master.PrefixCompletionMessage.status.FAILED), this.self());
        }
    }

    private void handle(PartnerMessage message) {
        this.log.debug("I am " + this.self().path() + " and do partner finding for " + message.id);

        // add one since we start counting at 1 :(
        int partner = Solver.longestOverlapPartner(message.id, message.sequences) + 1;
        this.sender().tell(new Master.PartnerCompletionMessage(Master.PartnerCompletionMessage.status.DONE, message.id, partner), this.self());
    }

    private void handle(HashMiningMessage message) {
        this.log.debug("I am " + this.self().path() + " and do hash mining for " + message.id);
        String hash = Solver.findHash(message.partner, message.prefix);
        this.sender().tell(new Master.HashMiningCompletionMessage(Master.HashMiningCompletionMessage.status.DONE, message.id, hash), this.self());
    }
}