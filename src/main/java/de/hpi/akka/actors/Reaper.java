package de.hpi.akka.actors;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.Serializable;
import java.util.*;

/*
* Based on Reaper from Akka tutorial.
 */
public class Reaper extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "reaper";

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public static Props props() {
        return Props.create(Reaper.class);
    }

    public static class WatchMeMessage implements Serializable {
        private static final long serialVersionUID = -6058157411208395447L;
    }

    public static void watchWithDefaultReaper(AbstractActor actor) {
        ActorSelection reaper = actor.context().system().actorSelection("/user/reaper");
        reaper.tell(new WatchMeMessage(), actor.self());
    }

    private final Set<ActorRef> watchees = new HashSet<>();
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WatchMeMessage.class, message -> {
                    this.log.info("Watching " + this.sender());
                    if (this.watchees.add(this.sender()))
                        this.context().watch(this.sender());
                })
                .match(Terminated.class, message -> {
                    this.log.info("Terminated " + this.sender());
                    this.watchees.remove(this.sender());
                    if (this.watchees.isEmpty())
                        this.context().system().terminate();
                })
                .matchAny(object -> this.log().error("Unknown message"))
                .build();
    }
}