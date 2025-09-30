package io.kestra.scheduler;

import org.slf4j.Logger;

import java.util.Set;

/**
 A service interface responsible for managing and notifying schedulers about trigger
 * assignment changes across distributed or partitioned environments.
 */
public interface TriggerAssigner {
    
    /**
     * Subscribes a {@link TriggerAssignmentListener} to receive notifications
     * about trigger vNode assignment changes for the specified scheduler.
     * <p>
     * Once subscribed, the listener will be notified whenever:
     * <ul>
     *   <li>All current trigger assignments are revoked, via
     *       {@link TriggerAssignmentListener#onTriggerAssignmentRevoked()}, or</li>
     *   <li>New trigger assignments are made, via
     *       {@link TriggerAssignmentListener#onTriggerAssignmentAssigned(Set)}.</li>
     * </ul>
     *
     * @param schedulerId the unique identifier of the scheduler service subscribing
     *                    to trigger assignment updates; must not be {@code null}
     * @param listener    the listener that will receive trigger assignment notifications;
     *                    must not be {@code null}
     * @throws NullPointerException if {@code schedulerId} or {@code listener} is {@code null}
     */
    void subscribe(String schedulerId, TriggerAssignmentListener listener);
    
    /**
     * Interface for listening on trigger vNodes assignment changes.
     */
    interface TriggerAssignmentListener {
        
        /**
         * Invokes when all trigger assignments (vNodes) are revoked.
         * <p>
         * When this method return all vNodes are considered effectively revoked, and no trigger is
         * expected to be processed.
         */
        void onTriggerAssignmentRevoked();
        
        /**
         * Invokes when
         *
         * @param vNodes the list of vNodes assigned to the scheduler.
         */
        void onTriggerAssignmentAssigned(Set<Integer> vNodes);
    }
    
    class LoggerTriggerAssignmentListener implements TriggerAssignmentListener {
        
        private final TriggerAssignmentListener listener;
        private final Logger logger;
        
        private Set<Integer> currentAssignment = Set.of() ;
        
        public LoggerTriggerAssignmentListener(TriggerAssignmentListener listener, Logger logger) {
            this.listener = listener;
            this.logger = logger;
        }
        
        @Override
        public void onTriggerAssignmentRevoked() {
            logger.info("Trigger vNodes assignment revoked: {}", currentAssignment);
            listener.onTriggerAssignmentRevoked();
        }
        
        @Override
        public void onTriggerAssignmentAssigned(Set<Integer> vNodes) {
            logger.info("Trigger vNodes assignment assigned: {}", vNodes);
            listener.onTriggerAssignmentAssigned(vNodes);
            this.currentAssignment = vNodes;
        }
    }
}
