package io.kestra.scheduler.events;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.models.HasUID;
import io.kestra.core.models.triggers.TriggerId;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TriggerCreated.class, name = "TRIGGER_CREATED"),
    @JsonSubTypes.Type(value = TriggerUpdated.class, name = "TRIGGER_UPDATED"),
    @JsonSubTypes.Type(value = TriggerDeleted.class, name = "TRIGGER_DELETED"),
    @JsonSubTypes.Type(value = TriggerExecuted.class, name = "TRIGGER_EXECUTED"),
    @JsonSubTypes.Type(value = TriggerCompleted.class, name = "TRIGGER_COMPLETED"),
    @JsonSubTypes.Type(value = BackfillTrigger.class, name = "BACKFILL_TRIGGER"),
    @JsonSubTypes.Type(value = ResetTrigger.class, name = "RESET_TRIGGER"),
    @JsonSubTypes.Type(value = DisableTrigger.class, name = "DISABLE_TRIGGER"),
    @JsonSubTypes.Type(value = TriggerEvent.Invalid.class, name = "INVALID"),
})
public interface TriggerEvent extends HasUID {
    
    /**
     * @return the trigger identifier.
     */
    @JsonProperty
    @JsonDeserialize(as = TriggerId.Default.class)
    TriggerId id();
    
    /**
     * @return the event timestamp.
     */
    @JsonProperty
    Instant timestamp();
    
    /**
     * @return the event type.
     */
    @JsonProperty
    default Type type() {
        return Type.from(this.getClass());
    }
    
    /**
     * {@inheritDoc}
     */
    @JsonIgnore
    @Override
    default String uid() {
        return this.id().uid();
    }
    
    record Invalid(TriggerId id,
                   Instant timestamp,
                   Map<String, Object> properties
    ) implements TriggerEvent {
        
        @JsonCreator
        public Invalid(@JsonProperty("id") TriggerId id,
                       @JsonProperty("timestamp") Instant timestamp) {
            this(id, timestamp, new HashMap<>());
        }
        
        @JsonAnySetter
        public void addProperty(String key, Object value) {
            this.properties.put(key, value);
        }
    }
}
