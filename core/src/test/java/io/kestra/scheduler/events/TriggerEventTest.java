package io.kestra.scheduler.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.scheduler.models.TriggerId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TriggerEventTest {
    
    @Test
    void shouldSerializeEvent() throws JsonProcessingException {
        // Given
        TriggerId id = new TriggerId("tenant", "namespace", "flow", "trigger");
        TriggerCreated event = new TriggerCreated(id, Instant.now(), false);
        
        // When - then
        String serialized = JacksonMapper.ofJson().writeValueAsString(event);
        assertThat(JacksonMapper.ofJson().readValue(serialized, TriggerEvent.class)).isEqualTo(event);
    }
}