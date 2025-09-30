package io.kestra.scheduler.events;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TypeTest {
    
    @Test
    void shouldGetTriggerEventType() {
        assertThat(Type.from(ResetTrigger.class)).isEqualTo(Type.RESET_TRIGGER);
    }
}