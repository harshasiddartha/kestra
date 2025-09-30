package io.kestra.core.models.triggers;

import io.kestra.core.models.HasUID;
import io.kestra.core.models.flows.FlowId;
import io.kestra.core.utils.IdUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Represents a unique and global identifier for a trigger.
 */
public interface TriggerId extends HasUID {
    
    String getTenantId();
    
    String getNamespace();
    
    String getFlowId();
    
    String getTriggerId();
    
    @Override
    default String uid() {
        return IdUtils.fromParts(
            getTenantId(),
            getNamespace(),
            getFlowId(),
            getTriggerId()
        );
    }
    
    static TriggerId of(FlowId flowId, AbstractTrigger trigger) {
        return new Default(flowId.getTenantId(), flowId.getNamespace(), flowId.getId(), trigger.getId());
    }
    
    static TriggerId of(TriggerId triggerId) {
        return new Default(triggerId.getTenantId(), triggerId.getNamespace(), triggerId.getFlowId(), triggerId.getTriggerId());
    }
    
    
    @Getter
    @AllArgsConstructor
    class Default implements TriggerId {
        private final String tenantId;
        private final String namespace;
        private final String flowId;
        private final String triggerId;
    }
}
