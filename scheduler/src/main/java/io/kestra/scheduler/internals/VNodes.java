package io.kestra.scheduler.internals;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.kestra.core.models.flows.FlowId;
import io.kestra.core.models.triggers.TriggerId;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class VNodes {
    
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32_fixed();
    
    public static Set<Integer> computeVNodeOwnership(final String serviceId, final List<String> activeServices, int vNodeCount) {
        Map<Integer, String> vnodeToScheduler = new HashMap<>();
        for (int vnode = 0; vnode < vNodeCount; vnode++) {
            // Hash the vNode string and mod by number of active services
            int hash = HASH_FUNCTION.hashString("vNode-" + vnode, StandardCharsets.UTF_8).asInt();
            // Ensure positive
            int idx = Math.floorMod(hash, activeServices.size());
            vnodeToScheduler.put(vnode, activeServices.get(idx));
        }
        
        // Collect all vNodes assigned to this service
        return vnodeToScheduler.entrySet()
            .stream()
            .filter(e -> e.getValue().equals(serviceId))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }
    
    public static int computeVNodeFromTrigger(final TriggerId id, int vNodeCount) {
        Objects.requireNonNull(id, "id cannot be null");
        return computeVNode(vNodeCount, FlowId.uidWithoutRevision(FlowId.of(id.getTenantId(), id.getNamespace(), id.getFlowId(), null)));
    }
    
    public static int computeVNodeFromFlow(final FlowId id, int vNodeCount) {
        Objects.requireNonNull(id, "id cannot be null");
        return computeVNode(vNodeCount, FlowId.uidWithoutRevision(id));
    }
    
    private static int computeVNode(int vNodeCount, String key) {
        int hash = HASH_FUNCTION.hashString(key, StandardCharsets.UTF_8).asInt();
        return Math.floorMod(hash, vNodeCount);
    }
    

}
