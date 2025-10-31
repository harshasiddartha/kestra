package io.kestra.core.models.namespaces.files;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.kestra.core.models.DeletedInterface;
import io.kestra.core.models.HasUID;
import io.kestra.core.models.TenantInterface;
import io.kestra.core.utils.IdUtils;
import io.swagger.v3.oas.annotations.Hidden;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.*;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Optional;

@Builder(toBuilder = true)
@Slf4j
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public class NamespaceFileMetadata implements DeletedInterface, TenantInterface, HasUID {
    @With
    @Hidden
    @Pattern(regexp = "^[a-z0-9][a-z0-9_-]*")
    private String tenantId;

    @NotNull
    private String namespace;

    @NotNull
    private String path;

    @NotNull
    private Integer version;

    @Builder.Default
    private boolean last = true;

    @NotNull
    private Long size;

    @Nullable
    @Builder.Default
    private Instant created = Instant.now();

    @Nullable
    private Instant updated;

    private boolean directory;

    private boolean deleted;

    @JsonCreator
    public NamespaceFileMetadata(String tenantId, String namespace, String path, Integer version, Long size, @Nullable Instant updated, boolean deleted) {
        this.tenantId = tenantId;
        this.namespace = namespace;
        this.path = path;
        this.version = version;
        this.size = size;
        this.updated = updated;
        this.directory = path.endsWith("/");
        this.deleted = deleted;
    }

    public NamespaceFileMetadata asLast() {
        Instant saveDate = Instant.now();
        return this.toBuilder().updated(saveDate).last(true).build();
    }

    @Override
    public String uid() {
        return IdUtils.fromParts(getTenantId(), getNamespace(), getPath(), String.valueOf(getVersion()));
    }
}
