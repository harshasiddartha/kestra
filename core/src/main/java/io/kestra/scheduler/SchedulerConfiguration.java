package io.kestra.scheduler;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties("kestra.scheduler")
public record SchedulerConfiguration(
    @Bindable(defaultValue = "16")
    Integer vnodes
) {
}
