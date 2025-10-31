package io.kestra.core.storages;

import io.kestra.core.models.QueryFilter;
import io.kestra.core.models.namespaces.files.NamespaceFileMetadata;
import io.kestra.core.repositories.ArrayListTotal;
import io.kestra.core.repositories.NamespaceFileMetadataRepositoryInterface;
import io.micronaut.data.model.Pageable;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * The default {@link Namespace} implementation.
 * This class acts as a facade to the {@link StorageInterface} for manipulating namespace files.
 *
 * @see Storage#namespace()
 * @see Storage#namespace(String)
 */
public class InternalNamespace implements Namespace {

    private static final Logger LOG = LoggerFactory.getLogger(InternalNamespace.class);

    private final String namespace;
    private final String tenant;
    private final StorageInterface storage;
    private final NamespaceFileMetadataRepositoryInterface namespaceFileMetadataRepository;
    private final Logger logger;

    /**
     * Creates a new {@link InternalNamespace} instance.
     *
     * @param namespace The namespace
     * @param storage   The storage.
     */
    public InternalNamespace(@Nullable final String tenant, final String namespace, final StorageInterface storage, final NamespaceFileMetadataRepositoryInterface namespaceFileMetadataRepository) {
        this(LOG, tenant, namespace, storage, namespaceFileMetadataRepository);
    }

    /**
     * Creates a new {@link InternalNamespace} instance.
     *
     * @param logger    The logger to be used by this class.
     * @param namespace The namespace
     * @param tenant    The tenant.
     * @param storage   The storage.
     */
    public InternalNamespace(final Logger logger, @Nullable final String tenant, final String namespace, final StorageInterface storage, final NamespaceFileMetadataRepositoryInterface namespaceFileMetadataRepositoryInterface) {
        this.logger = Objects.requireNonNull(logger, "logger cannot be null");
        this.namespace = Objects.requireNonNull(namespace, "namespace cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.namespaceFileMetadataRepository = Objects.requireNonNull(namespaceFileMetadataRepositoryInterface, "namespaceFileMetadataRepository cannot be null");
        this.tenant = tenant;
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public String tenantId() {
        return tenant;
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public List<NamespaceFile> all() throws IOException {
        return all(null);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public List<NamespaceFile> all(final String containing) throws IOException {
        ArrayListTotal<NamespaceFileMetadata> namespaceFilesMetadata = namespaceFileMetadataRepository.find(Pageable.UNPAGED, tenant, Stream.concat(
            Stream.of(QueryFilter.builder().field(QueryFilter.Field.NAMESPACE).operation(QueryFilter.Op.EQUALS).value(namespace).build()),
            Optional.ofNullable(containing).map(p -> QueryFilter.builder().field(QueryFilter.Field.QUERY).operation(QueryFilter.Op.EQUALS).value(p).build()).stream()
        ).toList(), false);

        return namespaceFilesMetadata.map(nsFileMetadata -> NamespaceFile.of(namespace, Path.of(nsFileMetadata.getPath()), nsFileMetadata.getVersion()));
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public NamespaceFile get(final Path path) throws IOException {
        int version = namespaceFileMetadataRepository.findByPath(tenant, namespace, path.toString()).map(NamespaceFileMetadata::getVersion).orElse(1);

        return NamespaceFile.of(namespace, path, version);
    }

    public Path relativize(final URI uri) {
        return NamespaceFile.of(namespace)
            .storagePath()
            .relativize(Path.of(uri.getPath()));
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public List<NamespaceFile> findAllFilesMatching(final Predicate<Path> predicate) throws IOException {
        return all().stream().filter(it -> predicate.test(it.path(true))).toList();
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public InputStream getFileContent(final Path path) throws IOException {
        Optional<NamespaceFileMetadata> inRepository = namespaceFileMetadataRepository.findByPath(tenant, namespace, path.toString());
        int version = inRepository.map(NamespaceFileMetadata::getVersion).orElse(1);

        Path namespaceFilePath = NamespaceFile.of(namespace, path, version).storagePath();
        return storage.get(tenant, namespace, namespaceFilePath.toUri());
    }

    @Override
    public FileAttributes getFileMetadata(Path path) throws IOException {
        return namespaceFileMetadataRepository.findByPath(tenant, namespace, path.toString()).map(NamespaceFileAttributes::new).orElse(null);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return namespaceFileMetadataRepository.findByPath(tenant, namespace, path.toString())
            .map(namespaceFileMetadata -> !namespaceFileMetadata.isDeleted())
            .orElse(false);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public NamespaceFile putFile(final Path path, final InputStream content, final Conflicts onAlreadyExist) throws IOException, URISyntaxException {
        Optional<NamespaceFileMetadata> inRepository = namespaceFileMetadataRepository.findByPath(tenant, namespace, path.toString());
        int version = inRepository.map(NamespaceFileMetadata::getVersion).orElse(1);
        Path storagePath = NamespaceFile.of(namespace, path, version).storagePath();
        // Remove Windows letter
        URI cleanUri = new URI(storagePath.toUri().toString().replaceFirst("^file:///[a-zA-Z]:", ""));

        return switch (onAlreadyExist) {
            case OVERWRITE -> {
                URI uri = storage.put(tenant, namespace, cleanUri, content);
                namespaceFileMetadataRepository.save(
                    inRepository.orElse(NamespaceFileMetadata.builder()
                        .tenantId(tenant)
                        .namespace(namespace)
                        .path(path.toString())
                        .size(storage.getAttributes(tenant, namespace, cleanUri).getSize())
                        .build())
                );
                NamespaceFile namespaceFile = new NamespaceFile(relativize(uri), uri, namespace);
                if (inRepository.isPresent()) {
                    logger.debug(String.format(
                        "File '%s' overwritten into namespace '%s'.",
                        path,
                        namespace
                    ));
                } else {
                    logger.debug(String.format(
                        "File '%s' added to namespace '%s'.",
                        path,
                        namespace
                    ));
                }
                yield namespaceFile;
            }
            case ERROR -> {
                if (inRepository.isEmpty()) {
                    URI uri = storage.put(tenant, namespace, cleanUri, content);
                    namespaceFileMetadataRepository.save(
                        NamespaceFileMetadata.builder()
                            .tenantId(tenant)
                            .namespace(namespace)
                            .path(path.toString())
                            .size(storage.getAttributes(tenant, namespace, cleanUri).getSize())
                            .build()
                    );
                    yield new NamespaceFile(relativize(uri), uri, namespace);
                } else {
                    throw new IOException(String.format(
                        "File '%s' already exists in namespace '%s' and conflict is set to %s",
                        path,
                        namespace,
                        Conflicts.ERROR
                    ));
                }
            }
            case SKIP -> {
                if (inRepository.isEmpty()) {
                    URI uri = storage.put(tenant, namespace, cleanUri, content);
                    namespaceFileMetadataRepository.save(
                        NamespaceFileMetadata.builder()
                            .tenantId(tenant)
                            .namespace(namespace)
                            .path(path.toString())
                            .size(storage.getAttributes(tenant, namespace, cleanUri).getSize())
                            .build()
                    );
                    NamespaceFile namespaceFile = new NamespaceFile(relativize(uri), uri, namespace);
                    logger.debug(String.format(
                        "File '%s' added to namespace '%s'.",
                        path,
                        namespace
                    ));
                    yield namespaceFile;
                } else {
                    logger.debug(String.format(
                        "File '%s' already exists in namespace '%s' and conflict is set to %s. Skipping.",
                        path,
                        namespace,
                        Conflicts.SKIP
                    ));
                    URI uri = URI.create(StorageContext.KESTRA_PROTOCOL + storagePath);
                    yield new NamespaceFile(relativize(uri), uri, namespace);
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public URI createDirectory(Path path) throws IOException {
        namespaceFileMetadataRepository.save(
            NamespaceFileMetadata.builder()
                .tenantId(tenant)
                .namespace(namespace)
                .path(path.toString())
                .size(0L)
                .build()
        );
        return storage.createDirectory(tenant, namespace, NamespaceFile.of(namespace, path, 1).storagePath().toUri());
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public boolean delete(Path path) throws IOException {
        return storage.delete(tenant, namespace, URI.create(path.toString().replace("\\", "/")));
    }
}
