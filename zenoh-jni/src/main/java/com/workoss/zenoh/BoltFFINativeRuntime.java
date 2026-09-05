package com.workoss.zenoh;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

final class BoltFFINativeRuntime {
    static void load(
        Class<?> owner,
        String preferredLibrary,
        String fallbackLibrary
    ) {
        loadDesktopLibraries(owner, preferredLibrary, fallbackLibrary);
    }

    private BoltFFINativeRuntime() {}

    private static void loadDesktopLibraries(
        Class<?> owner,
        String preferredLibrary,
        String fallbackLibrary
    ) {
        DesktopLibraries desktopLibraries = DesktopLibraries.prepare(
            owner,
            preferredLibrary
        );
        LoadResult preferredResult = desktopLibraries.loadPreferred(preferredLibrary);
        if (preferredResult.isLoaded()) {
            return;
        }

        if (!preferredLibrary.equals(fallbackLibrary)) {
            desktopLibraries = desktopLibraries.withFallback(fallbackLibrary);
        }
        LoadResult sharedResult = desktopLibraries.loadAfterPreferredFailure(
            preferredResult,
            preferredLibrary,
            fallbackLibrary
        );
        if (sharedResult.isLoaded()) {
            return;
        }
        throw sharedResult.failure();
    }

    private static UnsatisfiedLinkError nativeLibraryFailure(
        String message,
        Throwable cause
    ) {
        UnsatisfiedLinkError failure = new UnsatisfiedLinkError(message);
        failure.initCause(cause);
        return failure;
    }
    private static List<String> desktopNativeDirectories() {
        String operatingSystem = System.getProperty("os.name", "")
            .toLowerCase(Locale.ROOT);
        String architecture = System.getProperty("os.arch", "")
            .toLowerCase(Locale.ROOT);

        if ((operatingSystem.contains("mac") || operatingSystem.contains("darwin"))
            && (architecture.equals("aarch64") || architecture.equals("arm64"))) {
            return Arrays.asList(
                "darwin-arm64",
                "darwin-aarch64"
            );
        }

        if ((operatingSystem.contains("mac") || operatingSystem.contains("darwin"))
            && (architecture.equals("x86_64") || architecture.equals("amd64"))) {
            return Arrays.asList(
                "darwin-x86_64",
                "darwin-x86-64"
            );
        }

        if ((operatingSystem.contains("linux"))
            && (architecture.equals("x86_64") || architecture.equals("amd64"))) {
            return Arrays.asList(
                "linux-x86_64",
                "linux-x86-64"
            );
        }

        if ((operatingSystem.contains("linux"))
            && (architecture.equals("aarch64") || architecture.equals("arm64"))) {
            return Arrays.asList(
                "linux-aarch64",
                "linux-arm64"
            );
        }

        if ((operatingSystem.contains("windows"))
            && (architecture.equals("x86_64") || architecture.equals("amd64"))) {
            return Arrays.asList(
                "windows-x86_64",
                "windows-x86-64",
                "win32-x86_64"
            );
        }

        if ((operatingSystem.contains("windows"))
            && (architecture.equals("aarch64") || architecture.equals("arm64"))) {
            return Arrays.asList(
                "windows-aarch64",
                "windows-arm64",
                "win32-arm64"
            );
        }


        return Collections.emptyList();
    }
    private static final class LoadResult {
        private final boolean loaded;
        private final UnsatisfiedLinkError failure;

        private LoadResult(
            boolean loaded,
            UnsatisfiedLinkError failure
        ) {
            this.loaded = loaded;
            this.failure = failure;
        }

        private static LoadResult loaded() {
            return new LoadResult(true, null);
        }

        private static LoadResult unavailable() {
            return new LoadResult(false, null);
        }

        private static LoadResult failed(UnsatisfiedLinkError failure) {
            return new LoadResult(false, failure);
        }

        private static LoadResult system(String libraryName) {
            try {
                System.loadLibrary(libraryName);
                return loaded();
            } catch (UnsatisfiedLinkError failure) {
                return failed(failure);
            } catch (SecurityException failure) {
                return failed(nativeLibraryFailure(
                    "Could not load system native library '" + libraryName + "'",
                    failure
                ));
            }
        }

        private boolean isLoaded() {
            return loaded;
        }

        private UnsatisfiedLinkError failure() {
            return failure;
        }

        private LoadResult merge(LoadResult other) {
            if (loaded) {
                return this;
            }
            if (other.loaded) {
                return other;
            }
            if (failure == null) {
                return other;
            }
            if (other.failure != null) {
                failure.addSuppressed(other.failure);
            }
            return new LoadResult(false, failure);
        }
    }
    private static final class DesktopLibraries {
        private final Class<?> owner;
        private final ExtractionRoot extraction;
        private final BundledLibrary preferred;
        private final BundledLibrary fallback;

        private DesktopLibraries(
            Class<?> owner,
            ExtractionRoot extraction,
            BundledLibrary preferred,
            BundledLibrary fallback
        ) {
            this.owner = owner;
            this.extraction = extraction;
            this.preferred = preferred;
            this.fallback = fallback;
        }

        private static DesktopLibraries prepare(
            Class<?> owner,
            String preferredName
        ) {
            ExtractionRoot extraction = new ExtractionRoot();
            BundledLibrary library = BundledLibrary.extract(
                owner,
                preferredName,
                extraction
            );
            return new DesktopLibraries(owner, extraction, library, library);
        }

        private DesktopLibraries withFallback(String fallbackName) {
            BundledLibrary fallbackLibrary = BundledLibrary.extract(
                owner,
                fallbackName,
                extraction
            );
            return new DesktopLibraries(
                owner,
                extraction,
                preferred,
                fallbackLibrary
            );
        }

        private LoadResult loadAfterPreferredFailure(
            LoadResult preferredResult,
            String preferredName,
            String fallbackName
        ) {
            if (preferredName.equals(fallbackName)) {
                return preferredResult;
            }

            LoadResult fallbackResult = loadFallback(fallbackName);
            if (!fallbackResult.isLoaded()) {
                return preferredResult.merge(fallbackResult);
            }

            return preferredResult.merge(loadPreferred(preferredName));
        }

        private LoadResult loadPreferred(String libraryName) {
            return load(preferred, libraryName);
        }

        private LoadResult loadFallback(String libraryName) {
            return load(fallback, libraryName);
        }

        private LoadResult load(
            BundledLibrary bundled,
            String libraryName
        ) {
            LoadResult bundledResult = bundled.tryLoad(libraryName);
            return bundledResult.isLoaded()
                ? bundledResult
                : bundledResult.merge(LoadResult.system(libraryName));
        }
    }
    private static final class BundledLibrary {
        private final File file;
        private final String failureMessage;
        private final Throwable failureCause;

        private BundledLibrary(
            File file,
            String failureMessage,
            Throwable failureCause
        ) {
            this.file = file;
            this.failureMessage = failureMessage;
            this.failureCause = failureCause;
        }

        private static BundledLibrary extract(
            Class<?> owner,
            String libraryName,
            ExtractionRoot extraction
        ) {
            String mappedName = System.mapLibraryName(libraryName);
            try {
                validateMappedName(mappedName);
                BundledResource resource = BundledResource.find(owner, mappedName);
                if (resource == null) {
                    return absent();
                }
                try (InputStream input = resource.input) {
                    return extracted(
                        extraction.directory().extract(mappedName, input)
                    );
                }
            } catch (IOException failure) {
                return failed(
                    "Could not extract bundled native library '" + mappedName + "'",
                    failure
                );
            } catch (SecurityException failure) {
                return failed(
                    "Could not access bundled native library '" + mappedName + "'",
                    failure
                );
            }
        }

        private static BundledLibrary absent() {
            return new BundledLibrary(null, null, null);
        }

        private static BundledLibrary extracted(File file) {
            return new BundledLibrary(file, null, null);
        }

        private static BundledLibrary failed(
            String message,
            Throwable cause
        ) {
            return new BundledLibrary(null, message, cause);
        }

        private LoadResult tryLoad(String libraryName) {
            if (file == null) {
                UnsatisfiedLinkError failure = extractionFailure();
                return failure == null
                    ? LoadResult.unavailable()
                    : LoadResult.failed(failure);
            }

            try {
                System.load(file.getAbsolutePath());
                return LoadResult.loaded();
            } catch (UnsatisfiedLinkError failure) {
                return LoadResult.failed(failure);
            } catch (SecurityException failure) {
                return LoadResult.failed(nativeLibraryFailure(
                    "Could not load bundled native library '" + libraryName + "'",
                    failure
                ));
            }
        }

        private UnsatisfiedLinkError extractionFailure() {
            return failureMessage == null
                ? null
                : nativeLibraryFailure(failureMessage, failureCause);
        }

        private static void validateMappedName(
            String mappedName
        ) throws IOException {
            if (mappedName.isEmpty()
                || mappedName.indexOf('/') >= 0
                || mappedName.indexOf('\\') >= 0) {
                throw new IOException(
                    "invalid mapped native library name '" + mappedName + "'"
                );
            }
        }
    }
    private enum DirectorySecurity {
        POSIX,
        ACL;

        private static DirectorySecurity detect(
            Path path
        ) throws IOException {
            FileStore store = Files.getFileStore(path);
            if (store.supportsFileAttributeView(
                AclFileAttributeView.class
            )) {
                return ACL;
            }
            if (store.supportsFileAttributeView(
                PosixFileAttributeView.class
            )) {
                return POSIX;
            }
            throw new IOException(
                "owner-only directory attributes are unavailable"
            );
        }

        private FileAttribute<?> ownerAttribute(
            UserPrincipal owner
        ) {
            return this == POSIX
                ? PosixFilePermissions.asFileAttribute(
                    ownerPermissions()
                )
                : aclAttribute(owner);
        }

        private void verify(
            Path directory,
            UserPrincipal expectedOwner
        ) throws IOException {
            verifyDirectory(directory);
            if (this == POSIX) {
                PosixFileAttributes attributes =
                    posixAttributes(directory);
                if (!attributes.owner().equals(expectedOwner)
                    || !attributes.permissions().equals(ownerPermissions())) {
                    throw new IOException(
                        "native library extraction directory is not owner-only"
                    );
                }
                return;
            }

            AclFileAttributeView view = aclView(directory);
            List<AclEntry> acl = view.getAcl();
            boolean ownerOnly = !acl.isEmpty()
                && acl.stream().allMatch(entry ->
                    entry.type() == AclEntryType.ALLOW
                        && entry.principal().equals(expectedOwner)
                        && entry.flags().equals(ownerAclFlags())
                );
            Set<AclEntryPermission> permissions =
                acl.stream()
                    .flatMap(entry -> entry.permissions().stream())
                    .collect(Collectors.toCollection(() ->
                        EnumSet.noneOf(
                            AclEntryPermission.class
                        )
                    ));
            if (!view.getOwner().equals(expectedOwner)
                || !ownerOnly
                || !permissions.containsAll(ownerAclPermissions())) {
                throw new IOException(
                    "native library extraction ACL is not owner-only"
                );
            }
        }

        private UserPrincipal currentOwner(
            Path parent
        ) throws IOException {
            Path probe = null;
            try {
                probe = Files.createTempFile(
                    parent,
                    "boltffi-owner-",
                    ".probe"
                );
                BasicFileAttributes attributes =
                    Files.readAttributes(
                        probe,
                        BasicFileAttributes.class,
                        LinkOption.NOFOLLOW_LINKS
                    );
                if (attributes.isSymbolicLink() || !attributes.isRegularFile()) {
                    throw new IOException(
                        "native extraction owner probe is not a regular file"
                    );
                }
                UserPrincipal owner = this == POSIX
                    ? posixAttributes(probe).owner()
                    : aclView(probe).getOwner();
                Files.delete(probe);
                return owner;
            } catch (IOException failure) {
                ExtractionRoot.discard(probe, failure);
                throw failure;
            } catch (SecurityException failure) {
                ExtractionRoot.discard(probe, failure);
                throw failure;
            }
        }

        private static Set<
            PosixFilePermission
        > ownerPermissions() {
            return EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE
            );
        }

        private static PosixFileAttributes
        posixAttributes(
            Path directory
        ) throws IOException {
            return Files.readAttributes(
                directory,
                PosixFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS
            );
        }

        private static AclFileAttributeView aclView(
            Path directory
        ) throws IOException {
            AclFileAttributeView view =
                Files.getFileAttributeView(
                    directory,
                    AclFileAttributeView.class,
                    LinkOption.NOFOLLOW_LINKS
                );
            if (view == null) {
                throw new IOException(
                    "native library extraction ACL is unavailable"
                );
            }
            return view;
        }

        private static List<AclEntry> ownerAcl(
            UserPrincipal owner
        ) {
            AclEntry entry =
                AclEntry.newBuilder()
                    .setType(AclEntryType.ALLOW)
                    .setPrincipal(owner)
                    .setPermissions(ownerAclPermissions())
                    .setFlags(ownerAclFlags())
                    .build();
            return Collections.singletonList(entry);
        }

        private static Set<
            AclEntryPermission
        > ownerAclPermissions() {
            return EnumSet.allOf(
                AclEntryPermission.class
            );
        }

        private static Set<
            AclEntryFlag
        > ownerAclFlags() {
            return EnumSet.of(
                AclEntryFlag.FILE_INHERIT,
                AclEntryFlag.DIRECTORY_INHERIT
            );
        }

        private static FileAttribute<
            List<AclEntry>
        > aclAttribute(
            final UserPrincipal owner
        ) {
            final List<AclEntry> acl = ownerAcl(owner);
            return new FileAttribute<
                List<AclEntry>
            >() {
                public String name() {
                    return "acl:acl";
                }

                public List<AclEntry> value() {
                    return acl;
                }
            };
        }

        private static void verifyDirectory(
            Path directory
        ) throws IOException {
            BasicFileAttributes attributes =
                Files.readAttributes(
                    directory,
                    BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS
                );
            if (attributes.isSymbolicLink() || !attributes.isDirectory()) {
                throw new IOException(
                    "native library extraction path is not a directory"
                );
            }
        }
    }
    private static final class OwnedDirectory {
        private final Path path;

        private OwnedDirectory(Path path) {
            this.path = path;
        }

        private static OwnedDirectory temporary(
            Path parent,
            String prefix,
            DirectorySecurity security,
            UserPrincipal owner
        ) throws IOException {
            Path path = null;
            try {
                path = Files.createTempDirectory(
                    parent,
                    prefix,
                    security.ownerAttribute(owner)
                );
                security.verify(path, owner);
                path.toFile().deleteOnExit();
                return new OwnedDirectory(path);
            } catch (IOException failure) {
                ExtractionRoot.discard(path, failure);
                throw failure;
            } catch (SecurityException failure) {
                ExtractionRoot.discard(path, failure);
                throw failure;
            } catch (UnsupportedOperationException unsupported) {
                IOException failure = new IOException(
                    "owner-only directory attributes are unavailable",
                    unsupported
                );
                ExtractionRoot.discard(path, failure);
                throw failure;
            }
        }

        private File extract(
            String mappedName,
            InputStream input
        ) throws IOException {
            Path destination = child(mappedName);
            try {
                try (
                    OutputStream output = Files.newOutputStream(
                        destination,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE
                    )
                ) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = input.read(buffer)) != -1) {
                        output.write(buffer, 0, bytesRead);
                    }
                }
                BasicFileAttributes attributes = Files.readAttributes(
                    destination,
                    BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS
                );
                if (attributes.isSymbolicLink() || !attributes.isRegularFile()) {
                    throw new IOException(
                        "extracted native library is not a regular file"
                    );
                }
                File extracted = destination.toFile();
                extracted.deleteOnExit();
                return extracted;
            } catch (IOException failure) {
                ExtractionRoot.discard(destination, failure);
                throw failure;
            } catch (SecurityException failure) {
                ExtractionRoot.discard(destination, failure);
                throw failure;
            }
        }

        private Path child(String name) throws IOException {
            Path child = path.resolve(name).normalize();
            if (!path.equals(child.getParent())
                || child.getFileName() == null
                || !child.getFileName().toString().equals(name)) {
                throw new IOException("invalid native library extraction path");
            }
            return child;
        }
    }    private static final class BundledResource {
        private final InputStream input;

        private BundledResource(InputStream input) {
            this.input = input;
        }

        private static BundledResource find(
            Class<?> owner,
            String mappedName
        ) {
            BundledResource resource = find(
                owner,
                desktopNativeDirectories().iterator(),
                mappedName
            );
            return resource == null
                ? openPath(owner, "/" + mappedName)
                : resource;
        }

        private static BundledResource find(
            Class<?> owner,
            Iterator<String> directories,
            String mappedName
        ) {
            if (!directories.hasNext()) {
                return null;
            }
            String directory = directories.next();
            BundledResource resource = openPath(
                owner,
                "/" + directory + "/" + mappedName
            );
            if (resource != null) {
                return resource;
            }
            resource = openPath(
                owner,
                "/native/" + directory + "/" + mappedName
            );
            return resource == null
                ? find(owner, directories, mappedName)
                : resource;
        }

        private static BundledResource openPath(
            Class<?> owner,
            String resourcePath
        ) {
            InputStream input = owner.getResourceAsStream(resourcePath);
            return input == null ? null : new BundledResource(input);
        }
    }
    private static final class ExtractionRoot {
        private OwnedDirectory directory;
        private Throwable failure;

        private OwnedDirectory directory() throws IOException {
            if (directory != null) {
                return directory;
            }
            if (failure != null) {
                throwFailure();
            }
            try {
                directory = open();
                return directory;
            } catch (IOException | SecurityException openFailure) {
                failure = openFailure;
                throw openFailure;
            }
        }

        private void throwFailure() throws IOException {
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            throw (SecurityException) failure;
        }

        private static OwnedDirectory open() throws IOException {
            String temporaryRoot = System.getProperty("java.io.tmpdir", "");
            if (temporaryRoot.isEmpty()) {
                throw new IOException("temporary directory property is unavailable");
            }
            Path temporaryDirectory = Paths.get(temporaryRoot).toRealPath();
            BasicFileAttributes attributes = Files.readAttributes(
                temporaryDirectory,
                BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS
            );
            if (attributes.isSymbolicLink() || !attributes.isDirectory()) {
                throw new IOException("temporary directory is unavailable");
            }
            DirectorySecurity security = DirectorySecurity.detect(temporaryDirectory);
            UserPrincipal owner = security.currentOwner(temporaryDirectory);
            return OwnedDirectory.temporary(
                temporaryDirectory,
                "boltffi-native-",
                security,
                owner
            );
        }

        private static void discard(Path path, Throwable failure) {
            if (path == null) {
                return;
            }
            try {
                Files.deleteIfExists(path);
            } catch (IOException | SecurityException cleanupFailure) {
                if (cleanupFailure != failure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
    }}