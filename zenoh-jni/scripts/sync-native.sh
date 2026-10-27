#!/usr/bin/env bash
# Rebuilds the Rust FFI crate (boltffi-zenoh) and replaces this module's generated bindings and
# native library with the result. Bindings and dylib must always move together: JNI symbol names
# change between boltffi versions, and mixing them fails with UnsatisfiedLinkError.
#
#   scripts/sync-native.sh               # boltffi pack java --release, then sync
#   scripts/sync-native.sh --skip-build  # sync the existing dist/java output only
#
# BOLTFFI_ZENOH_DIR overrides the crate location (default ~/IDE/rustProjects/boltffi-zenoh).
set -euo pipefail

FFI_DIR="${BOLTFFI_ZENOH_DIR:-$HOME/IDE/rustProjects/boltffi-zenoh}"
MODULE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PKG="com/workoss/zenoh"
DIST="$FFI_DIR/dist/java"
JAVA_OUT="$MODULE_DIR/src/main/java/$PKG"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) PLATFORM=darwin-arm64; LIB=libzenoh_ffi_jni.dylib; TRIPLE=aarch64-apple-darwin ;;
  Darwin-x86_64) PLATFORM=darwin-x86_64; LIB=libzenoh_ffi_jni.dylib; TRIPLE=x86_64-apple-darwin ;;
  Linux-x86_64) PLATFORM=linux-x86_64; LIB=libzenoh_ffi_jni.so; TRIPLE=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) PLATFORM=linux-aarch64; LIB=libzenoh_ffi_jni.so; TRIPLE=aarch64-unknown-linux-gnu ;;
  *) echo "Unsupported build host $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

if [[ "${1:-}" != "--skip-build" ]]; then
  : "${JAVA_HOME:?JAVA_HOME must point to a JDK; boltffi compiles the JNI glue against it}"
  (cd "$FFI_DIR" && boltffi pack java --release)
fi

[[ -f "$DIST/native/$PLATFORM/$LIB" ]] || { echo "Missing $DIST/native/$PLATFORM/$LIB" >&2; exit 1; }

# boltffi links the JNI dylib from the Rust staticlib + jni_glue.c without an export list, so
# ~65k internal Rust symbols leak into the dyld exports trie and string table (~20 MB of
# __LINKEDIT). Relink with only the JNI entry points exported; -dead_strip then removes every
# object unreachable from them. Requires the staticlib and glue from a prior boltffi build.
relink_jni() {
  local staticlib="$FFI_DIR/target/$TRIPLE/release/libzenoh_ffi.a"
  local glue="$DIST/jni/jni_glue.c"
  [[ -f "$staticlib" && -f "$glue" ]] || { echo "Skip relink: missing $staticlib or $glue" >&2; return 0; }
  : "${JAVA_HOME:?JAVA_HOME required for JNI headers}"
  local out="$DIST/native/$PLATFORM/$LIB"
  local jni_inc="$JAVA_HOME/include"
  local arch; arch="$(uname -m)"
  local rc=0
  case "$(uname -s)" in
    Darwin)
      local keep="$FFI_DIR/exported-jni.sym"
      [[ -f "$keep" ]] || { echo "Skip relink: missing $keep" >&2; return 0; }
      cc -dynamiclib -O2 -fPIC \
        -I"$jni_inc" -I"$jni_inc/darwin" -I"$DIST/jni" \
        -Wl,-dead_strip -Wl,-x -Wl,-S \
        -Wl,-exported_symbols_list,"$keep" \
        -mmacosx-version-min=11.0 -arch "$arch" \
        -o "$out" "$glue" "$staticlib" -lSystem -liconv 2>relink.err || rc=$?
      ;;
    Linux)
      local map="$FFI_DIR/exported-jni.map"
      [[ -f "$map" ]] || { echo "Skip relink: missing $map" >&2; return 0; }
      cc -shared -O2 -fPIC \
        -I"$jni_inc" -I"$jni_inc/linux" -I"$DIST/jni" \
        -Wl,--gc-sections -Wl,--strip-all \
        -Wl,--version-script,"$map" \
        -o "$out" "$glue" "$staticlib" -lpthread -ldl -lm 2>relink.err || rc=$?
      ;;
  esac
  if [[ $rc -ne 0 ]]; then
    cat relink.err >&2
    rm -f relink.err
    echo "Relink failed (exit $rc)" >&2
    return 1
  fi
  grep -v "warning:" relink.err >&2 || true
  rm -f relink.err
  [[ -s "$out" ]] || { echo "Relink produced an empty $out" >&2; return 1; }
}
relink_jni

# The package holds generated files only, so a full replace also drops types removed in Rust.
rm -f "$JAVA_OUT"/*.java
cp "$DIST/$PKG"/*.java "$JAVA_OUT"/

# boltffi turns Result<Self, String> factories into constructors but discards the native error,
# leaving "Constructor failed" for every bad endpoint or TLS file. Surface the real message.
perl -pi -e 's/throw new RuntimeException\("Constructor failed"\);/throw new RuntimeException(new WireReader(__boltffi_error.bytes()).readString());/' "$JAVA_OUT"/*.java
if grep -l 'Constructor failed' "$JAVA_OUT"/*.java; then
  echo "Unpatched constructor error paths remain (generator output changed?)" >&2
  exit 1
fi

mkdir -p "$MODULE_DIR/src/main/resources/$PLATFORM"
cp "$DIST/native/$PLATFORM/$LIB" "$MODULE_DIR/src/main/resources/$PLATFORM/$LIB"
if [[ "$(uname -s)" == Darwin ]]; then
  xattr -c "$MODULE_DIR/src/main/resources/$PLATFORM/$LIB" || true
fi

echo "Synced $(ls "$DIST/$PKG"/*.java | wc -l | tr -d ' ') bindings and $PLATFORM/$LIB from $FFI_DIR"
