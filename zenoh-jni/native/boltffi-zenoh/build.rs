// Restrict the cdylib's dyld exports to JNI entry points. Without this, ~65k Rust
// monomorphizations become global exports and add ~20 MB of string table + exports
// trie to __LINKEDIT. Only affects the cdylib, not rlib/proc-macro consumers.
fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "macos" {
        return;
    }
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let list = std::path::Path::new(&manifest_dir).join("exported-jni.sym");
    println!("cargo:rerun-if-changed=exported-jni.sym");
    println!(
        "cargo:rustc-cdylib-link-arg=-Wl,-exported_symbols_list,{}",
        list.display()
    );
}
