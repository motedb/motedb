fn main() {
    // macOS extension modules link against the host interpreter's symbols
    // at load time; allow undefined lookups when building the cdylib.
    // 🔑 macOS ONLY: on Linux this pair makes rust-lld fail with
    // "cannot open dynamic_lookup" (rustc ≥1.91 wires -fuse-ld=lld by
    // default), breaking every Linux wheel build.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
