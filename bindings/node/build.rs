fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if target_os == "macos" {
        // Node resolves napi_* symbols at load time; the classic macOS dylib
        // link needs undefined-dynamic-lookup (napi CLI does the same).
        println!("cargo:rustc-link-arg=-undefined");
        println!("cargo:rustc-link-arg=dynamic_lookup");
    }
    if target_os == "linux" && target_env == "gnu" {
        // wasm? no — nothing needed on linux (default exported/undefined ok).
    }
}
