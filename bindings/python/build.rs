fn main() {
    // macOS extension modules link against the host interpreter's symbols
    // at load time; allow undefined lookups when building the cdylib.
    println!("cargo:rustc-cdylib-link-arg=-undefined");
    println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
}
