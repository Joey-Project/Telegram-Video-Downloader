use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_NATIVE");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if env::var_os("CARGO_FEATURE_NATIVE").is_none() || target_os != "macos" {
        return;
    }

    pkg_config::Config::new()
        .atleast_version("1.4.0")
        .statik(true)
        .probe("flac")
        .unwrap_or_else(|error| {
            panic!("the macOS `native` feature requires libFLAC via pkg-config: {error}")
        });

    println!("cargo:rustc-link-lib=framework=AudioToolbox");
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
}
