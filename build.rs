use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=macos/music-automation.entitlements.plist");

    let target = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target != "macos" {
        return;
    }

    let test_runtime = env::var_os("CARGO_FEATURE_TEST_RUNTIME").is_some();
    let (bundle_id, executable_name, display_name) = if test_runtime {
        (
            "io.github.telegram-local-downloader.bot.test",
            "telegram-video-downloader-test",
            "Telegram Video Downloader Test",
        )
    } else {
        (
            "io.github.telegram-local-downloader.bot",
            "telegram-video-downloader",
            "Telegram Video Downloader",
        )
    };

    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key><string>en</string>
  <key>CFBundleExecutable</key><string>{executable_name}</string>
  <key>CFBundleIdentifier</key><string>{bundle_id}</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>CFBundleName</key><string>{display_name}</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>0.1.0</string>
  <key>CFBundleVersion</key><string>0.1.0</string>
  <key>NSAppleEventsUsageDescription</key>
  <string>Telegram Video Downloader imports requested audio into Apple Music and reads track metadata, playback state and cloud-library status.</string>
</dict>
</plist>
"#
    );

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let plist_path = out_dir.join("music-automation-Info.plist");
    fs::write(&plist_path, plist).expect("write embedded macOS Info.plist");

    let bin_name = env::var("CARGO_PKG_NAME").expect("Cargo sets package name");
    let linker_arg = format!(
        "-Wl,-sectcreate,__TEXT,__info_plist,{}",
        plist_path.display()
    );
    println!("cargo:rustc-link-arg-bin={bin_name}={linker_arg}");
}
