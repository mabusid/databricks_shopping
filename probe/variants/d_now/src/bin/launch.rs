//! The image's entry point, built for the default x86-64 CPU. It replaces itself
//! (exec, so it stays PID 1) with the x86-64-v3 build (AVX2, BMI1/2, POPCNT,
//! LZCNT, FMA, MOVBE, F16C) when this CPU has all of those, else with the default build.
use std::os::unix::process::CommandExt;

fn main() {
    #[cfg(target_arch = "x86_64")]
    let v3 = is_x86_feature_detected!("avx2")
        && is_x86_feature_detected!("bmi1")
        && is_x86_feature_detected!("bmi2")
        && is_x86_feature_detected!("popcnt")
        && is_x86_feature_detected!("lzcnt")
        && is_x86_feature_detected!("fma")
        && is_x86_feature_detected!("movbe")
        && is_x86_feature_detected!("f16c");
    #[cfg(not(target_arch = "x86_64"))]
    let v3 = false;
    let dir = std::env::var("SHOPPING_BIN_DIR").unwrap_or_else(|_| "/app".into());
    let v3_bin = format!("{dir}/shopping-v3");
    let bin = if v3 && std::path::Path::new(&v3_bin).exists() { v3_bin } else { format!("{dir}/shopping-base") };
    let err = std::process::Command::new(&bin).exec();
    eprintln!("exec {bin}: {err}");
    std::process::exit(1);
}
