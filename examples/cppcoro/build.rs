// cxx-async/examples/cppcoro/build.rs

use pkg_config::Config;

fn main() {
    let cppcoro = Config::new().probe("cppcoro").unwrap();

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=include/cppcoro_example.h");
    println!("cargo:rerun-if-changed=src/cppcoro_example.cpp");

    cxx_build::bridge("src/main.rs")
        .file("src/cppcoro_example.cpp")
        .flag("-std=c++20")
        .flag_if_supported("-Wall")
        .include("include")
        .include("../common/include")
        .include("../../cxx-async/include")
        .includes(&cppcoro.include_paths)
        .compile("cppcoro_example");
}
