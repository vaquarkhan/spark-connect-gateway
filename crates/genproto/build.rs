use std::path::PathBuf;

fn main() {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("proto");
    let connect_dir = proto_root.join("spark").join("connect");
    let entries: Vec<PathBuf> = std::fs::read_dir(&connect_dir)
        .expect("read proto dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "proto"))
        .collect();

    for p in &entries {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&entries, &[proto_root])
        .expect("tonic-prost-build compile_protos");
}
