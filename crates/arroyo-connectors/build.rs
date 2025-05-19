fn main() -> Result<(), String> {
    // recursively find all json files in the src directory
    glob::glob("src/**/*.json")
        .unwrap()
        .filter_map(Result::ok)
        .for_each(|path| {
            println!("cargo:rerun-if-changed={}", path.display());
        });

    // Compile gRPC proto files if the grpc feature is enabled
    #[cfg(feature = "grpc")]
    {
        println!("cargo:rerun-if-changed=proto");
        tonic_build::compile_protos("proto/push.proto")
            .map_err(|e| format!("Failed to compile protos: {}", e))?;
    }

    Ok(())
}
