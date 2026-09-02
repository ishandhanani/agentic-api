// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/agent_rt/control.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/agent_rt/control.proto");
    Ok(())
}
