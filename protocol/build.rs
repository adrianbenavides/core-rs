use std::path::Path;
use std::process::{self, Command};
use std::str;

fn main() {
    let includes = {
        let mut includes = Vec::new();
        let go_modules = &[
            "go.gazette.dev/core@v0.89.0",
            "github.com/gogo/protobuf@v1.3.2",
        ];
        for module in go_modules {
            let go_list = Command::new("go")
                .args(&["list", "-f", "{{ .Dir }}", "-m", module])
                .stderr(process::Stdio::inherit())
                .output()
                .expect("failed to run 'go'");

            if !go_list.status.success() {
                panic!("go list {} failed", module);
            }

            let dir = str::from_utf8(&go_list.stdout).unwrap().trim_end();
            includes.push(Path::new(dir).to_owned());
        }
        println!("proto_include: {:?}", includes);
        includes
    };
    let protos = {
        let protos = [includes[0].join("broker/protocol/protocol.proto")];
        // Tell cargo to re-run this build script if any of the protobuf files are modified.
        for path in protos.iter() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
        // According to (https://doc.rust-lang.org/cargo/reference/build-scripts.html#rerun-if-changed)
        // setting rerun-if-changed will override the default, so we explicitly tell it to re-run if
        // any files in the crate root are modified.
        println!("cargo:rerun-if-changed=.");
        protos
    };
    tonic_build::configure()
        .out_dir(Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/generated"))
        .compile(&protos, &includes)
        .expect("failed to compile protobuf");
}
