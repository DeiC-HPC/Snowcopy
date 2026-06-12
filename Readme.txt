Run project with:
run --package snowcopy --bin snowcopy -- my-file.txt lumio:rusty-rust-delete-me5

Add config.json to project file with following structure:

{
  "s3_access_key": "<SomeKey>",
  "s3_secret_key": "<SomeKey>",
  "s3_url": "https://lumidata.eu"
}


Add one the following to run config to log aws client requests:
RUST_LOG=aws_smithy_http=trace cargo run
RUST_LOG=aws_sdk_s3=debug cargo run
RUST_LOG=info cargo run




Useful examples for coding https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/rustv1/examples/s3#code-examples

Build on Linux with:
cargo build --release --target x86_64-unknown-linux-musl

Build on Windows setup:


rustup toolchain install stable-x86_64-pc-windows-msvc
rustup default stable-x86_64-pc-windows-msvc

Install strawberry perl https://strawberryperl.com/
Perl must be in PATH

Open https://visualstudio.microsoft.com/visual-cpp-build-tools/
Install:  Desktop development with C++
You only need the build tools — not the full Visual Studio IDE.

cargo build --release --target x86_64-pc-windows-msvc