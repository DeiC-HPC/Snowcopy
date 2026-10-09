### Useful examples:
Useful examples for coding https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/rustv1/examples/s3#code-examples

### Run project with (Rust rover):
run --package snowcopy --bin snowcopy -- my-file.txt lumio:rusty-rust-delete-me5

### Build on Linux with:
cargo build --release --target x86_64-unknown-linux-musl

### Build on Windows setup:
rustup toolchain install stable-x86_64-pc-windows-msvc
rustup default stable-x86_64-pc-windows-msvc

Install strawberry perl https://strawberryperl.com/
Perl must be in PATH

Open https://visualstudio.microsoft.com/visual-cpp-build-tools/
Install:  Desktop development with C++
You only need the build tools — not the full Visual Studio IDE.

cargo build --release --target x86_64-pc-windows-msvc

### Generate Third party notice licences
We use cargo about. Install it as so:
cargo install cargo-about --features cli

Generate like so:
<Windows PowerShell> cargo about generate about.hbs -o THIRD-PARTY-NOTICES.html
<Linux> cargo about generate about.hbs > THIRD-PARTY-NOTICES.html

This uses the about.toml for which licences are allowed.

### Check known security vulnerabilities an licences
We use cargo deny. Install it as so:
cargo install cargo-deny

Check like so:
cargo deny check

This uses the deny.toml for which licences are allowed.