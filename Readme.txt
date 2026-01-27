Run project with:
run --package snowplowRust --bin snowplowRust -- -b rust-test ./Readme.txt


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