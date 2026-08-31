# Snowcopy
Snowcopy is an easy-to-use, fast S3 transfer command line tool with strong data integrity checking.

Main features:
* Auto-tuning parallelization and memory usage based on client hardware, for effortless performance out of the box.
* Data integrity verification for *both* upload and download.
* Very fast transfers. See [benchmarks](https://cdn-icons-png.flaticon.com/512/5578/5578703.png).
* Graphic tui for easy config management
* Incremental transfers: only transfer files not already the destination

## Alpha limitation
There are some known limitations in the alpha, with the current features:

* Snowcopy cannot currently transfer files larger than 50GB.
  * If a file is larger than 50GB is seen, Snowcopy skips it, transfers everything else, then exits with a non-zero status and an error message.
* The amount of memory used by Snowcopy is auto-tuned, but cannot currently be manually configured. It's tuned excessively for the actual needs and will be more optimized at a later time.
* Using a file descriptor limit less than 1024 might crash the program.

We are especially interested in feedback about any instability or correctness issues experienced. Any general feedback is also welcome to lukas.dalgaard@deic.dk or jon.jacobsen@deic.dk

## Release
TODO: link to release and information

## Usage
Before first use, configure a S3 server by running:
```
snowcopy config
```
Which will display a clickable TUI for the user to input the credentials and URL information:
![config tui](readme/config-example.png)


When transferring files, Snowcopy use the [src:dst] pattern:

```
Upload:
	snowcopy cp my-folder profile:bucket
	snowcopy cp file.txt profile:bucket

Download:
	snowcopy cp profile:bucket my-folder

List buckets on server:
	snowcopy ls profile:
	
List files in bucket:
	snowcopy ls profile:bucket

Remove/delete bucket:
	snowcopy rm profile:bucket
```

## Features
Snowcopy preserves modification time of transferred files. It uses the same metadata scheme for it as rclone and is thus compatible with files uploaded by rclone.

### Auto-tuning
Snowcopy scans the computer hardware and network link to determine how the transfer should be configured. It will display the auto-tuned parameters in the console:
![config tui](readme/auto-tuning-example.png)

**WIP**:
It is possible to tune parameters by hand using flags when running the program see help for more information:
```
snowcopy help
```

### Incremental upload
Snowcopy uses file name, size and modification time to determine if a file is already present at the destination. If any file is found to already be at the target, a second progressbar will be displayed:
![Incremental transfer progress bar](readme/incremental-transfer-example.png)

The "total" progress bar will show the total number of files already present at the destination. It can be files that were here before the transfer started or files that has been transferred by the running command. "Existing" is the amount of data that already exist and "at target" is the total newly transferred + existing. 

### Integrity
Snowcopy validates the integrity of all transfers by either md5 or SHA256 depending on the situation. It does so by providing hashes of the content in all uploads. The S3 server then validates that it has received the same content and returns an error if there is a mismatch between the hash and the content.
For downloads, snowcopy stores the necessary metadata to verify the hash against the downloaded file, on the object at the S3 server. Snowcopy can then download the file and verify the hash against the downloaded file.
If there is a mismatch between the hash and the transferred content, the transfer will be retried. If the missmatch is persistent, then snowcopy will abort the transfer with a non-zero exit code.

## Error codes
Snowcopy can exit an operation with errors in two different ways:

![Soft error](readme/soft-error.png)

A soft error is where something could not be done, but the rest of the operation is still possible to continue. An example could be a too large file here in the alpha or a S3 object with a key that is not valid as a file on the local file system. The log will contain specific information. A user can trust that the files transferred are correct, but that the transfer is incomplete.

![Fatal error](readme/fatal-error.png)

A fatal error will be shown when an irrecoverable situation is encountered. An example could be a disk full error. A user **cannot** trust the state of transferred files at the target.

### List of non-zero exit codes:
 ```
  Unknown = 1
  ServerStateChangeDuringExecution = 2
  RequestFailure = 3
  DiskIoError = 4
  HttpUnsafeError = 5
  SkippedInvalidKeysDownload = 6
  AlphaFileSizeLimitExceeded = 7
 ```

## Build
TODO: 

## License
TODO: EUPL
