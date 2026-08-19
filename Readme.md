# Snowcopy
Snowcopy is an easy-to-use, fast S3 transfer command line tool with strong data integrity checking.

Main features:
* Auto-tuning parallelization and memory usage based on client hardware, for effortless performance out of the box.
* Data integrity verification for *both* upload and download.
* Very fast transfers. See [benchmarks](https://cdn-icons-png.flaticon.com/512/5578/5578703.png).
* Graphic tui for easy config management
* Incremental transfers: only transfer files not already the destination

# Release
TODO: link to release and information

# Usage
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

# Features
Snowcopy preserves modification time of transferred files. It uses the same metadata scheme for it as rclone and is thus compatible with rclone.

## Auto-tuning
Snowcopy scans the computer hardware and network link to determine how the transfer should be configured. It will display the auto tuned parameters in the console:
![config tui](readme/auto-tuning-example.png)

**WIP**:
Its possible to tune parameters by hand using flags when running the program see help for more information:
```
snowcopy help
```

## Incremental upload
Snowcopy uses file name, size and modification time to determine if a file is already present at the destination.

## Integrity
Snowcopy validates the integrity of all transfers by either md5 or SHA256 depending of the situation.

# Build
TODO: 

# License
TODO: EUPL
