# tigerbeetle

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

**Take part in TigerBeetle's $20k consensus challenge: [Viewstamped Replication Made Famous](https://github.com/tigerbeetledb/viewstamped-replication-made-famous)**

Watch an introduction to TigerBeetle on [Zig SHOWTIME](https://www.youtube.com/watch?v=BH2jvJ74npM) for our design decisions regarding performance, safety, and financial accounting primitives:

[![A million financial transactions per second in Zig](https://img.youtube.com/vi/BH2jvJ74npM/0.jpg)](https://www.youtube.com/watch?v=BH2jvJ74npM)

Read more about the [history](./docs/HISTORY.md) of TigerBeetle, the problem of balance tracking at scale, and the solution of a purpose-built financial accounting database.

## TigerBeetle (under active development)

TigerBeetle is not yet production-ready. The production version of
**TigerBeetle is now under active development**. Our [DESIGN
doc](docs/DESIGN.md) provides an overview of TigerBeetle's data
structures.

Check out our
[roadmap](https://github.com/tigerbeetledb/tigerbeetle/issues/259)
below for where we're heading! And [join one of our communities](#Community) to stay
in the loop about fixes and features!

## Documentation

Check out [docs.tigerbeetle.com](https://docs.tigerbeetle.com/).
Here are a few key pages you might be interested in:

- Deployment
  - [Hardware](https://docs.tigerbeetle.com/deploy/hardware)
- Usage
  - [Integration](https://docs.tigerbeetle.com/#designing-for-tigerbeetle)
- Reference
  - [Accounts](https://docs.tigerbeetle.com/reference/accounts)
  - [Transfers](https://docs.tigerbeetle.com/reference/transfers)
  - [Operations](https://docs.tigerbeetle.com/reference/operations)

## Quickstart

TigerBeetle is easy to run with or without Docker, depending on your
preference. First, we'll cover running the [Single
Binary](#single-binary). And below that is how to run [with
Docker](#with-docker).

### Single Binary

Install TigerBeetle by grabbing the latest release from
GitHub.

x86_64 and aarch64 builds are available for macOS and Linux. Only
x86_64 builds are available for Windows.

For example:

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2023-03-27-weekly/tigerbeetle-x86_64-linux-2023-03-27-weekly.zip
$ unzip tigerbeetle-x86_64-linux-2023-03-27-weekly.zip
$ sudo cp tigerbeetle /usr/local/bin/tigerbeetle # On Windows, add $(pwd) to $env:PATH instead.
$ tigerbeetle version --verbose | head -n6
TigerBeetle version experimental

git_commit="55c8fdf1f52c7a174d1bc9d9785cf4e327cae182"

build.mode=Mode.ReleaseSafe
build.zig_version=0.9.1
```

NOTE: This example version is not kept up-to-date. So always check the
[releases](https://github.com/tigerbeetledb/tigerbeetle/releases) page
for the latest version. You can also find debug builds for each
arch/OS combo on the release page as well.

#### Building from source

Or to build from source, clone the repo, checkout a release, and run
the install script.

You will need POSIX userland, curl or wget, tar, and xz.

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ cd tigerbeetle
$ git checkout 2022-11-16-weekly # Or latest tag
$ scripts/install.sh
```

Don't worry, this will only make changes within the `tigerbeetle`
directory. No global changes. The result will place the compiled
`tigerbeetle` binary into the current directory.

#### Running TigerBeetle

Then create the TigerBeetle data file.

```bash
$ ./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start a replica.

```bash
$ ./tigerbeetle start --addresses=3000 0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

Now skip ahead to [using the CLI](#using-the-cli).

### With Docker

First provision TigerBeetle's data directory.

```bash
$ docker run -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle \
    format --cluster=0 --replica=0 --replica-count=1 /data/0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

Then run a replica.

```bash
$ docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000
```

Note: if you are on macOS, you will need to call the Docker run
command with `--cap-add IPC_LOCK` or `--ulimit memlock=-1:-1`. See
[here](https://docs.tigerbeetle.com/deployment/with-docker#error-systemresources-on-macos) for
more information.

### Using the CLI

Now that you've got some replicas running (with or without Docker), let's
connect to the replicas and do some accounting!

First let's create two accounts. (Don't worry about the details, you
can read about them later.)

```console
$ tigerbeetle client --addresses=3000
TigerBeetle Client
  Hit enter after a semicolon to run a command.

Examples:
  create_accounts id=1 code=10 ledger=700,
                  id=2 code=10 ledger=700;
  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
  lookup_accounts id=1;
  lookup_accounts id=1, id=2;

> create_accounts id=1 code=10 ledger=700,
                  id=2 code=10 ledger=700;
info(message_bus): connected to replica 0
```

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```console
> create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
```

Now, the amount of `10` has been credited to account `2` and debited
from account `1`. Let's query TigerBeetle for these two accounts to
verify!

```console
> lookup_accounts id=1, id=2;
{
  "id":              "1",
  "user_data":       "0",
  "ledger":          "700",
  "code":            "10",
  "flags":           "",
  "debits_pending":  "0",
  "debits_posted":   "10",
  "credits_pending": "0",
  "credits_posted":  "0"
}
{
  "id":              "2",
  "user_data":       "0",
  "ledger":          "700",
  "code":            "10",
  "flags":           "",
  "debits_pending":  "0",
  "debits_posted":   "0",
  "credits_pending": "0",
  "credits_posted":  "10"
}
```

And indeed you can see that account `1` has `debits_posted` as `10`
and account `2` has `credits_posted` as `10`. The `10` amount is fully
accounted for!

For further reading:

* [Running a 3-node cluster locally with docker-compose](https://docs.tigerbeetle.com/quick-start/with-docker-compose)
* [Run a single-node cluster with Docker](https://docs.tigerbeetle.com/quick-start/with-docker)
* [Run a single-node cluster](https://docs.tigerbeetle.com/quick-start/single-binary)

## Clients

* For Node.js: [tigerbeetle-node](./src/clients/node)
* For Golang: [tigerbeetle-go](./src/clients/go)
* For Java: [tigerbeetle-java](./src/clients/java)
* For C# and Dotnet: [tigerbeetle-dotnet](./src/clients/dotnet)

## Community

* [Projects using TigerBeetle developed by community members.](./docs/COMMUNITY_PROJECTS.md)
* [Join the TigerBeetle chat on Slack.](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)
* [Follow us on Twitter](https://twitter.com/TigerBeetleDB), [YouTube](https://www.youtube.com/@tigerbeetledb), and [Twitch](https://www.twitch.tv/tigerbeetle).
* [Subscribe to our monthly newsletter for the backstory on recent database changes.](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)
* [Check out past and upcoming talks.](/docs/TALKS.md)

## Benchmarks

First grab the sources and run the setup script:

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ cd tigerbeetle
$ scripts/install.sh
```

With TigerBeetle installed, you are ready to benchmark!

```bash
$ scripts/benchmark.sh
```

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Contributing

Read [docs/HACKING.md](docs/HACKING.md).

## Performance Demos

Along the way, we also put together a series of performance demos and sketches to get you comfortable building TigerBeetle, show how low-level code can sometimes be easier than high-level code, help you understand some of the key components within TigerBeetle, and enable back-of-the-envelope calculations to motivate design decisions.

You may be interested in:

* [demos/protobeetle](./demos/protobeetle), how batching changes everything.
* [demos/bitcast](./demos/bitcast), how Zig makes zero-overhead network deserialization easy, fast and safe.
* [demos/io_uring](./demos/io_uring), how ring buffers can eliminate kernel syscalls, reduce server hardware requirements by a factor of two, and change the way we think about event loops.
* [demos/hash_table](./demos/hash_table), how linear probing compares with cuckoo probing, and what we look for in a hash table that needs to scale to millions (and billions) of account transfers.

## Roadmap

See https://github.com/tigerbeetledb/tigerbeetle/issues/259.

## License

Copyright 2023 TigerBeetle, Inc

Copyright 2020-2022 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
