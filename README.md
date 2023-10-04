<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/master/zenoh-dragon.png" height="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/workflows/Release/badge.svg)](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/actions/workflows/release.yml)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/2GJ958VuHs)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


# zplugin-ros2dds

A new Zenoh bridge for ROS2.

:warning: **Work in progress...** :warning:

## How to install it

No version has been released yet. Therefore only nightly built packages are available.

The ["Release" action](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/actions/workflows/release.yml) builds packages for most most of OSes. You can download those from the "Artifacts" section in each build.  
Just download the package for your OS, unzip it and run `./zenoh-bridge-ros2dds`.

A nightly build Docker image is also available. Pull it with this command: `docker pull eclipse/zenoh-bridge-ros2dds:nightly`


## How to build it

> :warning: **WARNING** :warning: : Zenoh and its ecosystem are under active development. When you build from git, make sure you also build from git any other Zenoh repository you plan to use (e.g. binding, plugin, backend, etc.). It may happen that some changes in git are not compatible with the most recent packaged Zenoh release (e.g. deb, docker, pip). We put particular effort in mantaining compatibility between the various git repositories in the Zenoh project.

> :warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the plugins should be
built with the exact same Rust version than `zenohd`, and using for `zenoh` dependency the same version (or commit number) than 'zenohd'.
Otherwise, incompatibilities in memory mapping of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

In order to build the zenoh bridge for DDS you need first to install the following dependencies:

- [Rust](https://www.rust-lang.org/tools/install). If you already have the Rust toolchain installed, make sure it is up-to-date with:

  ```bash
  $ rustup update
  ```

- On Linux, make sure the `llvm` and `clang` development packages are installed:
   - on Debians do: `sudo apt install llvm-dev libclang-dev`
   - on CentOS or RHEL do: `sudo yum install llvm-devel clang-devel`
   - on Alpine do: `apk install llvm11-dev clang-dev`
- [CMake](https://cmake.org/download/) (to build CycloneDDS which is a native dependency)

Once these dependencies are in place, you may clone the repository on your machine:

```bash
$ git clone https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds.git
$ cd zenoh-plugin-ros2dds
```
> :warning: **WARNING** :warning: : On Linux, don't use `cargo build` command without specifying a package with `-p`. Building both `zenoh-plugin-ros2dds` (plugin library) and `zenoh-bridge-ros2dds` (standalone executable) together will lead to a `multiple definition of `load_plugin'` error at link time. See [#117](https://github.com/eclipse-zenoh/zenoh-plugin-dds/issues/117#issuecomment-1439694331) for explanations.

You can then choose between building the zenoh bridge for DDS:
- as a plugin library that can be dynamically loaded by the zenoh router (`zenohd`):
```bash
$ cargo build --release -p zenoh-plugin-ros2dds
```
The plugin shared library (`*.so` on Linux, `*.dylib` on Mac OS, `*.dll` on Windows) will be generated in the `target/release` subdirectory.

- or as a standalone executable binary:
```bash
$ cargo build --release -p zenoh-bridge-ros2dds
```
The **`zenoh-bridge-ros2dds`** binary will be generated in the `target/release` sub-directory.


## Configuration

`zenoh-bridge-ros2dds` can be configured via a JSON5 file passed via the `-c`argument. You can see a commented example of such configuration file: [`DEFAULT_CONFIG.json5`](DEFAULT_CONFIG.json5).

:warning: Work in progress... might change with any commit!
