<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/master/zenoh-dragon.png" height="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/workflows/Release/badge.svg)](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/actions/workflows/release.yml)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/2GJ958VuHs)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Eclipse Zenoh
The Eclipse Zenoh: Zero Overhead Pub/sub, Store/Query and Compute.

Zenoh (pronounce _/zeno/_) unifies data in motion, data at rest and computations. It carefully blends traditional pub/sub with geo-distributed storages, queries and computations, while retaining a level of time and space efficiency that is well beyond any of the mainstream stacks.

Check the website [zenoh.io](http://zenoh.io) and the [roadmap](https://github.com/eclipse-zenoh/roadmap) for more detailed information.


-------------------------------
# A Zenoh bridge for ROS 2 over DDS

[ROS](https://ros.org/) (the Robot Operating System) is a set of software libraries and tools allowing to build robotic applications. In its version 2, ROS 2 relies mostly on [O.M.G. DDS](https://www.dds-foundation.org/) as a [middleware for communications](https://docs.ros.org/en/humble/Concepts/Intermediate/About-Different-Middleware-Vendors.html). This plugin bridges all ROS 2 communications using DDS over Zenoh.

While a Zenoh bridge for DDS already [exists](https://github.com/eclipse-zenoh/zenoh-plugin-dds) and helped lot of robotic use cases to overcome some [wireless connectivity](https://zenoh.io/blog/2021-03-23-discovery/), [bandwidth](https://zenoh.io/blog/2021-09-28-iac-experiences-from-the-trenches/) and [integration](https://zenoh.io/blog/2021-11-09-ros2-zenoh-pico/) issues, using a bridge dedicated to ROS 2 brings the following advantages:

 - A better integration of the ROS graph (all ROS topics/services/actions can be seen across bridges)
 - A better support of ROS toolings (`ros2`, `rviz2`...)
 - Configuration of a ROS namespace to be prepended to all routed topics/services/actions
 - Easier integration with Zenoh native applications (services and actions are mapped to Zenoh Queryables)
 - More compact exchanges of discovery information between the bridges

## Plugin or bridge ?

This software is built in 2 ways to choose from:
 - `zenoh-plugin-dds`: a Zenoh plugin - a dynamic library that can be loaded by a Zenoh router
 - `zenoh-bridge-dds`: a standalone executable

## How to install it

No version has been released yet. Therefore only nightly built packages are available.

The ["Release" action](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/actions/workflows/release.yml) builds packages for most most of OSes. You can download those from the "Artifacts" section in each build.  
Just download the package for your OS, unzip it. You'll get 3 zips: 1 for the plugin, 1 for the plugin as debian package and 1 for the bridge.
Unzip the `zenoh-bridge-dds-<platform>.zip` file, and you can run `./zenoh-bridge-dds`

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


# Usage

A typical usage is to run 1 bridge in a robot, and 1 bridge in another host monitoring and operating the robot.

> :warning: The bridge relies on [CycloneDDS](https://github.com/eclipse-cyclonedds/cyclonedds) and has been tested with `RMW_IMPLEMENTATION=rmw_cyclonedds_cpp`. While the DDS implementations are interoperable over UDP multicast and unicast, some specific and non-standard features of other DDS implementations (e.g. shared memory) might cause some issues.

It's important to make sure that NO DDS communication can occur between 2 hosts that are bridged by `zenoh-bridge-ros2dds`. Otherwise, some duplicate or looping traffic can occur.  
To make sure of this, you can either:
 - define `ROS_LOCALHOST_ONLY=1`
 - use different `ROS_DOMAIN_ID` on each hosts
 - use a `CYCLONEDDS_URI` that configures CycloneDDS to only use internal interfaces to the robot.  
   For instance for Turtlebot4 which embedds 2 hosts interconnected via USB:  
   ```xml
   <CycloneDDS>
    <Domain>
        <General>
            <Interfaces>
                <NetworkInterface name="usb0" priority="default" multicast="default"/>
                <NetworkInterface address="127.0.0.1" multicast="true"/>
            </Interfaces>
            <DontRoute>true</DontRoute>
        </General>
    </Domain>
  </CycloneDDS>
   ```

On the robot, run:
  - `zenoh-bridge-dds -l tcp/0.0.0.0:7447`

On the operating host run:
  - `zenoh-bridge-dds -e tcp/<robot-ip>:7447`
  - check if the robot's ROS interfaces are accessible via:
    - `ros2 topic list`
    - `ros2 service list`
    - `ros2 action list`



Other interconnectivity between the 2 bridges can be configured (e.g. automatic discovery via UDP multicast, interconnection via 1 or more Zenoh routers...).
See the [Zenoh documentation](https://zenoh.io/docs/getting-started/deployment/) to learn more about the possibile deployments allowed by Zenoh.


## Configuration

`zenoh-bridge-ros2dds` can be configured via a JSON5 file passed via the `-c` argument. You can see a commented and exhaustive example of such configuration file: [`DEFAULT_CONFIG.json5`](DEFAULT_CONFIG.json5).

The `"ros2dds"` part of this same configuration file can also be used in the configuration file for the zenoh router (within its `"plugins"` part). The router will automatically try to load the plugin library (`zenoh-plugin_dds`) at startup and apply its configuration.

`zenoh-bridge-dds` also allows some of those configuration values to be configured via command line arguments. Run this command to see which ones:  
- `zenoh-bridge-dds -h`

The command line arguments overwrite the equivalent keys configured in a configuration file.

:warning: Work in progress... the configuration scheme might change before the first release !

