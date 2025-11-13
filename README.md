<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/zenoh-dragon.png" height="150">

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
- Configuration of a ROS namespace on the bridge, instead of on each ROS Nodes
- Easier integration with Zenoh native applications (services and actions are mapped to Zenoh Queryables)
- More compact exchanges of discovery information between the bridges

## Plugin or bridge ?

This software is built in 2 ways to choose from:

- `zenoh-plugin-ros2dds`: a Zenoh plugin - a dynamic library that can be loaded by a Zenoh router
- `zenoh-bridge-ros2dds`: a standalone executable

The features and configurations described in this document applies to both.
Meaning the _"plugin"_ and _"bridge"_  words are interchangeables in the rest of this document.

## How to install it

To install the latest release of either the DDS plugin for the Zenoh router, either the `zenoh-bridge-ros2dds` standalone executable, you can do as follows:

### Manual installation (all platforms)

All release packages can be downloaded from:  

- [https://download.eclipse.org/zenoh/zenoh-plugin-ros2dds/latest/](https://download.eclipse.org/zenoh/zenoh-plugin-ros2dds/latest/)

Each subdirectory has the name of the Rust target. See the platforms each target corresponds to on [https://doc.rust-lang.org/stable/rustc/platform-support.html](https://doc.rust-lang.org/stable/rustc/platform-support.html)

Choose your platform and download:

- the `zenoh-plugin-ros2dds-<version>-<platform>.zip` file for the plugin.  
  Then unzip it in the same directory than `zenohd` or to any directory where it can find the plugin library (e.g. /usr/lib)
- the `zenoh-bridge-ros2dds-<version>-<platform>.zip` file for the standalone executable.  
  Then unzip it where you want, and run the extracted `zenoh-bridge-ros2dds` binary.

### Linux Debian

Add Eclipse Zenoh private repository to the sources list:

```bash
curl -L https://download.eclipse.org/zenoh/debian-repo/zenoh-public-key | sudo gpg --dearmor --yes --output /etc/apt/keyrings/zenoh-public-key.gpg
echo "deb [signed-by=/etc/apt/keyrings/zenoh-public-key.gpg] https://download.eclipse.org/zenoh/debian-repo/ /" | sudo tee -a /etc/apt/sources.list > /dev/null
sudo apt update
```

Then either:

- install the plugin with: `sudo apt install zenoh-plugin-ros2dds`.
- install the standalone executable with: `sudo apt install zenoh-bridge-ros2dds`.

### Docker images

The **`zenoh-bridge-ros2dds`** standalone executable is also available as a [Docker images](https://hub.docker.com/r/eclipse/zenoh-bridge-ros2dds/tags?page=1&ordering=last_updated) for both amd64 and arm64. To get it, do:

- `docker pull eclipse/zenoh-bridge-ros2dds:latest` for the latest release
- `docker pull eclipse/zenoh-bridge-ros2dds:nightly` for the main branch version (nightly build)

### Nightly builds

The ["Release" action](https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/actions/workflows/release.yml) builds packages for most most of OSes. You can download those from the "Artifacts" section in each build.  
Just download the package for your OS and unzip it. You'll get 3 zips: 1 for the plugin, 1 for the plugin as debian package and 1 for the bridge.
Unzip the `zenoh-bridge-ros2dds-<platform>.zip` file, and you can run `./zenoh-bridge-ros2dds`

## How to build it

> :warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the plugins should be
built with the exact same Rust version than `zenohd`, and using for `zenoh` dependency the same version (or commit number) than 'zenohd'.
Otherwise, incompatibilities in memory mapping of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

In order to build the zenoh bridge for DDS you need first to install the following dependencies:

- [Rust](https://www.rust-lang.org/tools/install). If you already have the Rust toolchain installed, make sure it is up-to-date with:

  ```bash
  rustup update
  ```

- On Linux, make sure the `llvm` and `clang` development packages are installed:
  - on Debians do: `sudo apt install llvm-dev libclang-dev`
  - on CentOS or RHEL do: `sudo yum install llvm-devel clang-devel`
  - on Alpine do: `apk install llvm11-dev clang-dev`
- [CMake](https://cmake.org/download/) (to build CycloneDDS which is a native dependency)

Once these dependencies are in place, you may clone the repository on your machine:

```bash
git clone https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds.git
cd zenoh-plugin-ros2dds
cargo build --release
```

The standalone executable binary `zenoh-bridge-ros2dds` and a plugin shared library (`*.so` on Linux, `*.dylib` on Mac OS, `*.dll` on Windows)
to be dynamically loaded by the zenoh router `zenohd` will be generated in the `target/release` subdirectory.

### DDS Library Symbol Prefixing

DDS support is provided by the [cyclors crate](https://crates.io/crates/cyclors). As this crate contains C code, symbol clashes may occur when loading the plugin statically with other plugins which use a different version of the ```cyclors``` crate (e.g. the ```zenoh-plugin-dds``` plugin).

To allow multiple versions of the ```cyclors``` crate to be loaded at the same time the symbols within the crate can be prefixed with the crate version. The optional ```prefix_symbols``` feature can be used to build the ROS2 plugin with prefixed DDS library symbols. e.g.

```bash
cargo build --features prefix_symbols
```

**Note:** The ```prefix_symbols``` feature cannot be used at the same time as the ```dds_shm``` feature.

## ROS 2 package

You can also build `zenoh-bridge-ros2dds` as a ROS package running:

```bash
rosdep install --from-paths . --ignore-src -r -y
colcon build --packages-select zenoh_bridge_ros2dds --cmake-args -DCMAKE_BUILD_TYPE=Release
```

The `rosdep` command will automatically install _Rust_ and _clang_ as build dependencies.

If you want to cross-compile the package on x86 device for any target, you can use the following command:

```bash
rosdep install --from-paths . --ignore-src -r -y
colcon build --packages-select zenoh_bridge_ros2dds --cmake-args -DCMAKE_BUILD_TYPE=Release  --cmake-args -DCROSS_ARCH=<target>
```

where `<target>` is the target architecture (e.g. `aarch64-unknown-linux-gnu`). The architecture list can be found [here](https://doc.rust-lang.org/nightly/rustc/platform-support.html).

The cross-compilation uses `zig` as a linker. You can install it with instructions in [here](https://ziglang.org/download/). Also, the `zigbuild` package is required to be installed on the target device. You can install it with instructions in [here](https://github.com/rust-cross/cargo-zigbuild#installation).

-------------------------------

# Usage

A typical usage is to run 1 bridge in a robot, and 1 bridge in another host monitoring and operating the robot.

> :warning: The bridge relies on [CycloneDDS](https://github.com/eclipse-cyclonedds/cyclonedds) and has been tested with `RMW_IMPLEMENTATION=rmw_cyclonedds_cpp`. While the DDS implementations are interoperable over UDP multicast and unicast, some specific and non-standard features of other DDS implementations (e.g. shared memory) might cause some issues.

It's important to make sure that NO DDS communication can occur between 2 hosts that are bridged by `zenoh-bridge-ros2dds`. Otherwise, some duplicate or looping traffic can occur.  
To make sure of this, you can either:

- use ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST after Iron and ROS_LOCALHOST_ONLY=1 before Iron.  
  Preferably, enable MULTICAST on the loopback interface with this command (on Linux): `sudo ip l set lo multicast on`
- use different `ROS_DOMAIN_ID` on each hosts
- use a `CYCLONEDDS_URI` that configures CycloneDDS to only use internal interfaces to the robot. This configuration has to be used for all ROS Nodes as well as for the bridge.  
  For instance for Turtlebot4 which embeds 2 hosts interconnected via USB:  

   ```xml
   <CycloneDDS>
    <Domain>
        <General>
            <Interfaces>
                <NetworkInterface name="usb0"/>
                <!-- For less traffic, force multicast usage on loopback even if not configured.         -->
                <!-- All ROS Nodes and bridges must have this same config, otherwise they won't discover -->
                <NetworkInterface address="127.0.0.1" multicast="true"/> 
            </Interfaces>
            <DontRoute>true</DontRoute>
        </General>
    </Domain>
  </CycloneDDS>
  ```

On the robot, run:

- `zenoh-bridge-ros2dds`

On the operating host run:

- `zenoh-bridge-ros2dds -e tcp/<robot-ip>:7447`
- check if the robot's ROS interfaces are accessible via:
  - `ros2 topic list`
  - `ros2 service list`
  - `ros2 action list`

Other interconnectivity between the 2 bridges can be configured (e.g. automatic discovery via UDP multicast, interconnection via 1 or more Zenoh routers...).
See the [Zenoh documentation](https://zenoh.io/docs/getting-started/deployment/) to learn more about the possible deployments allowed by Zenoh.

## Configuration

`zenoh-bridge-ros2dds` can be configured via a JSON5 file passed via the `-c` argument. You can see a commented and exhaustive example of such configuration file: [`DEFAULT_CONFIG.json5`](DEFAULT_CONFIG.json5).

The `"ros2dds"` part of this same configuration file can also be used in the configuration file for the zenoh router (within its `"plugins"` part). The router will automatically try to load the plugin library (`zenoh-plugin_dds`) at startup and apply its configuration.

`zenoh-bridge-ros2dds` also allows some of those configuration values to be configured via command line arguments. Run this command to see which ones:

- `zenoh-bridge-ros2dds -h`

The command line arguments overwrite the equivalent keys configured in a configuration file.

## Connectivity configurations

### DDS communications

The bridge discovers all ROS 2 Nodes and their topics/services/actions running on the same Domain ID (set via `ROS_DOMAIN_ID` or `0` by default) via UDP multicast, as per DDS specification.

As the bridge relies on [CycloneDDS](https://github.com/eclipse-cyclonedds/cyclonedds), it's DDS communications can be configured via a CycloneDDS XML configuration file as explained [here](https://github.com/eclipse-cyclonedds/cyclonedds/tree/8638e1fa1e8ae9f964c91ad4763653702b8f91e0?tab=readme-ov-file#run-time-configuration).

### Zenoh communications

Starting from **v0.11.0**, the `zenoh-bridge-ros2dds` is by default started in `router` mode (See the difference between modes in [Zenoh documentation](https://zenoh.io/docs/getting-started/deployment/)).
This means it's listening for incoming TCP connections by remote bridges or any Zenoh application on port `7447` via any network interface. It does perform discovery via scouting over UDP multicast or gossip protocol, but doesn't auto-connect to anything.  
As a consequence the connectivity between bridges has to be statically configured in one bridge connecting to the other (or several other bridges) via the `-e` command line option, or via the `connect` section in [configuration file](DEFAULT_CONFIG.json5).

If required, the automatic connection to other discovered bridges (also running in `router` mode) can be enabled adding such configuration:

```json5
scouting: {
  multicast: {
    autoconnect: { router: "router" }
  },
  gossip: {
    autoconnect: { router: "router" }
  }
},
```

Prior to **v0.11.0**, the `zenoh-bridge-ros2dds` was by default started in `peer` mode.  
It was listening for incoming TCP connections on a random port (chosen by the OS), and was automatically connecting to any discovered bridge, router or peer.

## Easy multi-robots via Namespace configuration

Deploying a `zenoh-bridge-ros2dds` in each robot and configuring each with its own namespace brings several benefits:

1. No need to configure each ROS Node with a namespace. As the DDS traffic between all Nodes of a single robot remains internal to the robot, no namespace needs to be configured
2. Configuring each `zenoh-bridge-ros2dds` with `namespace: "/botX"` (where `'X'` is a unique id), each topic/service/action name routed to Zenoh is prefixed with `"/botX"`. Robots messages are not conflicting with each other.
3. On a monitoring/controlling host, you have 2 options:
   - Run a `zenoh-bridge-ros2dds` with `namespace: "/botX"` corresponding to 1 robot. Then to monitor/operate that specific robot, just any ROS Node without namespace.  
   E.g.: `rviz2`
   - Run a `zenoh-bridge-ros2dds` without namespace. Then you can monitor/operate any robot remapping the namespace to the robot's one, or each topic/service/action name you want to use adding the robot's namespace as a prefix.  
  E.g.:  `rviz2 --ros-args -r /tf:=/botX2/tf -r /tf_static:=/botX/tf_static`

NOTE: the bridge prefixes ALL topics/services/actions names with the configured namespace, including `/rosout`, `/parameter_events`, `/tf` and `/tf_static`.

## Admin space

The bridge exposes some internal states via a Zenoh admin space under `@/<id>/ros2/**`, where `<id>` is the unique Zenoh ID of the bridge (configurable).  
This admin space can be queried via Zenoh `get()` operation. If the REST plugin is configured for the bridge for instance via `--rest-http-port 8000` argument, such URLs can be queried:

- `http://\<bridge-IP\>:8000/@/local/ros2/node/**` : to get all ROS nodes with their interfaces discovered by the bridge
- `http://\<bridge-IP\>:8000/@/local/ros2/dds/**` : to get all the DDS Readers/Writers discovered by the bridge
- `http://\<bridge-IP\>:8000/@/local/ros2/route/**` : to get all routes between ROS interfaces and Zenoh established by the bridge
- `http://\<bridge-IP\>:8000/@/*/ros2/node/**` : to get all ROS nodes discovered by all bridges
- `http://\<bridge-IP\>:8000/@/*/ros2/route/**/cmd_vel` : to get all routes between established by all bridges on `cmd_vel` topic
