# Examples of Zenoh Rust applications communicating with ROS 2 Nodes

## Building the examples
In order to build the examples you will need to:
* [Install Rust and Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
* [Install Zenoh Rust](https://github.com/eclipse-zenoh/zenoh?tab=readme-ov-file#how-to-install-it)

Once this is done, compile by running the following:
```
cd examples/rust
cargo build
```

## Messages Publication: [talker.rs](src/bin/talker.rs)

This code mimics the ROS 2 [Topics "talker" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/talker.cpp). It's compatible with the ROS 2 [Topics "listener" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/listener.cpp) running those commands:
- `ros2 run demo_nodes_cpp listener`
- `zenoh-bridge-ros2dds`
- `cargo run --bin talker`

## Messages Subscription: [listener.rs](src/bin/listener.rs)

This code mimics the ROS 2 [Topics "listener" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/listener.cpp). It's compatible with the ROS 2 [Topics "talker" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/talker.cpp) running those commands:
- `ros2 run demo_nodes_cpp talker`
- `zenoh-bridge-ros2dds`
- `cargo run --bin listener`

## Services Client: [add_two_ints_client.rs](src/bin/add_two_ints_client.rs)

This code mimics the ROS 2 [Services "add_two_ints_client" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/services/add_two_ints_client.cpp). It's compatible with the ROS 2 [Services "add_two_ints_server" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/services/add_two_ints_server.cpp) running those commands:
- `ros2 run demo_nodes_cpp add_two_ints_server`
- `zenoh-bridge-ros2dds`
- `cargo run --bin add_two_ints_client`

## Actions Client: [fibonnacci_action_client.rs](src/bin/fibonnacci_action_client.rs)

This code mimics the ROS 2 [Actions "fibonnacci_action_client" demo](https://github.com/ros2/demos/blob/rolling/action_tutorials/action_tutorials_cpp/src/fibonacci_action_client.cpp). It's compatible with the ROS 2 [Actions "fibonnacci_action_server" demo](https://github.com/ros2/demos/blob/rolling/action_tutorials/action_tutorials_cpp/src/fibonacci_action_server.cpp) running those commands:
- `ros2 run action_tutorials_cpp fibonacci_action_server`
- `zenoh-bridge-ros2dds`
- `cargo run --bin fibonnacci_action_client`