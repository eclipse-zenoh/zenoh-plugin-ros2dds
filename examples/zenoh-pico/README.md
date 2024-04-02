# Examples of Zenoh Pico applications communicating with ROS 2 Nodes


## Messages Subscription: [listener.c](listener.c)

This code mimics the ROS 2 [Topics "listener" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/listener.cpp). It's compatible with the ROS 2 [Topics "talker" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/talker.cpp) running those commands:

- Terminal 1
  ```
  source /opt/ros/humble/local_setup.bash
  export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
  ros2 run demo_nodes_cpp talker
  ```

- Terminal 2
  ```
  source /opt/ros/humble/local_setup.bash
  export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
  zenho-bridge-ros2dds -l tcp/0.0.0.0:7447
  ```

- Terminal 3
  ```
  listener
  ```
  | Here We subscribe to the `rosout` topic because there is a conflict when using `idlc` to compile `std_msgs/msg/String.idl`

## Services Client: [add_two_ints_client.c](add_two_ints_client.c)

This code mimics the ROS 2 [Services "add_two_ints_client" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/services/add_two_ints_client.cpp). It's compatible with the ROS 2 [Services "add_two_ints_server" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/services/add_two_ints_server.cpp) running those commands:


- Terminal 1

  ```
  source /opt/ros/humble/local_setup.bash
  export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
  ros2 run demo_nodes_cpp add_two_ints_server
  ```

- Terminal 2
  ```
  source /opt/ros/humble/local_setup.bash
  zenho-bridge-ros2dds -l tcp/0.0.0.0:7447
  ```

- Terminal 3
  ```
  add_two_ints_client -a 1000 -b 2000
  ```