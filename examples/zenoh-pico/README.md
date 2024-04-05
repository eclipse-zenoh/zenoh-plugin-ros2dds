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

## How to use idlc to compile *.idl 

> rmw_cyclonedds_cpp doesn't expect XCDR encoding, but CDR encoding. Thus, we shall use -x final with idlc command.

> ROS2 env must be set up first. ( Iron recommended )

```
find_package(CycloneDDS REQUIRED)
find_package(rcl_interfaces REQUIRED)
find_package(builtin_interfaces REQUIRED)

foreach(_idl ${rcl_interfaces_IDL_FILES})
    configure_file(${rcl_interfaces_DIR}/../${_idl} ${IDL_OUT_PATH}/rcl_interfaces/${_idl} COPYONLY)
    list(APPEND IDL_FILES "${IDL_OUT_PATH}/rcl_interfaces/${_idl}")
endforeach()

foreach(_idl ${builtin_interfaces_IDL_FILES})
    configure_file(${builtin_interfaces_DIR}/../${_idl} ${IDL_OUT_PATH}/builtin_interfaces/${_idl} COPYONLY)
    list(APPEND IDL_FILES "${IDL_OUT_PATH}/builtin_interfaces/${_idl}")
endforeach()

foreach(_idl ${IDL_FILES})    
    execute_process(
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/src
        COMMAND idlc -I ${CMAKE_BINARY_DIR}/idl -o ${CMAKE_BINARY_DIR}/src -b ${CMAKE_BINARY_DIR}/idl -x final ${_idl} 
        OUTPUT_VARIABLE cmd_output
    )
endforeach()
```