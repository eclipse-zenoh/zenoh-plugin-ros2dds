# Examples of Zenoh Python applications communicating with ROS 2 Nodes


## Messages Publication: [talker.py](src/talker.py)

This code mimics the ROS 2 [Topics "talker" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/talker.cpp). It's compatible with the ROS 2 [Topics "listener" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/listener.cpp) running those commands:
- `ros2 run demo_nodes_cpp listener`
- `zenoh-bridge-ros2dds`
- `python ./talker.py`

## Messages Subscription: [listener.py](src/listener.py)

This code mimics the ROS 2 [Topics "listener" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/listener.cpp). It's compatible with the ROS 2 [Topics "talker" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/topics/talker.cpp) running those commands:
- `ros2 run demo_nodes_cpp talker`
- `zenoh-bridge-ros2dds`
- `python ./listener.py`

## Services Client: [add_two_ints_client.py](src/add_two_ints_client.py)

This code mimics the ROS 2 [Services "add_two_ints_client" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/services/add_two_ints_client.cpp). It's compatible with the ROS 2 [Services "add_two_ints_server" demo](https://github.com/ros2/demos/blob/rolling/demo_nodes_cpp/src/services/add_two_ints_server.cpp) running those commands:
- `ros2 run demo_nodes_cpp add_two_ints_server`
- `zenoh-bridge-ros2dds`
- `python ./add_two_ints_client.py`

## Actions Client: [fibonnacci_action_client.py](src/fibonnacci_action_client.py)

This code mimics the ROS 2 [Actions "fibonnacci_action_client" demo](https://github.com/ros2/demos/blob/rolling/action_tutorials/action_tutorials_cpp/src/fibonacci_action_client.cpp). It's compatible with the ROS 2 [Actions "fibonnacci_action_server" demo](https://github.com/ros2/demos/blob/rolling/action_tutorials/action_tutorials_cpp/src/fibonacci_action_server.cpp) running those commands:
- `ros2 run action_tutorials_cpp fibonacci_action_server`
- `zenoh-bridge-ros2dds`
- `python ./fibonnacci_action_client.py`
