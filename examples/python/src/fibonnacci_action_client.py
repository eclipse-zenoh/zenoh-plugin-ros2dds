#
# Copyright (c) 2024 ZettaScale Technology
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
# which is available at https://www.apache.org/licenses/LICENSE-2.0.
#
# SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
#
# Contributors:
#   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
#
import time
import argparse
import zenoh

# pycdr2 is the serializer of data in CDR format (required by ROS2)
import pycdr2
from pycdr2 import IdlStruct
from dataclasses import dataclass


@dataclass
class Time(IdlStruct, typename="Time"):
    sec: pycdr2.types.uint32
    nsec: pycdr2.types.uint32

# Equivalent to Fibonnaci.Goal.Request class, but serializable by pycdr2
@dataclass
class Fibonacci_SendGoal_Request(IdlStruct, typename="Fibonacci_SendGoal_Request"):
    goal_id: pycdr2.types.array[pycdr2.types.uint8, 16]
    order: pycdr2.types.int32

# Equivalent to Fibonnaci.Goal.Response class, but serializable by pycdr2
@dataclass
class Fibonacci_SendGoal_Response(IdlStruct, typename="Fibonacci_SendGoal_Response"):
    accepted: bool
    stamp: Time

# Equivalent to Fibonnaci.Goal.Request class, but serializable by pycdr2
@dataclass
class Fibonacci_GetResult_Request(IdlStruct, typename="Fibonacci_GetResult_Request"):
    goal_id: pycdr2.types.array[pycdr2.types.uint8, 16]

# Equivalent to Fibonnaci.Goal.Response class, but serializable by pycdr2
@dataclass
class Fibonacci_GetResult_Response(IdlStruct, typename="Fibonacci_GetResult_Response"):
    status: pycdr2.types.int8
    sequence: pycdr2.types.sequence[pycdr2.types.int32]

@dataclass
class Fibonacci_Feedback(IdlStruct, typename="Fibonacci_Feedback"):
    goal_id: pycdr2.types.array[pycdr2.types.uint8, 16]
    partial_sequence: pycdr2.types.sequence[pycdr2.types.int32]




def feedback_callback(sample: zenoh.Sample):
    # Deserialize the message
    feedback = Fibonacci_Feedback.deserialize(sample.payload)
    print('Next number in sequence received: {0}'.format(feedback.partial_sequence))


def main():
    parser = argparse.ArgumentParser(
        prog='fibonacci_action_client',
        description='Zenoh/ROS2 fibonacci_action_client example')
    parser.add_argument('--config', '-c', dest='config',
        metavar='FILE',
        type=str,
        help='A configuration file.')
    args = parser.parse_args()

    # Create Zenoh Config from file if provoded, or a default one otherwise
    conf = zenoh.Config.from_file(args.config) if args.config is not None else zenoh.Config()
    # Open Zenoh Session
    session = zenoh.open(conf)

    # Declare a subscriber for feedbacks
    pub = session.declare_subscriber('fibonacci/_action/feedback', feedback_callback)

    goal_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    req = Fibonacci_SendGoal_Request(goal_id, order=10)
    # Send the query with the serialized request
    print('Sending goal')
    replies = session.get('fibonacci/_action/send_goal', zenoh.Queue(), value=req.serialize())
    # Zenoh could get several replies for a request (e.g. from several "Service Servers" using the same name)
    for reply in replies.receiver:
        # Deserialize the response
        rep = Fibonacci_SendGoal_Response.deserialize(reply.ok.payload)
        if not rep.accepted:
            print('Goal rejected :(')
            return

    print('Goal accepted by server, waiting for result')

    req = Fibonacci_GetResult_Request(goal_id)
    # Send the query with the serialized request
    replies = session.get('fibonacci/_action/get_result', zenoh.Queue(), value=req.serialize())
    # Zenoh could get several replies for a request (e.g. from several "Service Servers" using the same name)
    for reply in replies.receiver:
        # Deserialize the response
        rep = Fibonacci_GetResult_Response.deserialize(reply.ok.payload)
        print('Result: {0}'.format(rep.sequence))


    session.close()


if __name__ == '__main__':
    main()
