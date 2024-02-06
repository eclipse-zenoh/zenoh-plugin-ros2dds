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

# Equivalent to AddTwoInts.Request class, but serializable by pycdr2
@dataclass
class AddTwoInts_Request(IdlStruct, typename="AddTwoInts_Request"):
    a: pycdr2.types.int64
    b: pycdr2.types.int64

# Equivalent to AddTwoInts.Response class, but serializable by pycdr2
@dataclass
class AddTwoInts_Response(IdlStruct, typename="AddTwoInts_Request"):
    sum: pycdr2.types.int64

def main():
    parser = argparse.ArgumentParser(
        prog='add_two_ints_client',
        description='Zenoh/ROS2 add_two_ints_client example')
    parser.add_argument('--config', '-c', dest='config',
        metavar='FILE',
        type=str,
        help='A configuration file.')
    args = parser.parse_args()

    # Create Zenoh Config from file if provoded, or a default one otherwise
    conf = zenoh.Config.from_file(args.config) if args.config is not None else zenoh.Config()
    # Open Zenoh Session
    session = zenoh.open(conf)

    req = AddTwoInts_Request(a=2, b=3)
    # Send the query with the serialized request
    replies = session.get('add_two_ints', zenoh.Queue(), value=req.serialize())
    # Zenoh could get several replies for a request (e.g. from several "Service Servers" using the same name)
    for reply in replies.receiver:
        # Deserialize the response
        rep = AddTwoInts_Response.deserialize(reply.ok.payload)
        print('Result of add_two_ints: %d' % rep.sum)

    session.close()


if __name__ == '__main__':
    main()
