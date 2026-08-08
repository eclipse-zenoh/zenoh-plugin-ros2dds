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
from pycdr2 import IdlStruct
from dataclasses import dataclass

# Equivalent to std_msgs.msg.String class, but serializable by pycdr2
@dataclass
class String(IdlStruct, typename="String"):
    data: str

def main():
    parser = argparse.ArgumentParser(
        prog='talker',
        description='Zenoh/ROS2 talker example')
    parser.add_argument('--config', '-c', dest='config',
        metavar='FILE',
        type=str,
        help='A configuration file.')
    args = parser.parse_args()

    # Create Zenoh Config from file if provoded, or a default one otherwise
    conf = zenoh.Config.from_file(args.config) if args.config is not None else zenoh.Config()
    # Open Zenoh Session
    session = zenoh.open(conf)

    # Declare a publisher (optional but allows Zenoh to perform some optimizations)
    pub = session.declare_publisher('chatter')

    try:
        i = 0
        while True:
            i += 1
            msg = String(data='Hello World: {0}'.format(i))
            print('Publishing: "{0}"'.format(msg.data))
            # Publish the serialized message
            pub.put(msg.serialize())
            time.sleep(1)

    except (KeyboardInterrupt):
        pass
    finally:
        pub.undeclare()
        session.close()


if __name__ == '__main__':
    main()
