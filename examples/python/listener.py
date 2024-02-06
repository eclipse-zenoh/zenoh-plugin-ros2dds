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

def chatter_callback(sample: zenoh.Sample):
    # Deserialize the message
    msg = String.deserialize(sample.payload)
    print('I heard: [%s]' % msg.data)

def main():
    parser = argparse.ArgumentParser(
        prog='listener',
        description='Zenoh/ROS2 listener example')
    parser.add_argument('--config', '-c', dest='config',
        metavar='FILE',
        type=str,
        help='A configuration file.')
    args = parser.parse_args()

    # Create Zenoh Config from file if provoded, or a default one otherwise
    conf = zenoh.Config.from_file(args.config) if args.config is not None else zenoh.Config()
    # Open Zenoh Session
    session = zenoh.open(conf)

    # Declare a subscriber
    pub = session.declare_subscriber('chatter', chatter_callback)

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt):
        pass
    finally:
        pub.undeclare()
        session.close()


if __name__ == '__main__':
    main()
