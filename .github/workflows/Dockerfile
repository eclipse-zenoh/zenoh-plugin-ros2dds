#
# Copyright (c) 2022 ZettaScale Technology
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

###
### Dockerfile creating the eclipse/zenoh-bridge-ros2dds image from cross-compiled binaries.
### It assumes that zenoh-bridge-ros2dds is installed in docker/$TARGETPLATFORM/
### where $TARGETPLATFORM is set by buildx to a Docker supported platform such as linux/amd64 or linux/arm64
### (see https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images)
###


FROM alpine:latest

ARG TARGETPLATFORM

RUN apk add --no-cache libgcc libstdc++

COPY docker/$TARGETPLATFORM/zenoh-bridge-ros2dds /

RUN echo '#!/bin/ash' > /entrypoint.sh
RUN echo 'echo " * Starting: /zenoh-bridge-ros2dds $*"' >> /entrypoint.sh
RUN echo 'exec /zenoh-bridge-ros2dds $*' >> /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 7446/udp
EXPOSE 7447/tcp
EXPOSE 8000/tcp

ENV RUST_LOG info

ENTRYPOINT ["/entrypoint.sh"]
