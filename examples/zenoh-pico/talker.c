#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>

#include <zenoh-pico.h>
#include "rcl_interfaces/msg/Log.h"
// CycloneDDS CDR Deserializer
#include <dds/cdr/dds_cdrstream.h>

// CDR Xtypes header {0x00, 0x01} indicates it's Little Endian (CDR_LE representation)
const uint8_t ros2_header[4] = {0x00, 0x01, 0x00, 0x00};

static dds_cdrstream_allocator_t allocator = {.malloc = malloc, .realloc = realloc, .free = free};

const size_t alloc_size = 4096;  // Abitrary size

int main(int argc, char **argv) {
    const char *keyexpr = "rosout";
    char *const default_value = "Pub from Pico!";
    char *value = default_value;
    const char *mode = "client";
    char *clocator = NULL;
    char *llocator = NULL;
    int n = 2147483647;  // max int value by default

    int opt;
    while ((opt = getopt(argc, argv, "k:v:e:m:l:n:")) != -1) {
        switch (opt) {
            case 'k':
                keyexpr = optarg;
                break;
            case 'v':
                value = optarg;
                break;
            case 'e':
                clocator = optarg;
                break;
            case 'm':
                mode = optarg;
                break;
            case 'l':
                llocator = optarg;
                break;
            case 'n':
                n = atoi(optarg);
                break;
            case '?':
                if (optopt == 'k' || optopt == 'v' || optopt == 'e' || optopt == 'm' || optopt == 'l' ||
                    optopt == 'n') {
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                } else {
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                }
                return 1;
            default:
                return -1;
        }
    }

    z_owned_config_t config = z_config_default();
    zp_config_insert(z_loan(config), Z_CONFIG_MODE_KEY, z_string_make(mode));
    if (clocator != NULL) {
        zp_config_insert(z_loan(config), Z_CONFIG_CONNECT_KEY, z_string_make(clocator));
    }
    if (llocator != NULL) {
        zp_config_insert(z_loan(config), Z_CONFIG_LISTEN_KEY, z_string_make(llocator));
    }

    printf("Opening session...\n");
    z_owned_session_t s = z_open(z_move(config));
    if (!z_check(s)) {
        printf("Unable to open session!\n");
        return -1;
    }

    // Start read and lease tasks for zenoh-pico
    if (zp_start_read_task(z_loan(s), NULL) < 0 || zp_start_lease_task(z_loan(s), NULL) < 0) {
        printf("Unable to start read and lease tasks\n");
        z_close(z_session_move(&s));
        return -1;
    }

    printf("Declaring publisher for '%s'...\n", keyexpr);
    z_owned_publisher_t pub = z_declare_publisher(z_loan(s), z_keyexpr(keyexpr), NULL);
    if (!z_check(pub)) {
        printf("Unable to declare publisher for key expression!\n");
        return -1;
    }

    // Set HelloWorld IDL message
    rcl_interfaces_msg_Log msg;
    msg.stamp.sec = 0;
    msg.stamp.nanosec = 0;
    msg.level = 20;
    msg.name = "zenoh_log_test";
    msg.msg = "Hello from Zenoh to ROS2 encoded with CycloneDDS dds_cdrstream serializer";
    msg.function = "z_publisher_put";
    msg.file = "z_pub_ros2.c";
    msg.line = 140;

    dds_ostream_t os;
    // Allocate buffer for ROS2 header
    uint8_t *buf = malloc(alloc_size);
    memcpy(buf, ros2_header, sizeof(ros2_header));

    struct dds_cdrstream_desc desc_wr;
    dds_cdrstream_desc_init(&desc_wr, &allocator, 
            rcl_interfaces_msg_Log_desc.m_size, 
            rcl_interfaces_msg_Log_desc.m_align, 
            rcl_interfaces_msg_Log_desc.m_flagset,
            rcl_interfaces_msg_Log_desc.m_ops,
            rcl_interfaces_msg_Log_desc.m_keys,
            rcl_interfaces_msg_Log_desc.m_nkeys);

    printf("Press CTRL-C to quit...\n");
    for (int idx = 0; idx < n; ++idx) {
        sleep(1);
        printf("Putting Data ('%s')...\n", keyexpr);

        os.m_buffer = buf;
        os.m_index = sizeof(ros2_header);  // Offset for CDR Xtypes header
        os.m_size = alloc_size;
        os.m_xcdr_version = DDSI_RTPS_CDR_ENC_VERSION_2;

        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        msg.stamp.sec = ts.tv_sec;
        msg.stamp.nanosec = ts.tv_nsec;

        // Do serialization
        bool ret = dds_stream_write_sampleLE((dds_ostreamLE_t *)&os, &allocator, (void *)&msg, &desc_wr);
        if (!ret) {
            printf("dds_stream_write_sampleLE failed\n");
            continue;
        }

        z_publisher_put_options_t options = z_publisher_put_options_default();
        z_publisher_put(z_loan(pub), (const uint8_t *)os.m_buffer, os.m_index, &options);
    }

    z_undeclare_publisher(z_move(pub));

    dds_cdrstream_desc_fini(&desc_wr, &allocator);

    // Stop read and lease tasks for zenoh-pico
    zp_stop_read_task(z_loan(s));
    zp_stop_lease_task(z_loan(s));

    z_close(z_move(s));

    return 0;
}