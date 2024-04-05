#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>

#include <zenoh-pico.h>
#include "example_interfaces/srv/AddTwoInts.h"
// CycloneDDS CDR Deserializer
#include <dds/cdr/dds_cdrstream.h>

// CDR Xtypes header {0x00, 0x01} indicates it's Little Endian (CDR_LE representation)
const uint8_t ros2_header[4] = {0x00, 0x01, 0x00, 0x00};

static dds_cdrstream_allocator_t allocator = {.malloc = malloc, .realloc = realloc, .free = free};

z_condvar_t cond;
z_mutex_t mutex;

void reply_dropper(void *ctx) {
    (void)(ctx);
    printf(">> Received query final notification\n");
    z_condvar_signal(&cond);
    z_condvar_free(&cond);
}

void reply_handler(z_owned_reply_t *reply, void *ctx) {
    (void)(ctx);
    if (z_reply_is_ok(reply)) {
        z_sample_t sample = z_reply_ok(reply);
        z_owned_str_t keystr = z_keyexpr_to_string(sample.keyexpr);

        dds_istream_t is;
        is.m_buffer = sample.payload.start + 4;
        is.m_index = 0;
        is.m_size = sample.payload.len;
        is.m_xcdr_version = DDSI_RTPS_CDR_ENC_VERSION_2;

        struct dds_cdrstream_desc desc_rd;
        dds_cdrstream_desc_init(&desc_rd, &allocator, 
                example_interfaces_srv_AddTwoInts_Response_desc.m_size, 
                example_interfaces_srv_AddTwoInts_Response_desc.m_align, 
                example_interfaces_srv_AddTwoInts_Response_desc.m_flagset,
                example_interfaces_srv_AddTwoInts_Response_desc.m_ops,
                example_interfaces_srv_AddTwoInts_Response_desc.m_keys,
                example_interfaces_srv_AddTwoInts_Response_desc.m_nkeys);

        example_interfaces_srv_AddTwoInts_Response *data = calloc(1, desc_rd.size);
        dds_stream_read_sample(&is, data, &allocator, &desc_rd);
                
        printf(">> Received ('%s': %ld)\n", z_str_loan(&keystr), data->sum);

        z_str_drop(z_str_move(&keystr));

        dds_cdrstream_desc_fini(&desc_rd, &allocator);
    } else {
        printf(">> Received an error\n");
    }
}

int main(int argc, char **argv) {
    const char *keyexpr = "add_two_ints";
    const char *mode = "client";
    const char *clocator = NULL;
    const char *llocator = NULL;
    const char *req_a = "1000";
    const char *req_b = "2000";

    int opt;
    while ((opt = getopt(argc, argv, "k:e:m:v:a:b:l:")) != -1) {
        switch (opt) {
            case 'k':
                keyexpr = optarg;
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
            case 'a':
                req_a = optarg;
                break;
            case 'b':
                req_b = optarg;
                break;
            case '?':
                if (optopt == 'k' || optopt == 'e' || optopt == 'm' || optopt == 'v' || optopt == 'l') {
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                } else {
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                }
                return 1;
            default:
                return -1;
        }
    }

    allocator.malloc = malloc;
    allocator.free = free;
    allocator.realloc = realloc;

    z_mutex_init(&mutex);
    z_condvar_init(&cond);

    z_owned_config_t config = z_config_default();
    zp_config_insert(z_config_loan(&config), Z_CONFIG_MODE_KEY, z_string_make(mode));
    if (clocator != NULL) {
        zp_config_insert(z_config_loan(&config), Z_CONFIG_CONNECT_KEY, z_string_make(clocator));
    }
    if (llocator != NULL) {
        zp_config_insert(z_config_loan(&config), Z_CONFIG_LISTEN_KEY, z_string_make(llocator));
    }

    printf("Opening session...\n");
    z_owned_session_t s = z_open(z_config_move(&config));
    if (!z_session_check(&s)) {
        printf("Unable to open session!\n");
        return -1;
    }

    // Start read and lease tasks for zenoh-pico
    if (zp_start_read_task(z_session_loan(&s), NULL) < 0 || zp_start_lease_task(z_session_loan(&s), NULL) < 0) {
        printf("Unable to start read and lease tasks\n");
        z_close(z_session_move(&s));
        return -1;
    }

    z_keyexpr_t ke = z_keyexpr(keyexpr);
    if (!z_keyexpr_is_initialized(&ke)) {
        printf("%s is not a valid key expression", keyexpr);
        return -1;
    }
    
    z_mutex_lock(&mutex);

    // Setup ostream for serializer
    dds_ostream_t os;
    os.m_buffer = NULL;
    os.m_index = 0;
    os.m_size = 0;
    os.m_xcdr_version = DDSI_RTPS_CDR_ENC_VERSION_2;

    example_interfaces_srv_AddTwoInts_Request req;
    req.a = (int64_t)(atoi(req_a));
    req.b = (int64_t)(atoi(req_b));
    
    printf("Sending Query '%s'... with a = %ld, b = %ld\n", keyexpr, req.a, req.b);
    // Allocate buffer for serialized message
    uint8_t *buf = malloc(sizeof(ros2_header));

    // Add ROS2 header
    memcpy(buf, ros2_header, sizeof(ros2_header));
    os.m_buffer = buf;
    os.m_index = sizeof(ros2_header);  // Offset for CDR Xtypes header
    os.m_size = sizeof(ros2_header);
    os.m_xcdr_version = DDSI_RTPS_CDR_ENC_VERSION_2;

    struct dds_cdrstream_desc desc_wr;
    dds_cdrstream_desc_init(&desc_wr, &allocator, 
            example_interfaces_srv_AddTwoInts_Request_desc.m_size, 
            example_interfaces_srv_AddTwoInts_Request_desc.m_align, 
            example_interfaces_srv_AddTwoInts_Request_desc.m_flagset,
            example_interfaces_srv_AddTwoInts_Request_desc.m_ops,
            example_interfaces_srv_AddTwoInts_Request_desc.m_keys,
            example_interfaces_srv_AddTwoInts_Request_desc.m_nkeys);
    
    // Do serialization
    bool ret = dds_stream_write_sample(&os, &allocator, (void *)&req, &desc_wr);
    if (!ret) {
      printf("dds_stream_write_sampleLE failed\n");
      exit(-1);
    }

    z_get_options_t opts = z_get_options_default();
    opts.value.payload.start = os.m_buffer;
    opts.value.payload.len = os.m_index;
    opts.value.payload._is_alloc = false;

    z_owned_closure_reply_t callback = z_closure_reply(reply_handler, reply_dropper, NULL);
    if (z_get(z_session_loan(&s), ke, "", z_closure_reply_move(&callback), &opts) < 0) {
        printf("Unable to send query.\n");
        return -1;
    }

    z_condvar_wait(&cond, &mutex);
    z_mutex_unlock(&mutex);

    dds_cdrstream_desc_fini(&desc_wr, &allocator);

    // Stop read and lease tasks for zenoh-pico
    zp_stop_read_task(z_session_loan(&s));
    zp_stop_lease_task(z_session_loan(&s));

    z_close(z_session_move(&s));

    return 0;
}
