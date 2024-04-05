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

static struct dds_cdrstream_desc desc_rd;

void data_handler(const z_sample_t *sample, void *arg) {
    (void)(arg);

    z_owned_str_t keystr = z_keyexpr_to_string(sample->keyexpr);
    printf(">> [Subscriber] Received ('%s' size '%d')\n", z_loan(keystr), (int)sample->payload.len);
    z_drop(z_move(keystr));

    dds_cdrstream_desc_init(&desc_rd, &allocator, 
        rcl_interfaces_msg_Log_desc.m_size, 
        rcl_interfaces_msg_Log_desc.m_align, 
        rcl_interfaces_msg_Log_desc.m_flagset,
        rcl_interfaces_msg_Log_desc.m_ops,
        rcl_interfaces_msg_Log_desc.m_keys,
        rcl_interfaces_msg_Log_desc.m_nkeys);

    // Deserialize Msg
    dds_istream_t is;
    is.m_buffer = (char *)sample->payload.start + 4;
    is.m_index = 0;
    is.m_size = sample->payload.len;
    is.m_xcdr_version = DDSI_RTPS_CDR_ENC_VERSION_2;

    rcl_interfaces_msg_Log *msg = calloc(1, desc_rd.size);

    dds_stream_read_sample(&is, msg, &allocator, &desc_rd);
    /* print result */
    char buf[5000];
    is.m_index = 0;
    dds_stream_print_sample (&is, &desc_rd, buf, 5000);
    printf ("read sample: %s\n\n", buf);

    dds_cdrstream_desc_fini(&desc_rd, &allocator);

    printf("Log : %s\n", msg->name);
}


int main(int argc, char **argv) {
    const char *keyexpr = "rosout";
    const char *mode = "client";
    const char *clocator = NULL;
    const char *llocator = NULL;

    int opt;
    while ((opt = getopt(argc, argv, "k:e:m:l:")) != -1) {
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
        
    z_owned_closure_sample_t callback = z_closure_sample(data_handler, NULL, NULL);
    printf("Declaring Subscriber on '%s'...\n", keyexpr);
    z_owned_subscriber_t sub =
        z_declare_subscriber(z_session_loan(&s), z_keyexpr(keyexpr), z_closure_sample_move(&callback), NULL);
    if (!z_subscriber_check(&sub)) {
        printf("Unable to declare subscriber.\n");
        return -1;
    }

    printf("Enter 'q' to quit...\n");
    char c = '\0';
    while (c != 'q') {
        fflush(stdin);
        scanf("%c", &c);
    }

    // Stop read and lease tasks for zenoh-pico
    zp_stop_read_task(z_session_loan(&s));
    zp_stop_lease_task(z_session_loan(&s));

    z_close(z_session_move(&s));

    return 0;
}
