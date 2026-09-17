#!/usr/bin/env bash
# A payload holds at most 64 KiB (65,536 bytes).
source "$(dirname "$0")/common.sh"
create_store

section "Payload size (limit: 64 KiB = 65,536 bytes)"
append_facts accept "payload of 1 byte"         "$(fact_json ORDER_PLACED order/1 "$(b64_bytes 1)")"
append_facts accept "payload of exactly 65,536 bytes" "$(fact_json ORDER_PLACED order/1 "$(b64_bytes 65536)")"
append_facts reject "payload of 65,537 bytes"   "$(fact_json ORDER_PLACED order/1 "$(b64_bytes 65537)")"
append_facts accept "empty payload"             "$(fact_json USER_LOGGED_OUT user/alice "")"
