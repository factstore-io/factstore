#!/usr/bin/env bash
# Text values: letters, digits and . _ : / -, starting and ending with a letter or digit.
# Subject, type, tag value and metadata value: at most 256 characters. Tag and metadata keys: at most 128.
source "$(dirname "$0")/common.sh"
create_store
P=$(b64_bytes 2)

section "Subject (max 256 characters)"
append_facts accept "subject of 256 characters"         "$(fact_json T "$(pad order/ 256)" "$P")"
append_facts reject "subject of 257 characters"         "$(fact_json T "$(pad order/ 257)" "$P")"
append_facts accept "hierarchical subject tenant/acme/order/1" "$(fact_json T tenant/acme/order/1 "$P")"
append_facts accept "subject with surrounding spaces (trimmed)" "$(fact_json T " order/1 " "$P")"
append_facts reject "subject with an inner space"       "$(fact_json T "order 1" "$P")"
append_facts reject "subject with non-ASCII text"       "$(fact_json T "city/München" "$P")"
append_facts reject "subject starting with a separator" "$(fact_json T /order/1 "$P")"

section "Fact type (max 256 characters)"
append_facts accept "type of 256 characters"            "$(fact_json "$(pad T 256)" order/1 "$P")"
append_facts reject "type of 257 characters"            "$(fact_json "$(pad T 257)" order/1 "$P")"
append_facts accept "type com.acme.OrderPlaced"         "$(fact_json com.acme.OrderPlaced order/1 "$P")"
append_facts reject "type with a space"                 "$(fact_json "Order Placed" order/1 "$P")"

section "Tag key (max 128 characters) and tag value (max 256 characters, may be empty)"
append_facts accept "tag key of 128 characters"   "$(fact_json T order/1 "$P" "{\"$(pad k 128)\":\"v\"}")"
append_facts reject "tag key of 129 characters"   "$(fact_json T order/1 "$P" "{\"$(pad k 129)\":\"v\"}")"
append_facts accept "tag value of 256 characters" "$(fact_json T order/1 "$P" "{\"k\":\"$(pad v 256)\"}")"
append_facts reject "tag value of 257 characters" "$(fact_json T order/1 "$P" "{\"k\":\"$(pad v 257)\"}")"
append_facts accept "empty tag value (presence only)" "$(fact_json T order/1 "$P" '{"archived":""}')"

section "Metadata key (max 128 characters) and value (max 256 characters, may be empty)"
append_facts accept "metadata key of 128 characters"   "$(fact_json T order/1 "$P" "" "{\"$(pad k 128)\":\"v\"}")"
append_facts reject "metadata key of 129 characters"   "$(fact_json T order/1 "$P" "" "{\"$(pad k 129)\":\"v\"}")"
append_facts accept "metadata value of 256 characters" "$(fact_json T order/1 "$P" "" "{\"k\":\"$(pad v 256)\"}")"
append_facts reject "metadata value of 257 characters" "$(fact_json T order/1 "$P" "" "{\"k\":\"$(pad v 257)\"}")"
