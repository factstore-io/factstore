#!/usr/bin/env bash
# An append holds 1 to 512 facts, totalling at most 1 MiB (1,048,576 bytes):
# the payloads plus the text values of all its facts.
source "$(dirname "$0")/common.sh"
create_store
P=$(b64_bytes 2)

section "Facts per append (limit: 512)"
facts=()
for ((i = 1; i <= 513; i++)); do facts+=("$(fact_json ORDER_PLACED "order/$i" "$P")"); done
append_facts accept "512 facts" "${facts[@]:0:512}"
append_facts reject "513 facts" "${facts[@]}"
printf '{"facts":[]}' | append reject "append without facts"

section "Total size of an append (limit: 1 MiB = 1,048,576 bytes)"
# Each fact is the type "T" (1 byte), the subject "s" (1 byte) and a 65,534-byte payload:
# 65,536 bytes, so 16 of them total exactly 1 MiB.
full=$(b64_bytes 65534)
facts=()
for ((i = 1; i <= 16; i++)); do facts+=("$(fact_json T s "$full")"); done
append_facts accept "16 facts totalling exactly 1,048,576 bytes" "${facts[@]}"
facts[15]=$(fact_json T s "$(b64_bytes 65535)")
append_facts reject "16 facts totalling 1,048,577 bytes" "${facts[@]}"
