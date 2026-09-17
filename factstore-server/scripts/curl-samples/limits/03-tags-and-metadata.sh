#!/usr/bin/env bash
# A fact carries at most 5 tags and 20 metadata entries.
source "$(dirname "$0")/common.sh"
create_store
P=$(b64_bytes 2)

section "Tags per fact (limit: 5)"
append_facts accept "5 tags" "$(fact_json T order/1 "$P" "$(entries_json 5)")"
append_facts reject "6 tags" "$(fact_json T order/1 "$P" "$(entries_json 6)")"

section "Metadata entries per fact (limit: 20)"
append_facts accept "20 metadata entries" "$(fact_json T order/1 "$P" "" "$(entries_json 20)")"
append_facts reject "21 metadata entries" "$(fact_json T order/1 "$P" "" "$(entries_json 21)")"
