#!/usr/bin/env bash
# A tag query requires at most 5 tags, since no fact carries more.
source "$(dirname "$0")/common.sh"
create_store

# N tag parameters: &tag=k1%3Dv1&tag=k2%3Dv2...
tag_params() {
  local count=$1 i
  for ((i = 1; i <= count; i++)); do printf '&tag=k%d%%3Dv%d' "$i" "$i"; done
}

section "Tags in a query (limit: 5)"
query accept "find facts by 5 tags" "direction=forward$(tag_params 5)"
query reject "find facts by 6 tags" "direction=forward$(tag_params 6)"
