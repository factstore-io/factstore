#!/usr/bin/env bash
# Shared helpers for the limit samples. Sourced by the numbered scripts.

BASE_URL="${BASE_URL:-http://localhost:8080}"
STORE="${STORE:-limits-samples}"

# N zero bytes, base64-encoded as the JSON API expects payloads.
b64_bytes() { head -c "$1" /dev/zero | base64 | tr -d '\n'; }

# PREFIX padded with 'x' to exactly LENGTH characters.
pad() {
  local prefix=$1 length=$2
  printf '%s' "$prefix"
  head -c $((length - ${#prefix})) /dev/zero | tr '\0' 'x'
}

# A JSON object with COUNT entries: {"k1":"v1","k2":"v2",...}
entries_json() {
  local count=$1 separator="" i
  printf '{'
  for ((i = 1; i <= count; i++)); do
    printf '%s"k%d":"v%d"' "$separator" "$i" "$i"
    separator=","
  done
  printf '}'
}

# One fact: TYPE SUBJECT PAYLOAD_BASE64 [TAGS_JSON] [METADATA_JSON]
fact_json() {
  local tags="${4-}" metadata="${5-}"
  [ -n "$tags" ] || tags='{}'
  [ -n "$metadata" ] || metadata='{}'
  printf '{"type":"%s","subject":"%s","payload":{"data":"%s"},"tags":%s,"metadata":%s}' \
    "$1" "$2" "$3" "$tags" "$metadata"
}

create_store() {
  curl -s -o /dev/null -X POST "$BASE_URL/api/v1/stores" \
    -H 'Content-Type: application/json' -d "{\"name\":\"$STORE\"}"
}

section() { printf '\n%s\n' "$1"; }

# report EXPECT LABEL STATUS BODY_FILE
report() {
  local expect=$1 label=$2 status=$3 body=$4 outcome verdict reason
  if [[ $status == 2* ]]; then outcome=accepted; else outcome=rejected; fi
  if [ "$outcome" = "${expect}ed" ]; then verdict="as expected"; else verdict="UNEXPECTED"; fi
  printf '  %-58s expect %-6s -> HTTP %s %-8s %s\n' "$label" "$expect" "$status" "$outcome" "$verdict"

  # A rejection's ApiError body carries the reason.
  if [ "$outcome" = rejected ]; then
    reason=$(grep -o '"message":"[^"]*"' "$body" | head -1)
    [ -n "$reason" ] && printf '      %s\n' "${reason:0:150}"
  fi
}

# append EXPECT LABEL, with the request body on stdin.
append() {
  local body status
  body=$(mktemp)
  status=$(curl -s -o "$body" -w '%{http_code}' -X POST "$BASE_URL/api/v1/stores/$STORE/facts" \
    -H 'Content-Type: application/json' --data-binary @-)
  report "$1" "$2" "$status" "$body"
  rm -f "$body"
}

# append_facts EXPECT LABEL FACT_JSON...: sends the given facts as one append request.
append_facts() {
  local expect=$1 label=$2 separator="" fact
  shift 2
  {
    printf '{"facts":['
    for fact in "$@"; do
      printf '%s%s' "$separator" "$fact"
      separator=","
    done
    printf ']}'
  } | append "$expect" "$label"
}

# query EXPECT LABEL QUERY_STRING
query() {
  local body status
  body=$(mktemp)
  status=$(curl -s -o "$body" -w '%{http_code}' "$BASE_URL/api/v1/stores/$STORE/facts?$3")
  report "$1" "$2" "$status" "$body"
  rm -f "$body"
}
