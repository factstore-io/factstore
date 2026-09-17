# Limit samples

curl samples that exercise FactStore's append limits against a running server. The limits are
described in `docs/decisions/002_factstore_limits_concise.md`.

```bash
./run-all.sh                                         # every sample
./03-tags-and-metadata.sh                            # a single group
BASE_URL=http://localhost:9090 STORE=other ./run-all.sh
```

Each case states whether FactStore should accept or reject the request, then prints the HTTP status
it returned and, for a rejection, the reason from the `ApiError` body. Rejected requests return
`400 Bad Request`. The samples append to the store `limits-samples`, creating it if needed.
