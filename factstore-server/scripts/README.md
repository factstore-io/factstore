# Scripts 

This folder gathers scripts useful for performance or integration testing. 

## How to Launch the k6 Scripts

Using Docker (example):

```bash
docker run --rm --network host -i grafana/k6 run - <k6/single_append_with_condition.js
```

The scripts create the store they write to, `k6` by default. Point them at another server or
store with environment variables:

```bash
docker run --rm --network host -i grafana/k6 run \
  -e BASE_URL=http://localhost:8080 -e STORE=k6 - <k6/single_append_with_condition.js
```

A run fails, with a non-zero exit code, when any request fails, not only when requests are slow.
