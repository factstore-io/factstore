# Scripts 

This folder gathers scripts useful for performance or integration testing. 

## How to Launch the k6 Scripts

Using Docker (example):

```bash
docker run --rm --network host -i grafana/k6 run - <k6/single_append_with_condition.js
```
