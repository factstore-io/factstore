# Limits

FactStore is built for event sourcing, where facts are small and few tags express what a fact
affects. It therefore limits what a client may append. The limits are part of the specification,
so they are the same for every storage backend, and they cannot be configured.

The reasoning behind the numbers is in [ADR 002](decisions/002_factstore_limits.md).

## What is limited

| What | Limit |
|---|---|
| Payload of a fact | 64 KiB (65,536 bytes), may be empty |
| Tags per fact | 5 |
| Metadata entries per fact | 20 |
| Facts per append | 1 to 512 |
| Total size of an append | 1 MiB (1,048,576 bytes) |
| Subject, fact type, tag value, metadata value | 256 characters |
| Tag key, metadata key | 128 characters |
| Store name | 256 characters |
| Tags a query may require | 1 to 5 |

The size of an append is the sum of what each fact carries: its payload plus its type, subject,
tag keys and values, and metadata keys and values. A client can compute it exactly before calling.

## What values may contain

A subject, fact type, tag key, tag value, metadata key and metadata value may contain ASCII
letters and digits and the separators `.`, `_`, `:`, `/` and `-`, and must start and end with a
letter or a digit:

```
^[A-Za-z0-9]([A-Za-z0-9._:/-]*[A-Za-z0-9])?$
```

So `order/12345`, `USER:ALICE` and `com.acme.OrderPlaced` are all fine, while a space, an empty
value, a leading `/` and non-ASCII text such as `München` are rejected. A tag value and a metadata
value may also be empty, which marks the tag as present without giving it a value.

Surrounding whitespace is removed before a value is checked, so `" order/1 "` is stored as
`order/1`.

A store name is stricter, because it appears in URLs: it starts with a letter and contains only
letters, digits, `_` and `-`.

The payload is never inspected. It is stored as opaque bytes, whatever they contain.

## What happens when a limit is exceeded

The request is rejected before anything is stored, and it does not use up its idempotency key.

- **HTTP:** `400 Bad Request` with an `ApiError` body whose `reason` is `InvalidInput` and whose
  `message` names the limit and the offending value.
- **gRPC:** status `INVALID_ARGUMENT`, with the same message as its description.

## If a limit is in your way

- **A payload larger than 64 KiB** belongs in a blob store, with a reference to it in the fact.
  Facts are meant to be read back in bulk by projections, which large payloads make slow.
- **More than 5 tags,** especially when the number grows with the data — an order that tags every
  product it contains — usually means the fact should be several facts, one per affected entity,
  each with its own small consistency boundary.
- **Text outside the character set** belongs in the payload, which is never validated or indexed.
- **More than 512 facts in one append** means the work is a bulk import rather than one decision;
  append it in several requests.

## Stability

These limits are part of the storage contract. A later release may **relax** them — a longer
subject, a wider character set — but never **restrict** them, because facts are re-validated when
they are read, so tightening a rule would make stored facts unreadable.
