import type { Store, Fact, Tag } from "~/types";

export const INITIAL_STORES: Store[] = [
  {
    id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    name: "orders",
    created: "2026-01-15T09:00:00Z",
    factCount: 48291,
  },
  {
    id: "b2c3d4e5-f6a7-8901-bcde-f12345678901",
    name: "payments",
    created: "2026-01-15T09:05:22Z",
    factCount: 31847,
  },
  {
    id: "c3d4e5f6-a7b8-9012-cdef-123456789012",
    name: "inventory",
    created: "2026-02-01T14:11:05Z",
    factCount: 12560,
  },
  {
    id: "d4e5f6a7-b8c9-0123-defa-234567890123",
    name: "audit-log",
    created: "2026-03-10T11:00:00Z",
    factCount: 3201,
  },
];

const STORE_TYPES = [
  ["order.placed", "order.updated", "order.cancelled", "order.shipped"],
  [
    "payment.initiated",
    "payment.captured",
    "payment.refunded",
    "payment.disputed",
  ],
  [
    "inventory.adjusted",
    "inventory.reserved",
    "inventory.released",
    "inventory.received",
  ],
  ["audit.login", "audit.action", "audit.policy-check", "audit.logout"],
];

const STORE_SUBJECTS = [
  [
    "order/ORD-9921",
    "order/ORD-9920",
    "order/ORD-9918",
    "order/ORD-9917",
    "order/ORD-9914",
  ],
  [
    "payment/PAY-4812",
    "payment/PAY-4811",
    "payment/PAY-4809",
    "payment/PAY-4807",
  ],
  [
    "product/SKU-001",
    "product/SKU-002",
    "product/SKU-103",
    "product/SKU-044",
  ],
  ["user/u-9881", "user/u-0012", "session/s-2041", "user/u-3392"],
];

const STORE_TAGS: Tag[][][] = [
  [
    [
      { k: "customerId", v: "cust-441" },
      { k: "region", v: "eu-west" },
    ],
    [
      { k: "customerId", v: "cust-882" },
      { k: "region", v: "us-east" },
    ],
  ],
  [
    [
      { k: "customerId", v: "cust-441" },
      { k: "currency", v: "EUR" },
    ],
    [
      { k: "customerId", v: "cust-003" },
      { k: "currency", v: "USD" },
    ],
  ],
  [
    [
      { k: "warehouseId", v: "wh-3" },
      { k: "sku", v: "SKU-001" },
    ],
    [
      { k: "warehouseId", v: "wh-1" },
      { k: "sku", v: "SKU-044" },
    ],
  ],
  [
    [
      { k: "userId", v: "u-9881" },
      { k: "ipHash", v: "a4f3b1..." },
    ],
    [
      { k: "userId", v: "u-0012" },
      { k: "ipHash", v: "c9d2e8..." },
    ],
  ],
];

const STORE_PAYLOADS = [
  `{\n  "orderId": "ORD-9921",\n  "total": 142.50,\n  "currency": "EUR",\n  "items": [\n    { "sku": "SKU-001", "qty": 2, "unitPrice": 71.25 }\n  ],\n  "status": "placed"\n}`,
  `{\n  "paymentId": "PAY-4812",\n  "amount": 142.50,\n  "currency": "EUR",\n  "method": "card",\n  "last4": "4242",\n  "status": "captured"\n}`,
  `{\n  "sku": "SKU-001",\n  "warehouseId": "wh-3",\n  "delta": -2,\n  "newLevel": 43,\n  "reason": "sale"\n}`,
  `{\n  "userId": "u-9881",\n  "action": "store.query",\n  "resource": "orders",\n  "resultCount": 12,\n  "allowed": true\n}`,
];

function storeIndex(storeId: string, stores: Store[]): number {
  const idx = stores.findIndex((s) => s.id === storeId);
  return (idx < 0 ? 0 : idx) % 4;
}

export function generateFacts(storeId: string, stores: Store[]): Fact[] {
  const si = storeIndex(storeId, stores);
  const types = STORE_TYPES[si];
  const subjects = STORE_SUBJECTS[si];
  const tagsets = STORE_TAGS[si];
  const payload = STORE_PAYLOADS[si];
  const base = new Date("2026-05-20T09:58:00Z");

  return Array.from({ length: 14 }, (_, i) => ({
    id: `${(1000 + si * 100 + i).toString(16).padStart(8, "0")}-e5f6-7890-abcd-ef${(123456780 + i).toString(16).padStart(12, "0")}`,
    appendedAt: new Date(base.getTime() - i * 67_000).toISOString(),
    factType: types[i % types.length],
    subject: subjects[i % subjects.length],
    tags: tagsets[i % tagsets.length],
    metadata:
      i % 3 === 0
        ? [
            { k: "sourceIp", v: `10.0.1.${5 + i}` },
            { k: "requestId", v: `req-${800 + i}` },
          ]
        : [],
    payload,
  }));
}

export function getStoreTypes(storeId: string, stores: Store[]): string[] {
  return STORE_TYPES[storeIndex(storeId, stores)];
}

export function getStoreSubjects(storeId: string, stores: Store[]): string[] {
  return STORE_SUBJECTS[storeIndex(storeId, stores)];
}
