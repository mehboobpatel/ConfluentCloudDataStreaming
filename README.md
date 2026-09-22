# Real-Time VIP Order Alerts — Confluent Cloud Streaming App

A real-time data streaming pipeline built on **Confluent Cloud** that ingests simulated e-commerce order events, uses **Apache Flink SQL** to continuously filter high-value ("VIP") orders and detect order-volume spikes, and pushes alerts downstream via an **HTTP Sink connector** to Slack.

Built for **Developer Day — Submit Your Confluent App (Most Impactful App Challenge)**.

---

## Architecture

```
Datagen Source Connector (ORDERS quickstart, AVRO)
            │
            ▼
        Orders (topic)
            │
            ▼
   Flink SQL (Stream Processing)
      ├── vip_alerts      (CTAS — filters high-value orders)
      └── order_spikes    (CTAS — 1-minute tumbling window aggregation)
            │
            ▼
     HTTP Sink Connector
            │
            ▼
     Slack (#datastreaming-alerts channel)
```

Schema Registry (Stream Governance) automatically registers and manages the Avro schemas for every topic in this pipeline — `Orders`, `vip_alerts`, and `order_spikes` — giving full schema lineage and compatibility enforcement out of the box.

---

## What it does

Simulated order events flow continuously into Confluent Cloud. Instead of waiting for a batch job or manual review, Flink SQL evaluates every single order **the moment it arrives**:

- **`vip_alerts`** — flags any order where `orderunits > 7`, capturing order ID, item, quantity, and customer location in real time.
- **`order_spikes`** — counts orders per item in rolling 1-minute tumbling windows, surfacing sudden demand spikes (useful for both fraud detection and inventory/demand signals).

Flagged VIP orders are pushed to a Slack channel via an HTTP Sink connector, so a support or fraud team is notified within seconds of the order being placed — not hours later during a batch review.

**Who it's for:** e-commerce fraud/support teams, and business teams tracking VIP customer activity or sudden demand spikes on specific products.

**Benefit:** faster fraud response, proactive VIP customer engagement, and real-time visibility into demand — all without any custom backend code, built entirely on managed Confluent Cloud services.

---

## Confluent components used

| Component | Purpose |
|---|---|
| **Datagen Source Connector** | Generates simulated order events (`ORDERS` quickstart template, AVRO format) into the `Orders` topic |
| **Flink SQL (Stream Processing)** | Two continuous CTAS queries: `vip_alerts` (filter) and `order_spikes` (windowed aggregation) |
| **Schema Registry (Stream Governance)** | Auto-registers and manages Avro schemas for `Orders`, `vip_alerts`, and `order_spikes` |
| **HTTP Sink Connector** | Delivers `vip_alerts` records to a Slack Incoming Webhook in real time |
| **Stream Lineage** | Visualizes the full pipeline end-to-end (see screenshot below) |

---

## Flink SQL — Stream Processing

### 1. Explore the raw data
```sql
SELECT orderid, itemid, orderunits, address
FROM Orders
LIMIT 20;
```

### 2. Filter high-value orders (VIP alerts)
```sql
CREATE TABLE vip_alerts AS
SELECT
  orderid,
  itemid,
  orderunits,
  address.city AS city,
  address.state AS state,
  ordertime
FROM Orders
WHERE orderunits > 7;
```
This is a **CTAS (Create Table As Select)** — Flink runs it continuously in the background, creating a new Kafka topic `vip_alerts`, auto-registering its Avro schema in Schema Registry, and streaming every matching order into it in real time.
<img width="1088" height="588" alt="1sem 122" src="https://github.com/user-attachments/assets/56a1b23d-2c18-4982-8505-6a1420cd91e2" />

### 3. Detect order spikes (windowed aggregation)
```sql
CREATE TABLE order_spikes AS
SELECT
  itemid,
  COUNT(*) AS order_count,
  window_start,
  window_end
FROM TABLE(
  TUMBLE(TABLE Orders, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE)
)
GROUP BY itemid, window_start, window_end;
```
Uses Flink's table-valued function (TVF) windowing syntax with Confluent's built-in `$rowtime` watermarked time attribute, counting orders per item in rolling 1-minute windows.

---

## Connectors

### Datagen Source Connector
- Topic: `Orders`
- Format: AVRO
- Quickstart template: `ORDERS`
- Tasks: 1

### HTTP Sink Connector
- Source topic: `vip_alerts`
- Input format: AVRO
- Destination: Slack Incoming Webhook (`#datastreaming-alerts` channel)
- Request method: POST
- Error tolerance: `all` (connector continues running even if downstream response format doesn't match Slack's exact schema)

Sanitized config examples are in [`connectors/`](./connectors).

---

## Schema (Avro — `Orders-value`)

> Paste your actual schema JSON from Schema Registry here (Schema Registry → `Orders-value` subject → copy schema).

```json
{
  "connect.name": "ksql.orders",
  "fields": [
    {
      "name": "ordertime",
      "type": "long"
    },
    {
      "name": "orderid",
      "type": "int"
    },
    {
      "name": "itemid",
      "type": "string"
    },
    {
      "name": "orderunits",
      "type": "double"
    },
    {
      "name": "address",
      "type": {
        "connect.name": "ksql.address",
        "fields": [
          {
            "name": "city",
            "type": "string"
          },
          {
            "name": "state",
            "type": "string"
          },
          {
            "name": "zipcode",
            "type": "long"
          }
        ],
        "name": "address",
        "type": "record"
      }
    }
  ],
  "name": "orders",
  "namespace": "ksql",
  "type": "record"
}
```

---
## Slack Integration/

<img width="1285" height="622" alt="Allow the StreamingData app" src="https://github.com/user-attachments/assets/34f56cbb-bb88-4f67-a423-3f3d06723f1a" />


## Stream Lineage

The full pipeline — Datagen connector → `Orders` topic → Flink processing → `vip_alerts` / `order_spikes` topics → HTTP Sink connector → Slack — is visualized end-to-end in Confluent Cloud's Stream Lineage view.

> Screenshot:
<img width="1423" height="758" alt="image" src="https://github.com/user-attachments/assets/0baf0df5-5617-4a00-9285-8a400925ecba" />


---

## Repo contents

```
├── README.md
├── flink-queries.sql          # All Flink SQL used in this project
└── connectors/
    ├── datagen-source-config.json
    └── http-sink-config.json
```

> **Note:** connector config files have API keys, secrets, and the Slack webhook URL redacted/replaced with placeholders. Never commit real credentials.
