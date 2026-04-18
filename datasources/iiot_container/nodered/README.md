# NodeRED Flows — IIoT Sensor Simulators

Three independent flows, one per instrumented machine. Each flow is three nodes:

```
[ inject (every 1s) ] → [ function (per-machine) ] → [ influxdb out ]
```

The simulator runs inside NodeRED on AWS, writes 1 Hz into the shared InfluxDB
bucket `sensors`, measurement `machine_metrics`. Values match the specs in
[CLAUDE.md §5](../../../CLAUDE.md#5-data-sources--iiot).

---

## Shared config

### Inject node

| Setting | Value |
|---|---|
| Payload | *(anything — ignored)* |
| Repeat | `interval` |
| every | `1` second |
| Inject once on deploy | off |

### InfluxDB Out node (InfluxDB 2.x)

| Setting | Value |
|---|---|
| Server | your InfluxDB 2.0 endpoint (e.g. `http://13.213.1.152:8086`) |
| Organization | `iiot_data_architecture` |
| Bucket | `sensors` |
| Measurement | `machine_metrics` |
| Advanced options → Precision | `s` |

The function nodes emit `msg.payload` as a 2-element array:

```js
msg.payload = [fieldsObject, tagsObject];
```

This is the canonical shape for `node-red-contrib-influxdb` 2.x — each key in
`fieldsObject` becomes a field, each key in `tagsObject` becomes an indexed
tag. The node's configured `Measurement` (`machine_metrics`) is used
automatically, and the timestamp defaults to "now" at write time.

### Shared state model (encoded inside each function)

All three machines share the same fault-injection logic so OEE calculations
see realistic availability drops:

| Parameter | Value | Source |
|---|---|---|
| `P_FAULT` (chance of entering FAULT per 1 s tick) | `1 / (4·3600) ≈ 6.94e-5` | Poisson λ = 1 event/4 h |
| `P_RECOVER` (chance of returning to RUNNING per 1 s tick) | `1 / (20·60) ≈ 8.33e-4` | Exponential μ = 20 min |
| Expected downtime | ~20 min / event | |
| Expected uptime | ~4 h between faults | |

Each function stores its state in NodeRED node `context` so it survives
between ticks but resets on NodeRED restart. That's fine — the ETL uses
8-hour aggregation windows anyway.

---

## M01 — Smelting Furnace

Stage 1, `ideal_cycle = 120 s`. Emits `temperature_c` (target ≈ 480 °C) and
`machine_state_num` (1 = RUNNING, 0 = FAULT). Temperature drops sharply
during a fault to look realistic (furnace cooling).

```javascript
// M01 — Smelting Furnace  (FURNACE, stage=smelting)
// Fields: temperature_c, machine_state_num

function gauss(mean, stddev) {
    // Box–Muller transform
    const u1 = Math.random();
    const u2 = Math.random();
    return mean + stddev * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

const P_FAULT   = 1 / (4 * 3600);   // ~1 fault event per 4 h
const P_RECOVER = 1 / (20 * 60);    // ~20 min downtime per event

let state = context.get("state") || { running: true };

if (state.running) {
    if (Math.random() < P_FAULT) {
        state.running = false;
        node.warn("M01 → FAULT");
    }
} else {
    if (Math.random() < P_RECOVER) {
        state.running = true;
        node.warn("M01 → RUNNING");
    }
}
context.set("state", state);

// Furnace temperature drops while tripped.
const temp = state.running ? gauss(480, 5) : gauss(200, 15);
const running = state.running ? 1 : 0;

msg.payload = [
    {
        temperature_c:     Math.round(temp * 10) / 10,
        machine_state_num: running
    },
    {
        machine_id: "M01",
        stage:      "smelting"
    }
];
return msg;
```

---

## M02 — Plate Assembly Unit

Stage 5, `ideal_cycle = 45 s`. Emits `cycle_count` (monotonic, +1 every
completed 45-second cycle while RUNNING), `vibration_g` (target ≈ 0.8 g
during normal operation, spikes when faulted), and `machine_state_num`.

```javascript
// M02 — Plate Assembly Unit  (ASSEMBLER, stage=assembly)
// Fields: cycle_count, vibration_g, machine_state_num

function gauss(mean, stddev) {
    const u1 = Math.random();
    const u2 = Math.random();
    return mean + stddev * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

const P_FAULT        = 1 / (4 * 3600);
const P_RECOVER      = 1 / (20 * 60);
const IDEAL_CYCLE_S  = 45;

let ctx = context.get("ctx") || {
    running: true,
    cycle_count: 0,
    seconds_in_current_cycle: 0
};

if (ctx.running && Math.random() < P_FAULT) {
    ctx.running = false;
    node.warn("M02 → FAULT");
} else if (!ctx.running && Math.random() < P_RECOVER) {
    ctx.running = true;
    node.warn("M02 → RUNNING");
}

// Only advance cycles while RUNNING — downtime freezes the counter.
if (ctx.running) {
    ctx.seconds_in_current_cycle += 1;
    if (ctx.seconds_in_current_cycle >= IDEAL_CYCLE_S) {
        ctx.cycle_count += 1;
        ctx.seconds_in_current_cycle = 0;
    }
}
context.set("ctx", ctx);

const vib     = ctx.running ? gauss(0.80, 0.08) : gauss(2.50, 0.50);
const running = ctx.running ? 1 : 0;

msg.payload = [
    {
        cycle_count:       ctx.cycle_count,
        vibration_g:       Math.round(vib * 100) / 100,
        machine_state_num: running
    },
    {
        machine_id: "M02",
        stage:      "assembly"
    }
];
return msg;
```

---

## M03 — Formation Charger

Stage 8, `ideal_cycle = 300 s`. Emits `current_a` (target ≈ 145 A while
charging, ≈ 0 during fault), `voltage_v` (ramps linearly from 10.5 V to
12.8 V over each 5-minute cycle, then resets), and `machine_state_num`.

```javascript
// M03 — Formation Charger  (CHARGER, stage=charging)
// Fields: current_a, voltage_v, machine_state_num

function gauss(mean, stddev) {
    const u1 = Math.random();
    const u2 = Math.random();
    return mean + stddev * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

const P_FAULT   = 1 / (4 * 3600);
const P_RECOVER = 1 / (20 * 60);
const CYCLE_S   = 300;     // 5 min voltage ramp
const V_START   = 10.5;
const V_END     = 12.8;

let ctx = context.get("ctx") || {
    running: true,
    cycle_progress_s: 0
};

if (ctx.running && Math.random() < P_FAULT) {
    ctx.running = false;
    node.warn("M03 → FAULT");
} else if (!ctx.running && Math.random() < P_RECOVER) {
    ctx.running = true;
    node.warn("M03 → RUNNING");
}

if (ctx.running) {
    ctx.cycle_progress_s = (ctx.cycle_progress_s + 1) % CYCLE_S;
}
context.set("ctx", ctx);

const progress = ctx.cycle_progress_s / CYCLE_S;   // 0 → 1 over 5 min
const voltage  = V_START + (V_END - V_START) * progress;
const current  = ctx.running ? gauss(145, 3) : gauss(0, 0.1);
const running  = ctx.running ? 1 : 0;

msg.payload = [
    {
        current_a:         Math.round(current * 10) / 10,
        voltage_v:         Math.round(voltage * 100) / 100,
        machine_state_num: running
    },
    {
        machine_id: "M03",
        stage:      "charging"
    }
];
return msg;
```

---

## Verifying the flow

After deploying, check the InfluxDB Data Explorer (or `influx query` CLI):

```flux
from(bucket: "sensors")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "machine_metrics")
  |> group(columns: ["machine_id", "_field"])
  |> last()
```

You should see six distinct rows (one per machine × field combo), refreshing
every second. Expected ranges:

| machine | fields | normal | during fault |
|---|---|---|---|
| M01 | `temperature_c` | 470–490 | 170–230 |
| M01 | `machine_state_num` | 1 | 0 |
| M02 | `cycle_count` | monotonically rising | flat |
| M02 | `vibration_g` | 0.7–0.9 | 1.5–3.5 |
| M03 | `current_a` | 142–148 | ≈ 0 |
| M03 | `voltage_v` | sawtooth 10.5 → 12.8 over 5 min | frozen |

If M01 stays at ≈ 480 °C for 4+ hours without a dip, the fault injection
probability may be too low on your deployment (e.g. clock skew or inject
node not firing at 1 Hz). Confirm the inject is set to `Repeat: interval /
every 1 s`.

## How the Airflow DAG reads this

[`etl_influxdb_to_oracle`](../../../db_module/pipeline/airflow/dags/etl_influxdb_to_oracle.py)
runs a Flux query with `aggregateWindow(every: 8h, fn: mean)` and pivots
fields into columns. The schema matches `STG_SENSOR_AGG` on the Oracle side:

| Column | Source field |
|---|---|
| `machine_id` | tag |
| `run_date` | `{{ ds }}` execution date |
| `avg_temp_c` | mean of `temperature_c` |
| `total_cycles` | max - min of `cycle_count` over window |
| `avg_vibration_g` | mean of `vibration_g` |
| `avg_current_a` | mean of `current_a` |
| `avg_voltage_v` | mean of `voltage_v` |

(The DAG currently no-ops when `INFLUX_URL` is unset — update `.env` on the
Airflow host and restart the compose stack to enable it.)
