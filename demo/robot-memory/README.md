# Robot Memory — a robot that remembers where the red cup is

An MoteDB demo of **object permanence for embodied AI**: a robot patrols a
room, and every sighting of every object becomes one row carrying all four
modalities at once. Ask "红杯子在哪?" and the database answers with the
latest sighting, the full history, and the habitual place — because a
KV/vector store remembers "now", while a database remembers "always".

```
cargo run --release -- story
```

```
═══ 开场: 空记忆 ═══
还没见过「红杯子」。先 patrol 巡逻几轮。

═══ 巡逻 4 轮（杯子还在茶几）═══
...
「红杯子」现在在: 茶几 (5.6, 1.9)，24分钟前目击
记忆: 共 16 次目击; 最初在茶几 (5.6,2.0)，40分钟前
习惯位置: 茶几（100% 的目击都在这里）

═══ 再巡逻 4 轮（中途有人把杯子挪进厨房）═══
   （有人把红杯子挪走了…机器人下一轮才会发现）
   （有人把遥控器挪走了…机器人下一轮才会发现）

═══ 问: 红杯子在哪? ═══
「红杯子」现在在: 厨房 (1.9, 7.4)，4分钟前目击
记忆: 共 30 次目击; 最初在茶几 (5.6,2.0)，40分钟前
习惯位置: 茶几（53% 的目击都在这里）   ← 历史权重透出

     0  1  2  3  4  5  6  7  8  9        ★ = 红杯子最新位置
   ...  厨  厨  ★  厨  厨 ...
```

## One row, four modalities

```sql
CREATE TABLE sightings (
    ts   TIMESTAMP,          -- WHEN: full history, forever
    obj  TEXT,
    note TEXT,               -- WHAT: '红杯子 在茶几' (FTS-indexed)
    x    FLOAT, y FLOAT,     -- denormalized for GROUP BY analytics
    zone TEXT,
    loc  GEOMETRY            -- WHERE: POINT3D(x, y, 0) + spatial index
);
```

The queries behind the answers (all verified paths):

| Question | SQL |
|---|---|
| 它现在在哪? | `WHERE obj = '红杯子' LATEST BY obj` |
| 附近有什么? | `ORDER BY loc <-> ST_POINT(x, y) LIMIT k` |
| 完整时间线 | `ORDER BY ts ASC` |
| 习惯位置? | `GROUP BY zone ORDER BY COUNT(*) DESC` |

The eyes are simulated (waypoint patrol + vision radius + jitter — see
`src/world.rs` for the room, the objects, and the human who keeps moving
the cup). The **brain is entirely MoteDB**: swap the simulator for a real
perception pipeline emitting the same rows and every query stays as-is.

## Found while building this demo

Dogfooding found two real engine bugs, both filed with minimal repros:

- [#17](https://github.com/motedb/motedb/issues/17) — `MATCH AGAINST` +
  `ORDER BY ts DESC` silently returns ascending order
- [#18](https://github.com/motedb/motedb/issues/18) — equality `WHERE` +
  `ORDER BY` without `LIMIT` projects TIMESTAMP columns as NULL

(The demo carries `LIMIT` workarounds, commented in code. An out-of-order
timestamp scare during development turned out to be the demo's own clock
drift — verified engine-innocent by Rust-side brute force before fixing
the simulator.)
