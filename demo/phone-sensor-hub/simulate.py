#!/usr/bin/env python3
"""Simulate a Termux sensor stream (JSONL) for off-phone testing.

Generates ~20 minutes of realistic phone-sensor data with three activity
phases: idle on a desk (gravity only), walking (cyclic ~2 Hz bounce),
and shake bursts. Writes Termux termux-sensor shaped JSON lines to stdout.

    python3 simulate.py > sim.jsonl
    ./sensor_hub ingest < sim.jsonl
"""
import json
import math
import random
import sys

random.seed(11)
import time as _t
T0_MS = int(_t.time() * 1000) // 60000 * 60000  # now, rounded to a minute
DT_MS = 200            # 5 Hz (termux-sensor -s 200)

def phase_sample(phase, t):
    """Return (ax, ay, az, gx, gy, gz) for the activity phase at time t."""
    if phase == "idle":
        ax = random.gauss(0.02, 0.02)
        ay = random.gauss(0.01, 0.02)
        az = random.gauss(9.81, 0.03)
        g = lambda: random.gauss(0.0, 0.002)
        return ax, ay, az, g(), g(), g()
    if phase == "walk":
        w = 2.0 * math.pi * 2.0  # ~2 Hz step
        ax = 0.6 * math.sin(w * t) + random.gauss(0, 0.15)
        ay = 0.4 * math.cos(w * t * 0.5) + random.gauss(0, 0.12)
        az = 9.81 + 1.3 * abs(math.sin(w * t)) + random.gauss(0, 0.2)
        g = lambda: random.gauss(0.0, 0.35)
        return ax, ay, az, g(), g(), g()
    # shake
    s = lambda: random.gauss(0.0, 12.0)
    return s(), s(), 9.81 + s(), s(), s(), s()

def main():
    # 20 minutes: idle 6m, walk 8m, idle 2m, shake 1m, walk 3m
    plan = [("idle", 6), ("walk", 8), ("idle", 2), ("shake", 1), ("walk", 3)]
    out = []
    t_ms = T0_MS
    for phase, minutes in plan:
        n = minutes * 60 * 1000 // DT_MS
        for _ in range(n):
            t = (t_ms - T0_MS) / 1000.0
            ax, ay, az, gx, gy, gz = phase_sample(phase, t)
            out.append({
                "timestamp": t_ms,
                "sensors": [
                    {"name": "ACCELEROMETER", "values": [round(ax, 4), round(ay, 4), round(az, 4)]},
                    {"name": "GYROSCOPE", "values": [round(gx, 5), round(gy, 5), round(gz, 5)]},
                ],
            })
            t_ms += DT_MS
    for line in out:
        sys.stdout.write(json.dumps(line) + "\n")

if __name__ == "__main__":
    main()
