//! The world: one room, five objects, one patrolling robot, and a human
//! who keeps moving the red cup.

pub const W: f64 = 10.0;
pub const H: f64 = 10.0;

/// Named zones of the room (x0,y0,x1,y1 inclusive). Used for notes
/// ("红杯子 在茶几") and the ASCII map legend.
pub const ZONES: &[(&str, f64, f64, f64, f64)] = &[
    ("茶几", 4.0, 0.0, 7.0, 3.0),
    ("沙发", 7.0, 1.0, 9.0, 4.0),
    ("走道", 3.0, 4.0, 6.0, 7.0),
    ("水槽", 0.0, 4.0, 2.0, 6.0),
    ("厨房", 0.0, 7.0, 4.0, 9.0),
    ("门口", 4.0, 8.0, 6.0, 9.0),
];

pub fn zone_of(x: f64, y: f64) -> &'static str {
    for (name, x0, y0, x1, y1) in ZONES {
        if x >= *x0 && x <= *x1 && y >= *y0 && y <= *y1 {
            return name;
        }
    }
    "房间角落"
}

/// Objects in the world. The story script moves some of them mid-demo —
/// that's the point: the robot's memory keeps the full history.
pub struct Object {
    pub name: &'static str,
    pub x: f64,
    pub y: f64,
}

pub fn objects() -> Vec<Object> {
    vec![
        Object {
            name: "红杯子",
            x: 5.5,
            y: 2.0,
        }, // 茶几 —— 会被挪走
        Object {
            name: "蓝碗",
            x: 1.0,
            y: 5.0,
        }, // 水槽
        Object {
            name: "充电器",
            x: 8.0,
            y: 2.5,
        }, // 沙发
        Object {
            name: "遥控器",
            x: 8.5,
            y: 3.5,
        }, // 沙发 —— 第 7 轮被挪到茶几
        Object {
            name: "钥匙",
            x: 5.0,
            y: 8.5,
        }, // 门口
    ]
}

/// The story: between patrol rounds, a "human" moves things.
/// (round_index, object_name, new_x, new_y)
pub const SCRIPT: &[(usize, &str, f64, f64)] = &[
    (5, "红杯子", 4.2, 5.5), // 拿起杯子走向厨房，半路放下
    (6, "红杯子", 2.0, 7.5), // 到了厨房
    (7, "遥控器", 5.8, 1.6), // 顺手把遥控器丢在茶几
    (9, "红杯子", 1.0, 8.2), // 又挪到厨房深处
];

/// Patrol waypoints (a loop around the room).
pub const WAYPOINTS: &[(f64, f64)] = &[
    (5.0, 5.0),
    (5.5, 1.5),
    (8.5, 3.0),
    (5.0, 8.7),
    (1.5, 5.2),
    (2.5, 8.5),
    (5.0, 5.0),
];

pub const VISION_RADIUS: f64 = 3.6;
