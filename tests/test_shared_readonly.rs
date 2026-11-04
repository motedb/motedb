//! B1 共享只读验收（0.18 设计文档 §B1）。
//!
//! flock 语义必须跨进程验证 —— 子进程复用本测试二进制（stability
//! round-5 同款模式）: `MOTEDB_SR_CHILD=<mode>` 环境变量分派。
//!
//! 验收:
//! ① 两读者进程并发查询一致;
//! ② 脏 WAL / 未干净关闭 → 明确报错;
//! ③ 写者持 EX 时读者 open 失败且带提示;
//! ④ kill -9 写者后读者报错; 写模式 open+close 治愈后读者可开;
//! ⑤ 只读 open+close 后目录字节级不变（比 RSS 更硬的零落盘证明）。

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};

// ── 子进程模式 ───────────────────────────────────────────────────────────

#[test]
fn child_mode() {
    let mode = match std::env::var("MOTEDB_SR_CHILD") {
        Ok(m) => m,
        Err(_) => return, // 正常套件运行——非子进程调用
    };
    let dir = std::env::var("MOTEDB_SR_DIR").expect("dir");
    let out_path = std::env::var("MOTEDB_SR_OUT").expect("out");
    let ready_path = std::env::var("MOTEDB_SR_READY").expect("ready");
    let emit = |line: &str| {
        std::fs::write(&out_path, line).unwrap();
    };
    let mark_ready = || {
        std::fs::write(&ready_path, "ready").unwrap();
    };
    match mode.as_str() {
        // 写者: 建库写数据并 checkpoint，保持句柄存活（持 EX 锁）。
        "writer-hold" => {
            // create-or-open: 首次建库; ③ 复用时打开（持 EX 等待 stdin EOF）。
            let db = if std::path::Path::new(&dir).join("lsm").exists() {
                motedb::Database::open(&dir).unwrap()
            } else {
                motedb::Database::create(&dir).unwrap()
            };
            // 幂等: 首次建表灌数; ③ 复用（表已存在）只 checkpoint 持锁。
            if db
                .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
                .is_ok()
            {
                for i in 0..50 {
                    db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 3))
                        .unwrap();
                }
            }
            db.checkpoint().unwrap();
            mark_ready();
            wait_stdin_eof();
            let _ = db.close();
        }
        // 读者: 共享只读打开，单行输出结果（父进程断言）。
        "reader" => match motedb::Database::open_shared_readonly(&dir) {
            Ok(db) => {
                use motedb::sql::QueryResult;
                let row = match db
                    .execute("SELECT COUNT(*), SUM(v) FROM t")
                    .unwrap()
                    .materialize()
                    .unwrap()
                {
                    QueryResult::Select { rows, .. } => {
                        format!("{:?}|{:?}", rows[0][0], rows[0][1])
                    }
                    o => panic!("{o:?}"),
                };
                let ids: Vec<String> = match db
                    .execute("SELECT id FROM t WHERE v >= 60 ORDER BY id")
                    .unwrap()
                    .materialize()
                    .unwrap()
                {
                    QueryResult::Select { rows, .. } => rows
                        .iter()
                        .map(|r| match &r[0] {
                            motedb::types::Value::Integer(i) => i.to_string(),
                            o => panic!("{o:?}"),
                        })
                        .collect(),
                    o => panic!("{o:?}"),
                };
                let denied = if db.execute("INSERT INTO t VALUES (9999, 1)").is_err() {
                    "WRITE-DENIED"
                } else {
                    "WRITE-ALLOWED!"
                };
                emit(&format!(
                    "READER-OK {row} n_ids={} {denied} ids={}",
                    ids.len(),
                    ids.join(",")
                ));
                let _ = db.close();
            }
            Err(e) => emit(&format!("READER-ERR {}", e.to_string().replace('\n', " "))),
        },
        // 治愈者: 写模式 open+close 一次（跑恢复/清 WAL/盖章）。
        "healer" => {
            let db = motedb::Database::open(&dir).unwrap();
            let _ = db.close();
            emit("HEALED");
        }
        // 受害写者: checkpoint 后再写一批进 WAL（不 close → 脏目录）。
        "victim" => {
            let db = motedb::Database::create(&dir).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
                .unwrap();
            for i in 0..50 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 3))
                    .unwrap();
            }
            db.checkpoint().unwrap();
            for i in 100..120 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 3))
                    .unwrap();
            }
            mark_ready();
            wait_stdin_eof(); // 父进程 kill -9
            unreachable!()
        }
        _ => panic!("unknown mode {mode:?}"),
    }
}

fn wait_stdin_eof() {
    let mut buf = [0u8; 1];
    let _ = std::io::stdin().read(&mut buf);
}

// ── 父进程帮手 ───────────────────────────────────────────────────────────

/// 启动子模式并等待其 ready 标记文件（确定性同步; libtest banner 会污染
/// 子进程 stdout，文件同步与 stability harness 的 journal 同理）。
/// 返回 (结果文件路径, 子进程)。writer/victim 的 ready = 数据就绪;
/// reader/healer 无 ready 概念——`no_ready=true` 跳过等待。
fn run_child(
    mode: &str,
    dir: &std::path::Path,
    tmp: &std::path::Path,
    no_ready: bool,
) -> (String, Child) {
    let tag = format!("{mode}-{}", std::process::id());
    let out_path = tmp.join(format!("{tag}.out"));
    let ready_path = tmp.join(format!("{tag}.ready"));
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(&ready_path);
    let child = Command::new(std::env::current_exe().unwrap())
        .arg("child_mode")
        .arg("--exact")
        .arg("--nocapture")
        .env("MOTEDB_SR_CHILD", mode)
        .env("MOTEDB_SR_DIR", dir)
        .env("MOTEDB_SR_OUT", &out_path)
        .env("MOTEDB_SR_READY", &ready_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(tmp.join(format!("{tag}.err"))).unwrap(),
        ))
        .spawn()
        .unwrap();
    if !no_ready {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !ready_path.exists() {
            if std::time::Instant::now() >= deadline {
                let err =
                    std::fs::read_to_string(tmp.join(format!("{tag}.err"))).unwrap_or_default();
                let tmp_ls = std::fs::read_dir(tmp)
                    .map(|it| {
                        it.flatten()
                            .map(|e| e.file_name().to_string_lossy().to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                panic!("{mode}: ready 标记超时; tmp={tmp_ls:?}; child stderr: {err:?}");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
    (out_path.display().to_string(), child)
}

/// 关闭子进程 stdin（触发其收尾），等待退出并读结果文件。
fn finish_child(child: Child, out_path: &str) -> String {
    let mut child = child;
    child.stdin.take();
    let _ = child.wait();
    std::fs::read_to_string(out_path)
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// 目录内容指纹（相对路径 → 内容 FNV-1a 的有序串; 排除 .lock——flock
/// 的载体文件本身允许被创建/触碰，不是数据变更）。
fn dir_fingerprint(dir: &std::path::Path) -> String {
    fn walk(dir: &std::path::Path, items: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, items);
            } else if p.file_name().map(|n| n == ".lock").unwrap_or(false) {
                continue;
            } else {
                let mut bytes = Vec::new();
                if std::fs::File::open(&p)
                    .and_then(|mut f| f.read_to_end(&mut bytes))
                    .is_ok()
                {
                    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
                    for b in &bytes {
                        h ^= *b as u64;
                        h = h.wrapping_mul(0x0000_0100_0000_01b3);
                    }
                    let rel = p.strip_prefix(dir).unwrap().display().to_string();
                    items.push(format!("{rel}:{h:016x}"));
                }
            }
        }
    }
    let mut items = Vec::new();
    walk(dir, &mut items);
    items.sort();
    items.join(";")
}

// ── 主测试 ───────────────────────────────────────────────────────────────

#[test]
fn b1_shared_readonly_acceptance() {
    let root = tempfile::TempDir::new().unwrap();
    let tmp = root.path().join("sync");
    std::fs::create_dir_all(&tmp).unwrap();
    let dir = root.path().join("sr.mote");

    // 干净目录: 写者建库 → checkpoint → 保持存活 → 干净关闭。
    {
        let (out, child) = run_child("writer-hold", &dir, &tmp, false);
        finish_child(child, &out);
    }

    // ── ⑤ 先验零落盘（后续步骤还会动目录） ──
    {
        let before = dir_fingerprint(&dir);
        let (out, child) = run_child("reader", &dir, &tmp, true);
        let out_line = finish_child(child, &out);
        assert!(
            out_line.starts_with("READER-OK"),
            "⑤ 读者应成功: {out_line:?}"
        );
        assert_eq!(
            dir_fingerprint(&dir),
            before,
            "⑤ 只读会话不得修改目录（字节级指纹）"
        );
    }

    // ── ① 两读者并发一致 + 写拒绝 ──
    {
        let (o1, c1) = run_child("reader", &dir, &tmp, true);
        let (o2, c2) = run_child("reader", &dir, &tmp, true);
        let s1 = finish_child(c1, &o1);
        let s2 = finish_child(c2, &o2);
        assert!(
            s1.starts_with("READER-OK") && s2.starts_with("READER-OK"),
            "{s1:?} {s2:?}"
        );
        assert_eq!(s1, s2, "① 两读者并发结果一致");
        assert!(s1.contains("WRITE-DENIED"), "写语句应被拒绝: {s1:?}");
    }

    // ── ③ 写者持 EX → 读者失败且带提示 ──
    {
        let (out, writer) = run_child("writer-hold", &dir, &tmp, false);
        let (ro, rchild) = run_child("reader", &dir, &tmp, true);
        let s = finish_child(rchild, &ro);
        assert!(s.starts_with("READER-ERR"), "③ 应失败: {s:?}");
        assert!(
            s.contains("exclusive") || s.contains("writer"),
            "③ 应提示写者持锁: {s:?}"
        );
        finish_child(writer, &out);
    }

    // ── ②/④ kill -9 写者（脏 WAL + 未盖章）→ 报错; 治愈后可读 ──
    {
        let dir2 = root.path().join("sr2.mote");
        let (out, mut child) = run_child("victim", &dir2, &tmp, false);
        child.kill().unwrap();
        let _ = child.wait();
        let _ = out;

        let (ro, rchild) = run_child("reader", &dir2, &tmp, true);
        let s = finish_child(rchild, &ro);
        assert!(s.starts_with("READER-ERR"), "④ 脏目录应报错: {s:?}");
        assert!(
            s.contains("cleanly closed") || s.contains("dirty") || s.contains("WAL"),
            "④ 应指向脏状态: {s:?}"
        );

        let (ho, healer) = run_child("healer", &dir2, &tmp, true);
        let s = finish_child(healer, &ho);
        assert_eq!(s, "HEALED", "治愈者应成功");

        let (ro, rchild) = run_child("reader", &dir2, &tmp, true);
        let s = finish_child(rchild, &ro);
        assert!(s.starts_with("READER-OK"), "④ 治愈后应可读: {s:?}");
    }
}
