//! Generate a small synthetic MCAP bag for import tests: two topics with
//! deterministic payloads. `cargo run --example gen_test_bag -- <out.mcap>`
use mcap::records::MessageHeader;
use mcap::WriteOptions;
use std::collections::BTreeMap;

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: gen_test_bag <out.mcap>");
    let mut buf = std::io::BufWriter::new(std::fs::File::create(&out).unwrap());
    let mut w = WriteOptions::new().create(&mut buf).unwrap();
    let meta = BTreeMap::new();
    let js = w.add_channel(0, "/joint_states", "cdr", &meta).unwrap();
    let cam = w
        .add_channel(0, "/camera/front", "protobuf", &meta)
        .unwrap();
    for i in 0..200u64 {
        let payload: Vec<u8> = (0..28u8).map(|j| j.wrapping_add(i as u8)).collect();
        w.write_to_known_channel(
            &MessageHeader {
                channel_id: js,
                sequence: i as u32,
                log_time: 1_700_000_000_000_000_000 + i * 10_000_000,
                publish_time: 1_700_000_000_000_000_000 + i * 10_000_000,
            },
            &payload,
        )
        .unwrap();
    }
    for i in 0..100u64 {
        let payload: Vec<u8> = vec![0xA5; 64 + i as usize];
        w.write_to_known_channel(
            &MessageHeader {
                channel_id: cam,
                sequence: i as u32,
                log_time: 1_700_000_000_000_000_000 + i * 33_000_000,
                publish_time: 1_700_000_000_000_000_000 + i * 33_000_000,
            },
            &payload,
        )
        .unwrap();
    }
    w.finish().unwrap();
    println!("wrote {out}");
}
