//! Zero-dependency web dashboard for `sensor_hub serve`.
//!
//! One TCP listener, two routes, no frameworks — the page is a single
//! embedded HTML string with vanilla JS (canvas sparkline + confidence
//! bars) polling `/api/state` four times a second. Works from any browser
//! on the same WiFi as the phone.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use crate::live::SharedState;

pub fn serve(state: Arc<Mutex<SharedState>>) {
    let port: u16 = std::env::var("SENSOR_HUB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let listener = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("dashboard: cannot bind :{port} ({e}) — continuing without it");
            return;
        }
    };
    for conn in listener.incoming() {
        let Ok(mut stream) = conn else { continue };
        let mut buf = [0u8; 2048];
        let n = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();

        let (status, ctype, body) = match path.as_str() {
            "/" => ("200 OK", "text/html; charset=utf-8", PAGE.to_string()),
            "/api/state" => ("200 OK", "application/json", api_json(&state)),
            _ => ("404 Not Found", "text/plain", "not found".into()),
        };
        let resp = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(resp.as_bytes());
        let _ = stream.flush();
    }
}

fn api_json(state: &Arc<Mutex<SharedState>>) -> String {
    let st = state.lock().unwrap();
    let per: Vec<String> = st
        .per_label
        .iter()
        .map(|(l, p)| format!("{{\"label\":\"{l}\",\"pct\":{p:.1}}}"))
        .collect();
    let mags: Vec<String> = st.mags.iter().map(|m| format!("{m:.2}")).collect();
    let mins: Vec<String> = st
        .minute_rows
        .iter()
        .map(|(ts, p)| format!("{{\"ts\":{ts},\"power\":{p}}}"))
        .collect();
    format!(
        "{{\"label\":\"{}\",\"conf\":{:.1},\"per\":[{}],\"mags\":[{}],\"samples\":{},\"shakes\":{},\"minutes\":[{}]}}",
        st.label, st.conf, per.join(","), mags.join(","), st.samples, st.shakes, mins.join(",")
    )
}

const PAGE: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MoteDB · Pocket Sensor Hub</title>
<style>
  :root{--bg:#0a0f16;--fg:#93a3b8;--acc:#ff8a2b;--ok:#3ddc84;--card:#111823}
  body{background:var(--bg);color:var(--fg);font-family:-apple-system,'PingFang SC',sans-serif;
       margin:0;padding:24px;display:flex;flex-direction:column;gap:16px}
  h1{font-size:15px;font-weight:600;color:#5d6f83;margin:0;letter-spacing:.12em}
  h1 b{color:var(--acc)}
  #label{font-size:52px;font-weight:800;color:var(--ok);line-height:1;margin:4px 0 0}
  #conf{color:var(--acc);font-size:15px;margin-top:2px}
  .row{display:flex;gap:16px;flex-wrap:wrap}
  .card{background:var(--card);border:1px solid #1d2735;border-radius:12px;padding:14px 16px;flex:1;min-width:260px}
  .card h2{font-size:12px;letter-spacing:.1em;color:#5d6f83;margin:0 0 10px;font-weight:600}
  .bar{display:flex;align-items:center;gap:8px;margin:6px 0}
  .bar .l{width:110px;font-size:13px}
  .bar .t{flex:1;height:12px;background:#0d141d;border-radius:6px;overflow:hidden}
  .bar .f{height:100%;background:linear-gradient(90deg,#ff8a2b,#ffb066);width:0%;transition:width .2s}
  .bar .p{width:44px;text-align:right;font-size:12px;color:#63788f}
  #stats{display:flex;gap:28px;font-size:13px}
  #stats b{display:block;font-size:22px;color:var(--fg)}
  canvas{width:100%;height:120px;display:block}
  .dim{color:#3d4f63;font-size:12px;margin-top:10px}
</style></head><body>
  <h1>MOTEDB · <b>POCKET SENSOR HUB</b> · the database is the classifier</h1>
  <div><div id="label">…</div><div id="conf"></div></div>
  <div class="row">
    <div class="card"><h2>1-NN VOTE (K=5, SQL KNN)</h2><div id="bars"></div></div>
    <div class="card"><h2>|ACCELEROMETER| · LIVE</h2><canvas id="mag" width="600" height="120"></canvas>
      <div class="dim">samples → MoteDB VECTOR(8) window fingerprint → ORDER BY fp &lt;-&gt; '[…]' LIMIT 5</div></div>
    <div class="card"><h2>SESSION</h2><div id="stats">
      <div><b id="s-samples">0</b>samples</div>
      <div><b id="s-shakes">0</b>shake events</div>
      <div><b id="s-minutes">0</b>minute buckets</div>
    </div></div>
  </div>
<script>
const $=id=>document.getElementById(id);
async function tick(){
  try{
    const s=await (await fetch('/api/state')).json();
    $('label').textContent=s.label;
    $('conf').textContent=s.conf.toFixed(0)+'% agreement';
    $('bars').innerHTML=(s.per||[]).map(p=>
      `<div class="bar"><div class="l">${p.label}</div><div class="t"><div class="f" style="width:${p.pct}%"></div></div><div class="p">${p.pct.toFixed(0)}%</div></div>`).join('');
    $('s-samples').textContent=s.samples;
    $('s-shakes').textContent=s.shakes;
    $('s-minutes').textContent=s.minutes.length;
    draw(s.mags||[]);
  }catch(e){}
  setTimeout(tick,400);
}
function draw(m){
  const c=$('mag'),x=c.getContext('2d');
  x.clearRect(0,0,c.width,c.height);
  if(m.length<2)return;
  const mx=Math.max(15,...m),w=c.width,h=c.height;
  x.strokeStyle='#1d2735';x.beginPath();x.moveTo(0,h-(15/mx)*h);x.lineTo(w,h-(15/mx)*h);x.stroke();
  x.strokeStyle='#ff8a2b';x.lineWidth=1.6;x.beginPath();
  m.forEach((v,i)=>{const px=i/(m.length-1)*w,py=h-(Math.min(v,mx)/mx)*(h-8)-4;i?x.lineTo(px,py):x.moveTo(px,py)});
  x.stroke();
}
tick();
</script></body></html>"#;
