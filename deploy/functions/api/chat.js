// 跨境智训 代理层 · Cloudflare Pages Function 版（与 Pages 静态页同域，推荐部署形态）
// 目录约定：deploy/functions/api/chat.js → 自动挂载为 /api/chat
// 环境变量在 Pages 项目 Settings → 变量和密钥 里配置：XF_API_KEY / XF_API_SECRET / XF_FLOW_ID
const XF_URL = "https://xingchen-api.xf-yun.com/workflow/v1/chat/completions";
const XF_UPLOAD = "https://xingchen-api.xf-yun.com/workflow/v1/upload_file";
const MAX_ROUNDS = 6;
const sessions = new Map();

function makeSanitizer() {
  let carry = "";
  function clean(line) {
    if (/^\s*\|/.test(line)) return line + "\n";
    if (/^\s*```/.test(line)) return "";
    let s = line.replace(/^(\s*)#{1,6}\s+/, "$1");
    s = s.replace(/\*\*([^*]+)\*\*/g, "$1").replace(/\*\*/g, "");
    s = s.replace(/\*([^*\n]+)\*/g, "$1");
    s = s.replace(/`([^`]+)`/g, "$1");
    return s + "\n";
  }
  return {
    feed(t) { carry += t; const p = carry.split("\n"); carry = p.pop(); let o = ""; for (const l of p) o += clean(l); return o; },
    flush() { const o = carry ? clean(carry) : ""; carry = ""; return o; },
  };
}

async function uploadImage(base64, name, auth) {
  const bin = Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
  const form = new FormData();
  form.append("file", new Blob([bin]), name || "upload.jpg");
  const resp = await fetch(XF_UPLOAD, { method: "POST", headers: { Authorization: auth }, body: form });
  const data = await resp.json();
  if (data.code !== 0 || !data.data || !data.data.url) throw new Error("图片上传失败: " + (data.message || resp.status));
  return data.data.url;
}

function sseError(msg) {
  return new Response("data: " + JSON.stringify({ error: msg }) + "\n\n",
    { headers: { "Content-Type": "text/event-stream; charset=utf-8", "Access-Control-Allow-Origin": "*" } });
}

export async function onRequestPost(context) {
  const { request, env } = context;
  const auth = `Bearer ${env.XF_API_KEY}:${env.XF_API_SECRET}`;
  let body;
  try { body = await request.json(); } catch { return sseError("请求体不是合法JSON"); }
  const { message = "", chatId = "", imageBase64, imageName, history: clientHist } = body;
  const sid = (chatId || "guest-" + Date.now()).slice(0, 32);
  const parameters = { AGENT_USER_INPUT: message };
  try {
    if (imageBase64) parameters.file = await uploadImage(imageBase64, imageName, auth);
  } catch (e) { return sseError(String(e.message || e)); }

  const hist = (Array.isArray(clientHist) && clientHist.length ? clientHist : (sessions.get(sid) || [])).slice(-MAX_ROUNDS * 2);
  const upstream = await fetch(XF_URL, {
    method: "POST",
    headers: { Authorization: auth, "Content-Type": "application/json", Accept: "text/event-stream" },
    body: JSON.stringify({ flow_id: env.XF_FLOW_ID, uid: "webuser", stream: true, parameters, chat_id: sid, history: hist }),
  });
  if (!upstream.ok || !upstream.body) return sseError("星辰API HTTP " + upstream.status);

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const enc = new TextEncoder();
  (async () => {
    const reader = upstream.body.getReader();
    const dec = new TextDecoder();
    const san = makeSanitizer();
    let buf = "", full = "", failed = null;
    const push = (obj) => writer.write(enc.encode("data: " + JSON.stringify(obj) + "\n\n"));
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        const frames = buf.split("\n\n");
        buf = frames.pop() || "";
        for (const f of frames) {
          const line = f.trim();
          if (!line.startsWith("data:")) continue;
          let j;
          try { j = JSON.parse(line.slice(5).trim()); } catch { continue; }
          if (j.code !== 0) { failed = j.message || ("星辰错误码 " + j.code); break; }
          const fr = j.choices && j.choices[0] && j.choices[0].finish_reason;
          if (fr === "ping") continue;
          const delta = j.choices && j.choices[0] && j.choices[0].delta;
          if (delta && delta.content) {
            full += delta.content;
            const out = san.feed(delta.content);
            if (out) await push({ content: out });
          }
        }
        if (failed) break;
      }
    } catch (e) { failed = "流读取中断: " + (e.message || e); }
    if (failed) await push({ error: failed });
    else {
      const tail = san.flush();
      if (tail) await push({ content: tail });
      const clipped = full.length > 4000 ? full.slice(0, 4000) : full;
      sessions.set(sid, [...hist, { role: "user", content_type: "text", content: message },
                               { role: "assistant", content_type: "text", content: clipped }].slice(-MAX_ROUNDS * 2));
      await push({ done: true, chatId: sid });
    }
    await writer.close();
  })();
  return new Response(readable, {
    headers: { "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*" },
  });
}

export async function onRequestOptions() {
  return new Response(null, { headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "*" } });
}
