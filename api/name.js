// AI起名服务端函数：接收用户需求 + 所选模型，
// 调用 OpenAI 兼容大模型接口流式生成名字方案，并把历史保存到 Supabase
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// modelConfigMap：集中存放各个大模型的 baseURL、apiKey、model，共用一套调用代码
const modelConfigMap = {
  doubao: {
    label: '豆包（火山方舟）',
    baseURL: 'https://ark.cn-beijing.volces.com/api/v1',
    apiKey: process.env.DOUBAO_API_KEY,
    // 默认使用公共模型名；如果你用的是自己的推理接入点，把 ep-xxx 填到环境变量 DOUBAO_MODEL
    model: process.env.DOUBAO_MODEL || 'doubao-seed-1-6-250615'
  },
  deepseek: {
    label: 'DeepSeek',
    baseURL: 'https://api.deepseek.com/v1',
    apiKey: process.env.DEEPSEEK_API_KEY,
    model: 'deepseek-chat'
  },
  qwen: {
    label: '通义千问（DashScope）',
    baseURL: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    apiKey: process.env.DASHSCOPE_API_KEY,
    model: 'qwen-plus'
  },
  kimi: {
    label: 'Kimi（月之暗面）',
    baseURL: 'https://api.moonshot.cn/v1',
    apiKey: process.env.MOONSHOT_API_KEY,
    model: 'moonshot-v1-8k'
  }
};

const SYSTEM_PROMPT = [
  '你是一位专业的起名大师，精通中国传统文化、诗词典故与姓名学。',
  '请根据用户的起名需求给出 3 组名字方案；如果用户提供了姓氏，名字必须与该姓氏搭配。',
  '请用 Markdown 格式输出，每一组按以下结构：',
  '### 方案X：名字',
  '- **寓意解析**：逐字解释含义与整体寓意',
  '- **出处灵感**：给出诗词或文化出处（不要编造不存在的古籍原文）',
  '- **音韵字形**：简评读音节奏与字形结构',
  '最后用一句话总结三组方案的特点。语言简洁优美。'
].join('\n');

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.status(405).json({ error: '仅支持 POST 请求' });
    return;
  }

  const { requirement, model } = req.body || {};
  if (!requirement || !String(requirement).trim()) {
    res.status(400).json({ error: '起名需求不能为空' });
    return;
  }

  const cfg = modelConfigMap[model];
  if (!cfg) {
    res.status(400).json({ error: '未知模型：' + model });
    return;
  }
  if (!cfg.apiKey) {
    res.status(400).json({ error: `模型【${cfg.label}】的 API Key 未配置，请先在环境变量中配置对应密钥` });
    return;
  }

  // 调用 OpenAI 兼容接口（四家平台的 /chat/completions 入参与 SSE 格式一致），开启 stream
  let upstream;
  try {
    upstream = await fetch(cfg.baseURL + '/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: 'Bearer ' + cfg.apiKey
      },
      body: JSON.stringify({
        model: cfg.model,
        stream: true,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: String(requirement) }
        ]
      })
    });
  } catch (e) {
    res.status(502).json({ error: '连接模型接口失败：' + e.message });
    return;
  }

  if (!upstream.ok) {
    const errText = await upstream.text().catch(() => '');
    res.status(502).json({ error: `模型接口调用失败(${upstream.status})：${errText.slice(0, 300)}` });
    return;
  }

  // SSE 流式响应给前端
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  if (typeof res.flushHeaders === 'function') res.flushHeaders();
  const send = (data) => res.write('data: ' + JSON.stringify(data) + '\n\n');
  send({ type: 'start', model: model, label: cfg.label });

  // 读取上游 SSE 流，逐块转发 delta.content，同时累积完整文本
  const reader = upstream.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  let fullText = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const payload = trimmed.slice(5).trim();
        if (!payload || payload === '[DONE]') continue;
        try {
          const json = JSON.parse(payload);
          const delta =
            (json.choices && json.choices[0] && json.choices[0].delta && json.choices[0].delta.content) || '';
          if (delta) {
            fullText += delta;
            send({ type: 'delta', content: delta });
          }
        } catch (e) {
          // 忽略个别无法解析的分片
        }
      }
    }
  } catch (e) {
    send({ type: 'error', content: '流读取中断：' + e.message });
  }

  // 保存起名历史到 Supabase 的 name_history 表
  if (fullText && process.env.SUPABASE_URL && process.env.SUPABASE_ANON_KEY) {
    try {
      const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
      const { error } = await supabase.from('name_history').insert({
        user_requirement: String(requirement),
        name_result: fullText,
        model_name: model
      });
      if (error) {
        send({ type: 'warn', content: '起名结果已生成，但历史保存失败：' + error.message });
      }
    } catch (e) {
      send({ type: 'warn', content: '保存历史时出现异常：' + e.message });
    }
  }

  send({ type: 'done' });
  res.end();
};
