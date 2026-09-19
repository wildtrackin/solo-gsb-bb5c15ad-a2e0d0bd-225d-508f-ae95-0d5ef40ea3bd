/* 公共前端逻辑：带自动重连的 WebSocket 封装。
 * clientId 存于 localStorage，断线重连后服务端以下发的全量状态快照恢复页面。 */
'use strict';

const CueClient = (() => {
  function getClientId() {
    let id = localStorage.getItem('cueClientId');
    if (!id) {
      id = crypto.randomUUID();
      localStorage.setItem('cueClientId', id);
    }
    return id;
  }

  /**
   * @param {object} opts
   * @param {string} opts.role - 'director' | 'control' | 'position'
   * @param {() => (string|null)} [opts.getPosition] - 岗位端返回岗位名
   * @param {(state: object) => void} opts.onState
   * @param {(message: string) => void} [opts.onError]
   * @param {(connected: boolean) => void} [opts.onConnChange]
   */
  function connect(opts) {
    let ws = null;
    let closed = false;

    function hello() {
      const payload = { type: 'hello', role: opts.role, clientId: getClientId() };
      if (opts.role === 'position' && opts.getPosition) {
        payload.position = opts.getPosition();
      }
      send(payload);
    }

    function open() {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      ws = new WebSocket(`${proto}://${location.host}/ws`);

      ws.onopen = () => {
        if (opts.onConnChange) opts.onConnChange(true);
        hello();
      };
      ws.onmessage = (ev) => {
        let data;
        try { data = JSON.parse(ev.data); } catch { return; }
        if (data.type === 'state') opts.onState(data.state);
        else if (data.type === 'error' && opts.onError) opts.onError(data.message);
      };
      ws.onclose = () => {
        if (opts.onConnChange) opts.onConnChange(false);
        if (!closed) setTimeout(open, 1000); // 断线自动重连
      };
      ws.onerror = () => ws.close();
    }

    function send(payload) {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(payload));
        return true;
      }
      return false;
    }

    open();
    return { send, rehello: hello, close: () => { closed = true; ws && ws.close(); } };
  }

  function fmtTime(isoOrMs) {
    const d = new Date(isoOrMs);
    return d.toLocaleTimeString('zh-CN', { hour12: false });
  }

  const EVENT_LABELS = {
    'sequence-set': '序列已保存',
    'session-start': '演出开始',
    'cue-started': '发出口令',
    'cue-completed': '口令完成',
    'cue-timeout': '确认超时',
    'cue-recalled': '撤回并纠正重发',
    'cue-resent': '修订版重新发出',
    'stale-confirm-ignored': '旧轮次确认被拒',
    'stale-timeout-ignored': '旧轮次定时器被拒',
    'stale-substitute-ignored': '旧轮次替岗被拒',
    'confirm': '岗位确认',
    'confirm-late': '迟到确认',
    'confirm-duplicate': '重复确认',
    'position-substituted': '临时替岗',
    'sequence-finished': '全部口令完成',
    'session-reset': '场次重置',
    'position-online': '岗位上线',
    'position-offline': '岗位离线',
  };

  function renderLog(log) {
    return log.slice().reverse().map((e) => {
      const label = EVENT_LABELS[e.event] || e.event;
      const detail = Object.entries(e.detail || {})
        .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join('/') : JSON.stringify(v)}`)
        .join(' ');
      const cls = ['cue-timeout', 'confirm-duplicate', 'confirm-late',
        'stale-confirm-ignored', 'stale-timeout-ignored', 'stale-substitute-ignored',
        'cue-recalled'].includes(e.event)
        ? 'log-warn' : '';
      return `<div class="log-row ${cls}"><span class="log-ts">${fmtTime(e.ts)}</span>` +
        `<span class="log-event">${label}</span><span class="log-detail">${escapeHtml(detail)}</span></div>`;
    }).join('');
  }

  // 渲染某条口令已封存的只读历史轮次（撤回/推进封存，新轮次不可再修改）
  function renderAttempts(hist, cue) {
    if (!hist || !Array.isArray(hist.attempts) || hist.attempts.length === 0) return '';
    return hist.attempts.slice().reverse().map((a) => {
      const subs = a.substitutions || {};
      const effective = (cue.targets || []).map((t) => (subs[t] ? subs[t].substitute : t));
      const confs = a.confirmations || {};
      const confirmed = Object.keys(confs);
      const timedOut = a.timedOut || [];
      const pending = effective.filter((t) => !confirmed.includes(t) && !timedOut.includes(t));
      const chips = [
        ...confirmed.map((t) =>
          `<span class="chip ok">${escapeHtml(t)} · 已确认${confs[t].late ? '（迟到）' : ''}</span>`),
        ...timedOut.map((t) => `<span class="chip timeout">${escapeHtml(t)} · 超时未确认</span>`),
        ...pending.map((t) => `<span class="chip">${escapeHtml(t)} · 未确认（轮次${a.endState === 'recalled' ? '撤回' : '结束'}）</span>`),
      ].join('');
      const subLine = Object.keys(subs).length
        ? `<div class="attempt-sub">替岗：${Object.entries(subs).map(([from, s]) =>
          `${escapeHtml(from)} → ${escapeHtml(s.substitute)}`).join('；')}</div>`
        : '';
      const stateBadge = a.endState === 'recalled'
        ? `<span class="chip timeout">第 ${a.revision} 版 · 已撤回封存</span>`
        : `<span class="chip ok">第 ${a.revision} 版 · 已完成封存</span>`;
      const reasonLine = a.endState === 'recalled' && a.reason
        ? `<div class="attempt-reason">撤回原因：${escapeHtml(a.reason)}</div>` : '';
      return `<div class="attempt-row ${a.endState}">
        <div class="attempt-head">${stateBadge}
          <span class="muted">${fmtTime(a.startedAt)} – ${fmtTime(a.endedAt)}</span></div>
        <div class="attempt-text">旧口令：${escapeHtml(a.text)}</div>
        ${reasonLine}
        <div class="attempt-chips">${chips || '<span class="muted">无确认记录</span>'}</div>
        ${subLine}
      </div>`;
    }).join('');
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  return { connect, getClientId, fmtTime, renderLog, renderAttempts, escapeHtml };
})();
