'use strict';

const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const DEFAULT_TIMEOUT_SEC = 30;
const MAX_LOG_ENTRIES = 500;

fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------------------------------------------------------------------------
// 会话状态（单场次，内存态；事件日志同时落盘到 data/events-<sessionId>.jsonl）
// ---------------------------------------------------------------------------
const state = {
  sessionId: crypto.randomUUID(),
  phase: 'idle', // idle | active | done
  sequence: [], // [{ id, seq, text, targets: [string] }]
  timeoutSec: DEFAULT_TIMEOUT_SEC,
  currentIndex: -1,
  attemptId: null, // 当前轮次（某次激活/某次修订重发）的唯一凭证，岗位确认必须原样带回
  // 每个口令的修订档案：cueId -> { revision, currentReason, attempts: [只读历史轮次] }
  cueHistory: {},
  confirmations: {}, // 当前轮次：position -> { clientId, at, late, attemptId }
  timedOut: [], // 当前轮次：超时未确认的岗位
  substitutions: {}, // 当前轮次的临时替岗：原岗位 -> { substitute, at }，仅对本轮次生效
  cueStartedAt: null,
  deadline: null,
  log: [], // [{ ts, event, detail }]
};

let timeoutTimer = null;
const logStream = fs.createWriteStream(
  path.join(DATA_DIR, `events-${state.sessionId}.jsonl`),
  { flags: 'a' }
);

function addLog(event, detail = {}) {
  const entry = { ts: new Date().toISOString(), event, detail };
  state.log.push(entry);
  if (state.log.length > MAX_LOG_ENTRIES) state.log.shift();
  logStream.write(JSON.stringify(entry) + '\n');
  return entry;
}

function currentCue() {
  if (state.phase !== 'active') return null;
  return state.sequence[state.currentIndex] || null;
}

// 某条口令的修订档案（修订号、修订原因、只读历史轮次）
function cueHistoryOf(cue) {
  if (!cue) return null;
  return state.cueHistory[cue.id] || null;
}

// 冻结当前轮次快照为只读历史（撤回/推进封存时使用，深拷贝确保之后无法被修改）
function snapshotCurrentAttempt(cue, endState, reason) {
  return {
    attemptId: state.attemptId,
    revision: cueHistoryOf(cue) ? cueHistoryOf(cue).revision : 1,
    text: cue.text,
    startedAt: state.cueStartedAt,
    deadline: state.deadline,
    endedAt: Date.now(),
    endState, // completed（全员确认后推进封存） | recalled（撤回纠正重发）
    reason: reason || null, // endState === recalled 时为撤回原因
    confirmations: structuredClone(state.confirmations),
    timedOut: structuredClone(state.timedOut),
    substitutions: structuredClone(state.substitutions),
  };
}

// 当前口令的有效目标：被替岗的原岗位由其替补顶替，仅对当前口令生效
function effectiveTargets(cue) {
  if (!cue) return [];
  return cue.targets.map((t) => {
    const sub = state.substitutions[t];
    return sub ? sub.substitute : t;
  });
}

function allConfirmed() {
  const cue = currentCue();
  if (!cue) return false;
  return effectiveTargets(cue).every((t) => state.confirmations[t]);
}

// 断线重连恢复依赖全量状态快照：任何变化都广播完整公开状态
function publicState() {
  const cue = currentCue();
  const hist = cueHistoryOf(cue);
  return {
    sessionId: state.sessionId,
    phase: state.phase,
    sequence: state.sequence,
    timeoutSec: state.timeoutSec,
    currentIndex: state.currentIndex,
    currentCue: cue,
    attemptId: state.attemptId,
    revision: hist ? hist.revision : 1,
    revisionReason: hist && hist.revision > 1 ? hist.currentReason : null,
    cueHistory: state.cueHistory,
    confirmations: state.confirmations,
    timedOut: state.timedOut,
    substitutions: state.substitutions,
    effectiveTargets: effectiveTargets(cue),
    cueStartedAt: state.cueStartedAt,
    deadline: state.deadline,
    serverNow: Date.now(),
    onlinePositions: [...onlinePositions],
    log: state.log.slice(-200),
  };
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const onlinePositions = new Set(); // 当前在线的岗位名

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) client.send(msg);
  }
}

function broadcastState() {
  broadcast({ type: 'state', state: publicState() });
}

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

// ---------------------------------------------------------------------------
// 口令推进逻辑
// ---------------------------------------------------------------------------
function clearCueTimer() {
  if (timeoutTimer) {
    clearTimeout(timeoutTimer);
    timeoutTimer = null;
  }
}

// 开启一轮全新确认时限（首次激活或撤回后修订重发均经此入口）。
// attemptId 为本轮唯一凭证；确认/超时/替岗/截止时间只属于本轮，绝不带入新轮次。
// cancelOldTimer=false（撤回场景）时不清除旧轮次定时器：让其到期后凭 attemptId 被拒绝并记录，
// 即“迟到的旧定时器回调只能被拒绝、绝不污染新轮次”；正常推进/重置仍直接取消旧定时器。
function startRound(cue, revision, startLog, { cancelOldTimer = true } = {}) {
  if (cancelOldTimer) clearCueTimer();
  // 确保该口令存在修订档案（第 1 版开始时建立；attempts 仅在封存时追加）
  if (!state.cueHistory[cue.id]) {
    state.cueHistory[cue.id] = { revision: 1, currentReason: null, attempts: [] };
  }
  state.attemptId = crypto.randomUUID();
  state.phase = 'active';
  // 关键：每轮的确认/超时/替岗状态独立，绝不串入下一轮/下一条（下一条仍用导演编排的原目标）
  state.confirmations = {};
  state.timedOut = [];
  state.substitutions = {};
  state.cueStartedAt = Date.now();
  state.deadline = state.cueStartedAt + state.timeoutSec * 1000;

  const attemptId = state.attemptId;
  if (startLog) {
    addLog(startLog.event, { ...startLog.detail, revision, attemptId });
  }

  timeoutTimer = setTimeout(() => {
    const cueNow = currentCue();
    // 旧轮次定时器回调：轮次已撤回/口令已推进，只记录、拒绝，绝不污染新轮次
    if (!cueNow || cueNow.id !== cue.id || state.attemptId !== attemptId) {
      addLog('stale-timeout-ignored', {
        seq: cue.seq,
        staleRevision: revision,
        staleAttemptId: attemptId,
        currentAttemptId: state.attemptId,
      });
      broadcastState();
      return;
    }
    const missing = effectiveTargets(cueNow).filter((t) => !state.confirmations[t]);
    if (missing.length === 0) return;
    state.timedOut = missing;
    addLog('cue-timeout', { seq: cueNow.seq, revision, attemptId, missing });
    broadcastState();
  }, state.timeoutSec * 1000);
}

function activateCue(index) {
  state.currentIndex = index;
  state.phase = 'active';
  const cue = state.sequence[index];
  startRound(cue, 1, { event: 'cue-started', detail: { seq: cue.seq, text: cue.text, targets: cue.targets } });
  broadcastState();
}

// 撤回并纠正重发：封存旧轮次为只读历史 → 替换口令文本、递增修订号 → 清空本轮状态并重新开始完整确认时限
function recallCue(ws, reason, newText) {
  const cue = currentCue();
  if (!cue) return sendError(ws, '当前没有进行中的口令，无法撤回');

  const hist = state.cueHistory[cue.id] || { revision: 1, currentReason: null, attempts: [] };
  const oldRevision = hist.revision;
  const oldAttemptId = state.attemptId;

  // 先封存旧轮次（在修改口令文本之前快照），旧轮次此后只读
  hist.attempts.push(snapshotCurrentAttempt(cue, 'recalled', reason));

  cue.text = newText;
  hist.revision = oldRevision + 1;
  hist.currentReason = reason;
  state.cueHistory[cue.id] = hist;

  addLog('cue-recalled', {
    seq: cue.seq,
    oldRevision,
    newRevision: hist.revision,
    oldAttemptId,
    reason,
    newText,
  });

  // 清空本轮状态并重新开始完整确认时限。
  // 不取消旧轮次定时器：它到期后凭 attemptId 被拒绝并记录（旧回调先于新截止时间触发）。
  startRound(cue, hist.revision, {
    event: 'cue-resent',
    detail: { seq: cue.seq, text: newText, reason },
  }, { cancelOldTimer: false });
  broadcastState();
}

function finishSequence() {
  clearCueTimer();
  state.phase = 'done';
  state.currentIndex = state.sequence.length;
  state.attemptId = null;
  state.confirmations = {};
  state.timedOut = [];
  state.substitutions = {};
  state.cueStartedAt = null;
  state.deadline = null;
  addLog('sequence-finished', {});
  broadcastState();
}

function handleMessage(ws, msg) {
  let data;
  try {
    data = JSON.parse(msg);
  } catch {
    return sendError(ws, '无法解析的消息');
  }

  switch (data.type) {
    case 'hello': {
      const prev = ws.meta;
      // 同一连接换岗：先移除旧岗位在线记录
      if (prev && prev.role === 'position' && prev.position && prev.position !== data.position) {
        const stillUsed = [...wss.clients].some(
          (c) => c !== ws && c.meta && c.meta.position === prev.position && c.readyState === c.OPEN
        );
        if (!stillUsed) onlinePositions.delete(prev.position);
      }
      ws.meta = {
        role: data.role,
        position: data.position || null,
        clientId: data.clientId || null,
      };
      if (data.role === 'position' && data.position) {
        onlinePositions.add(data.position);
        addLog('position-online', { position: data.position });
        broadcastState();
      } else {
        send(ws, { type: 'state', state: publicState() });
      }
      break;
    }

    case 'setSequence': {
      if (state.phase !== 'idle') {
        return sendError(ws, '演出进行中，请先重置再修改序列');
      }
      const cues = Array.isArray(data.cues) ? data.cues : [];
      const cleaned = cues
        .map((c) => ({
          text: String(c.text || '').trim(),
          targets: [...new Set((Array.isArray(c.targets) ? c.targets : [])
            .map((t) => String(t).trim())
            .filter(Boolean))],
        }))
        .filter((c) => c.text && c.targets.length > 0);
      if (cleaned.length === 0) {
        return sendError(ws, '序列不能为空，每条口令需包含口令内容与至少一个目标岗位');
      }
      state.sequence = cleaned.map((c, i) => ({
        id: crypto.randomUUID(),
        seq: i + 1,
        text: c.text,
        targets: c.targets,
      }));
      const timeoutSec = Number(data.timeoutSec);
      state.timeoutSec =
        Number.isFinite(timeoutSec) && timeoutSec >= 3 && timeoutSec <= 600
          ? Math.round(timeoutSec)
          : DEFAULT_TIMEOUT_SEC;
      state.currentIndex = -1;
      state.attemptId = null;
      state.cueHistory = {};
      addLog('sequence-set', {
        count: state.sequence.length,
        timeoutSec: state.timeoutSec,
        cues: state.sequence.map((c) => ({ seq: c.seq, text: c.text, targets: c.targets })),
      });
      broadcastState();
      break;
    }

    case 'start': {
      if (state.phase === 'active') return sendError(ws, '口令正在进行中');
      if (state.sequence.length === 0) return sendError(ws, '请先由导演端创建演出序列');
      if (state.phase === 'done') return sendError(ws, '本场已结束，请重置后重新开始');
      addLog('session-start', {});
      activateCue(0);
      break;
    }

    case 'advance': {
      if (state.phase !== 'active') return sendError(ws, '当前没有进行中的口令');
      if (!allConfirmed()) {
        const cue = currentCue();
        const missing = effectiveTargets(cue).filter((t) => !state.confirmations[t]);
        return sendError(ws, `以下岗位尚未确认，禁止推进：${missing.join('、')}`);
      }
      const doneCue = currentCue();
      const doneHist = state.cueHistory[doneCue.id] || { revision: 1, currentReason: null, attempts: [] };
      const doneRevision = doneHist.revision;
      // 已完成轮次封存为只读历史，随后旧轮次的迟到确认/定时器回调只能被拒绝
      doneHist.attempts.push(snapshotCurrentAttempt(doneCue, 'completed', null));
      state.cueHistory[doneCue.id] = doneHist;
      addLog('cue-completed', { seq: doneCue.seq, revision: doneRevision, attemptId: state.attemptId });
      if (state.currentIndex + 1 >= state.sequence.length) {
        finishSequence();
      } else {
        activateCue(state.currentIndex + 1);
      }
      break;
    }

    case 'reset': {
      clearCueTimer();
      state.phase = 'idle';
      state.currentIndex = -1;
      state.attemptId = null;
      state.cueHistory = {};
      state.confirmations = {};
      state.timedOut = [];
      state.substitutions = {};
      state.cueStartedAt = null;
      state.deadline = null;
      addLog('session-reset', {});
      broadcastState();
      break;
    }

    case 'confirm': {
      const meta = ws.meta || {};
      if (meta.role !== 'position' || !meta.position) {
        return sendError(ws, '仅岗位端可以确认口令');
      }
      const cue = currentCue();
      if (!cue) return sendError(ws, '当前没有进行中的口令');
      const hist = cueHistoryOf(cue);
      const revision = hist ? hist.revision : 1;
      // 旧轮次确认（迟到到达）：attemptId 与当前轮次不符，只拒绝并记录，绝不污染新轮次
      if (!data.attemptId || data.attemptId !== state.attemptId) {
        addLog('stale-confirm-ignored', {
          seq: cue.seq,
          position: meta.position,
          sentAttemptId: data.attemptId || null,
          currentAttemptId: state.attemptId,
          currentRevision: revision,
        });
        broadcastState();
        return sendError(ws, '该确认属于已撤回/已结束的旧轮次，已被拒绝；请按最新修订版口令重新确认');
      }
      const position = meta.position;
      if (!effectiveTargets(cue).includes(position)) {
        // 原岗位被替岗后，确认权已移交替补，不得再确认本轮
        if (cue.targets.includes(position) && state.substitutions[position]) {
          return sendError(ws, `本岗位的确认任务已移交「${state.substitutions[position].substitute}」，无需再确认`);
        }
        return sendError(ws, `本条口令不涉及岗位「${position}」`);
      }
      // 重复确认：实时标记进日志，不改变状态
      if (state.confirmations[position]) {
        addLog('confirm-duplicate', { seq: cue.seq, revision, attemptId: state.attemptId, position });
        broadcastState();
        return;
      }
      const late = state.timedOut.includes(position);
      state.confirmations[position] = {
        clientId: meta.clientId,
        at: Date.now(),
        late,
        attemptId: state.attemptId,
      };
      if (late) state.timedOut = state.timedOut.filter((t) => t !== position);
      addLog(late ? 'confirm-late' : 'confirm', {
        seq: cue.seq, revision, attemptId: state.attemptId, position,
      });
      broadcastState();
      break;
    }

    case 'substitute': {
      // 临时替岗：控制端指定一名在线替补，接管当前口令某个未确认目标
      const meta = ws.meta || {};
      if (meta.role !== 'control') {
        return sendError(ws, '仅控制端可以指定临时替岗');
      }
      const cue = currentCue();
      if (!cue) return sendError(ws, '当前没有进行中的口令');
      // 旧轮次的替岗请求（撤回后迟到到达）：拒绝并记录
      if (data.attemptId && data.attemptId !== state.attemptId) {
        addLog('stale-substitute-ignored', {
          seq: cue.seq,
          sentAttemptId: data.attemptId || null,
          currentAttemptId: state.attemptId,
        });
        broadcastState();
        return sendError(ws, '该替岗操作属于已撤回/已结束的旧轮次，已被拒绝');
      }
      const from = String(data.from || '').trim();
      const to = String(data.to || '').trim();
      if (!from || !to) return sendError(ws, '请指定被替岗的原岗位与替补岗位');
      if (!cue.targets.includes(from)) {
        return sendError(ws, `「${from}」不是本条口令的目标岗位`);
      }
      if (state.substitutions[from]) {
        return sendError(ws, `「${from}」的确认任务已被「${state.substitutions[from].substitute}」接管`);
      }
      if (state.confirmations[from]) {
        return sendError(ws, `「${from}」已确认，无需替岗`);
      }
      if (!onlinePositions.has(to)) {
        return sendError(ws, `替补岗位「${to}」不在线，无法接管`);
      }
      // 替补不能已是本条有效目标（含原目标、其他替补及 from 本身）
      if (effectiveTargets(cue).includes(to)) {
        return sendError(ws, `「${to}」已是本条口令的有效目标，不能作为替补`);
      }
      // 原子更新：仅当前口令的有效目标与确认权限，下一条口令不受影响
      state.substitutions[from] = { substitute: to, at: Date.now() };
      // 超时未确认标记随确认权一起移交给替补
      if (state.timedOut.includes(from)) {
        state.timedOut = state.timedOut.map((t) => (t === from ? to : t));
      }
      addLog('position-substituted', {
        seq: cue.seq, revision: cueHistoryOf(cue).revision, attemptId: state.attemptId, from, to,
      });
      broadcastState();
      break;
    }

    case 'recall': {
      // 撤回并纠正重发：仅控制端可发起，须填写撤回原因与修正版口令文本
      const meta = ws.meta || {};
      if (meta.role !== 'control') {
        return sendError(ws, '仅控制端可以撤回并纠正重发口令');
      }
      if (state.phase !== 'active' || !currentCue()) {
        return sendError(ws, '仅可撤回当前进行中的口令；已推进或已完成的口令不可撤回');
      }
      if (allConfirmed()) {
        return sendError(ws, '当前口令已全员确认完成，不可撤回；如需更正请在下一条口令中说明');
      }
      const reason = String(data.reason || '').trim();
      const newText = String(data.text || '').trim();
      if (!reason) return sendError(ws, '请填写撤回原因');
      if (!newText) return sendError(ws, '请填写修正版口令文本');
      recallCue(ws, reason, newText);
      break;
    }

    default:
      sendError(ws, `未知消息类型：${data.type}`);
  }
}

wss.on('connection', (ws) => {
  ws.on('message', (msg) => handleMessage(ws, msg));
  ws.on('close', () => {
    const meta = ws.meta;
    if (meta && meta.role === 'position' && meta.position) {
      // 同一岗位可能有多个连接，全部断开才算离线
      const stillOnline = [...wss.clients].some(
        (c) => c !== ws && c.meta && c.meta.position === meta.position && c.readyState === c.OPEN
      );
      if (!stillOnline) {
        onlinePositions.delete(meta.position);
        addLog('position-offline', { position: meta.position });
        broadcastState();
      }
    }
  });
  ws.on('error', () => {});
});

server.listen(PORT, () => {
  console.log(`舞台口令同步系统已启动: http://localhost:${PORT}`);
  console.log(`  导演端:   http://localhost:${PORT}/director.html`);
  console.log(`  控制端:   http://localhost:${PORT}/control.html`);
  console.log(`  岗位端:   http://localhost:${PORT}/position.html`);
  console.log(`  事件日志: data/events-${state.sessionId}.jsonl`);
});
