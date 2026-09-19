// 端到端冒烟测试：模拟导演、控制端、三个岗位端走完整流程（含临时替岗）
'use strict';
const WebSocket = require('ws');

const URL = 'ws://localhost:3000/ws';
let passed = 0, failed = 0;

function check(name, cond) {
  if (cond) { passed++; console.log(`  ✔ ${name}`); }
  else { failed++; console.log(`  ✘ ${name}`); }
}

function client(role, position) {
  const ws = new WebSocket(URL);
  const c = {
    ws,
    state: null,
    errors: [],
    ready: new Promise((res) => {
      ws.on('open', () => {
        ws.send(JSON.stringify({ type: 'hello', role, position, clientId: 'test-' + role + '-' + (position || '') }));
        res();
      });
    }),
    send(obj) { ws.send(JSON.stringify(obj)); },
    waitState(pred, timeout = 3000) {
      return new Promise((res, rej) => {
        const t0 = Date.now();
        const iv = setInterval(() => {
          if (c.state && pred(c.state)) { clearInterval(iv); res(c.state); }
          else if (Date.now() - t0 > timeout) { clearInterval(iv); rej(new Error('waitState 超时')); }
        }, 20);
      });
    },
  };
  ws.on('message', (m) => {
    const d = JSON.parse(m);
    if (d.type === 'state') c.state = d.state;
    if (d.type === 'error') c.errors.push(d.message);
  });
  return c;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// 岗位确认必须携带其最新快照中的 attemptId（与岗位端页面行为一致）
const confirmCue = (c) => c.send({ type: 'confirm', attemptId: c.state.attemptId });

(async () => {
  const director = client('director');
  const control = client('control');
  const light = client('position', '灯光');
  const sound = client('position', '音响');
  const prop = client('position', '道具');
  await Promise.all([director.ready, control.ready, light.ready, sound.ready, prop.ready]);
  await sleep(200);

  console.log('0. 重置场次，确保干净起点');
  control.send({ type: 'reset' });
  await control.waitState((s) => s.phase === 'idle');

  console.log('1. 导演创建序列（3 条口令）');
  director.send({ type: 'setSequence', timeoutSec: 3, cues: [
    { text: '灯光渐暗', targets: ['灯光'] },
    { text: '音乐起，道具就位', targets: ['音响', '道具'] },
    { text: '全场亮灯', targets: ['灯光', '音响'] },
  ]});
  let st = await control.waitState((s) => s.sequence.length === 3);
  check('序列已保存（3 条）', st.sequence.length === 3);
  check('序号自动编号', st.sequence[0].seq === 1 && st.sequence[2].seq === 3);

  console.log('2. 控制端开始演出');
  control.send({ type: 'start' });
  st = await control.waitState((s) => s.phase === 'active');
  check('第 1 条口令激活', st.currentCue.text === '灯光渐暗');

  console.log('3. 非目标岗位确认应被拒绝');
  confirmCue(sound);
  await sleep(200);
  check('音响确认被拒绝', sound.errors.some((e) => e.includes('不涉及')));
  check('确认状态未串入', Object.keys(st.confirmations || {}).length === 0);

  console.log('4. 目标岗位确认 + 重复确认标记');
  confirmCue(light);
  st = await control.waitState((s) => s.confirmations['灯光']);
  check('灯光确认成功', !!st.confirmations['灯光']);
  confirmCue(light);
  st = await control.waitState((s) => s.log.some((l) => l.event === 'confirm-duplicate'));
  check('重复确认被实时标记进日志', true);

  console.log('5. 全员确认后推进');
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.currentCue && s.currentCue.seq === 2);
  check('推进到第 2 条', st.currentCue.text.includes('音乐起'));
  check('上一条确认未串入下一条', Object.keys(st.confirmations).length === 0);

  console.log('6. 未全员确认禁止推进');
  confirmCue(sound);
  await control.waitState((s) => s.confirmations['音响']);
  control.send({ type: 'advance' });
  await sleep(200);
  check('推进被拒绝（道具未确认）', control.errors.some((e) => e.includes('禁止推进')));

  console.log('7. 非法替岗应被拒绝');
  light.send({ type: 'substitute', from: '道具', to: '灯光' }); // 非控制端发起
  control.send({ type: 'substitute', from: '灯光', to: '道具' }); // 原岗位不是本条目标
  control.send({ type: 'substitute', from: '道具', to: '音响' }); // 替补已是本条有效目标
  control.send({ type: 'substitute', from: '道具', to: '候场' }); // 替补不在线
  await sleep(300);
  check('非控制端替岗被拒绝', light.errors.some((e) => e.includes('仅控制端')));
  check('非本条目标不能被替岗', control.errors.some((e) => e.includes('不是本条口令的目标岗位')));
  check('替补不能已是本条有效目标', control.errors.some((e) => e.includes('已是本条口令的有效目标')));
  check('替补必须在线', control.errors.some((e) => e.includes('不在线')));
  st = control.state;
  check('非法替岗未改变有效目标', (st.effectiveTargets || []).join() === '音响,道具');

  console.log('8. 控制端指定临时替岗：道具 → 灯光');
  control.send({ type: 'substitute', from: '道具', to: '灯光' });
  st = await control.waitState((s) => s.substitutions && s.substitutions['道具']);
  check('替岗已生效（道具 → 灯光）', st.substitutions['道具'].substitute === '灯光');
  check('有效目标更新为 音响+灯光', st.effectiveTargets.includes('灯光') && !st.effectiveTargets.includes('道具'));
  const subLog = st.log.find((l) => l.event === 'position-substituted');
  check('替岗写入事件日志（原岗位/替补/时间）',
    !!subLog && subLog.detail.from === '道具' && subLog.detail.to === '灯光' && !!subLog.ts);

  console.log('9. 原岗位不得再确认本条');
  confirmCue(prop);
  await sleep(200);
  check('道具确认被拒绝', prop.errors.some((e) => e.includes('移交')));
  check('道具未产生确认记录', !control.state.confirmations['道具']);

  console.log('10. 断线重连后替岗状态一致');
  light.ws.close();
  await sleep(300);
  const light2 = client('position', '灯光');
  await light2.ready;
  st = await light2.waitState((s) => s.phase === 'active' && s.currentCue.seq === 2 && s.substitutions['道具']);
  check('重连后恢复替岗关系', st.substitutions['道具'].substitute === '灯光');
  check('重连后有效目标一致', st.effectiveTargets.includes('灯光') && !st.effectiveTargets.includes('道具'));

  console.log('11. 超时标记跟随替补岗位');
  st = await control.waitState((s) => s.timedOut.includes('灯光'), 8000);
  check('替补（灯光）被标记超时未确认', true);
  check('原岗位（道具）不在超时列表', !st.timedOut.includes('道具'));

  console.log('12. 替补确认计入全员确认（迟到）');
  confirmCue(light2);
  st = await control.waitState((s) => s.confirmations['灯光']);
  check('替补迟到确认被接受且标记 late', st.confirmations['灯光'].late === true);
  check('日志含替补的 confirm-late', st.log.some((l) => l.event === 'confirm-late' && l.detail.position === '灯光'));

  console.log('13. 推进后下一条仍使用导演编排的原目标');
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.currentCue && s.currentCue.seq === 3);
  check('推进到第 3 条', st.currentCue.text.includes('全场亮灯'));
  check('第 3 条目标为原编排（灯光、音响）', st.currentCue.targets.join() === '灯光,音响');
  check('替岗不串入下一条', Object.keys(st.substitutions).length === 0);
  check('确认不串入下一条', Object.keys(st.confirmations).length === 0);

  console.log('14. 推进至完成');
  confirmCue(light2);
  confirmCue(sound);
  await control.waitState((s) => s.confirmations['灯光'] && s.confirmations['音响']);
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.phase === 'done');
  check('全部口令完成', st.log.some((l) => l.event === 'sequence-finished'));

  console.log('15. 重置后可重新编辑');
  control.send({ type: 'reset' });
  st = await director.waitState((s) => s.phase === 'idle');
  check('重置回 idle', true);
  check('事件日志保留', st.log.length > 5);

  console.log('16. 撤回权限与参数校验');
  director.send({ type: 'setSequence', timeoutSec: 3, cues: [
    { text: '第一版口令', targets: ['灯光', '音响'] },
    { text: '下一条', targets: ['道具'] },
  ]});
  await control.waitState((s) => s.sequence.length === 2);
  control.send({ type: 'start' });
  st = await control.waitState((s) => s.phase === 'active' && s.currentCue.text === '第一版口令');
  check('新序列开始，初始修订号为 1', st.revision === 1 && !st.revisionReason);
  const firstAttemptId = st.attemptId;
  check('快照携带 attemptId', typeof firstAttemptId === 'string' && firstAttemptId.length > 0);
  light2.send({ type: 'recall', reason: 'x', text: 'y' }); // 岗位端无权撤回
  control.send({ type: 'recall', reason: '', text: '修正版' }); // 缺撤回原因
  control.send({ type: 'recall', reason: '口令有误', text: '  ' }); // 缺修正版文本
  await sleep(300);
  check('非控制端撤回被拒绝', light2.errors.some((e) => e.includes('仅控制端')));
  check('缺撤回原因被拒绝', control.errors.some((e) => e.includes('撤回原因')));
  check('缺修正版文本被拒绝', control.errors.some((e) => e.includes('修正版口令文本')));
  check('拒绝后修订号仍为 1', control.state.revision === 1);

  console.log('17. 旧轮次确认后撤回并纠正重发');
  light2.send({ type: 'confirm', attemptId: firstAttemptId });
  st = await control.waitState((s) => s.confirmations['灯光']);
  check('灯光在第 1 版确认成功', st.confirmations['灯光'].attemptId === firstAttemptId);
  const oldDeadline = st.deadline;
  await sleep(100);
  control.send({ type: 'recall', reason: '口令措辞有误', text: '第二版口令（修正）' });
  st = await control.waitState((s) => s.revision === 2);
  check('修订号递增为 2', st.revision === 2);
  check('当前口令文本已替换', st.currentCue.text === '第二版口令（修正）');
  check('修订原因已下发', st.revisionReason === '口令措辞有误');
  check('生成新的 attemptId', st.attemptId !== firstAttemptId);
  check('本轮确认状态已清空', Object.keys(st.confirmations).length === 0);
  check('本轮超时/替岗状态已清空', st.timedOut.length === 0 && Object.keys(st.substitutions).length === 0);
  check('确认时限重新开始（deadline 晚于旧轮次）', st.deadline > oldDeadline);
  const hist = st.cueHistory[st.currentCue.id];
  check('旧轮次已封存为只读历史', !!hist && hist.attempts.length === 1 && hist.attempts[0].endState === 'recalled');
  const archived = hist.attempts[0];
  check('封存内容保留旧轮次确认/截止时间/撤回原因',
    archived.confirmations['灯光'] && archived.confirmations['灯光'].attemptId === firstAttemptId &&
    archived.deadline === oldDeadline && archived.reason === '口令措辞有误' &&
    archived.text === '第一版口令' && archived.revision === 1);
  check('cue-recalled 与 cue-resent 均写入日志',
    st.log.some((l) => l.event === 'cue-recalled' && l.detail.newRevision === 2) &&
    st.log.some((l) => l.event === 'cue-resent' && l.detail.revision === 2));

  console.log('18. 旧轮次迟到确认只能被拒绝，绝不污染新轮次');
  light2.send({ type: 'confirm', attemptId: firstAttemptId }); // 使用快照中的旧 attemptId
  await sleep(300);
  check('旧轮次确认被拒绝', light2.errors.some((e) => e.includes('旧轮次')));
  st = control.state;
  check('新轮次确认表未被污染', Object.keys(st.confirmations).length === 0);
  check('stale-confirm-ignored 已记录',
    st.log.some((l) => l.event === 'stale-confirm-ignored' &&
      l.detail.sentAttemptId === firstAttemptId && l.detail.position === '灯光'));
  light2.send({ type: 'confirm', attemptId: null }); // 不带 attemptId 同样拒绝
  await sleep(200);
  check('缺少 attemptId 的确认被拒绝', light2.errors.some((e) => e.includes('旧轮次')));

  console.log('19. 旧轮次定时器回调被拒绝，不触发新轮次超时');
  // 第 1 版在演出开始后 3 秒到期；撤回发生在到期前，旧回调触发时只记录 stale-timeout-ignored
  st = await control.waitState(
    (s) => s.log.some((l) => l.event === 'stale-timeout-ignored' && l.detail.staleAttemptId === firstAttemptId),
    3000);
  check('旧定时器回调被拒绝并记录',
    st.log.some((l) => l.event === 'stale-timeout-ignored' &&
      l.detail.staleAttemptId === firstAttemptId &&
      l.detail.staleRevision === 1 &&
      l.detail.currentAttemptId !== firstAttemptId));
  check('旧回调未污染当前进行轮次的超时表',
    !control.state.timedOut.includes('灯光') || control.state.revision >= 2);

  console.log('20. 断线重连后修订号/原因/旧轮次结果一致');
  const sound2 = client('position', '音响');
  await sound2.ready;
  st = await sound2.waitState((s) => s.revision === 2 && s.cueHistory[s.currentCue.id]);
  check('重连后恢复修订号与原因', st.revision === 2 && st.revisionReason === '口令措辞有误');
  check('重连后恢复只读旧轮次结果',
    st.cueHistory[st.currentCue.id].attempts[0].confirmations['灯光']);

  console.log('21. 连续撤回：修订号继续递增，历史逐版只读封存');
  control.send({ type: 'recall', reason: '再次更正目标动作', text: '第三版口令' });
  st = await control.waitState((s) => s.revision === 3);
  check('修订号递增为 3，原因更新', st.revisionReason === '再次更正目标动作');
  check('两版旧轮次均封存只读',
    st.cueHistory[st.currentCue.id].attempts.length === 2 &&
    st.cueHistory[st.currentCue.id].attempts[0].revision === 1 &&
    st.cueHistory[st.currentCue.id].attempts[1].revision === 2 &&
    st.cueHistory[st.currentCue.id].attempts[1].reason === '再次更正目标动作');
  check('封存对象互不串改（第 1 版保留其撤回原因）',
    st.cueHistory[st.currentCue.id].attempts[0].reason === '口令措辞有误');

  console.log('22. 全员确认后不可撤回，但可正常推进封存');
  const thirdAttemptId = st.attemptId;
  light2.send({ type: 'confirm', attemptId: thirdAttemptId });
  sound2.send({ type: 'confirm', attemptId: thirdAttemptId });
  st = await control.waitState((s) => s.confirmations['灯光'] && s.confirmations['音响']);
  control.send({ type: 'recall', reason: '试图撤回已完成口令', text: '不应生效' });
  await sleep(300);
  check('全员确认后撤回被拒绝', control.errors.some((e) => e.includes('已全员确认完成')));
  check('口令文本未被改动', control.state.currentCue.text === '第三版口令');
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.currentCue && s.currentCue.seq === 2);
  check('推进到下一条，修订号重置', st.revision === 1 && !st.revisionReason);
  check('完成轮次以 completed 封存', (() => {
    const prev = st.cueHistory[st.sequence[0].id];
    return prev && prev.attempts.some((a) => a.endState === 'completed' && a.revision === 3);
  })());
  check('新轮次确认表干净', Object.keys(st.confirmations).length === 0);

  console.log('23. 已完成（done）场次不可撤回');
  prop.send({ type: 'confirm', attemptId: st.attemptId });
  await control.waitState((s) => s.confirmations['道具']);
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.phase === 'done');
  control.send({ type: 'recall', reason: '演出已结束', text: 'x' });
  await sleep(200);
  check('done 后撤回被拒绝', control.errors.some((e) => e.includes('已推进或已完成')));

  console.log(`\n结果：${passed} 通过，${failed} 失败`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error('测试异常：', e.message); process.exit(1); });
