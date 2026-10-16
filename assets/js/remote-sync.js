/* RP-Hub remote sync (self-hosted backend, server-authoritative).
 *
 * 语义（本机部署定制，非上游功能）：
 *   * 服务端为准：服务端持有权威副本，本地 IndexedDB 只当缓存；
 *   * 写入先落本地（保证界面不卡），随后批量推服务端；
 *   * 服务端若判冲突（本地 base_rev 落后），**不做合并**，直接把服务端版本
 *     覆盖回本地，本地那次改动丢弃；
 *   * 拉取：长轮询 /watch（有变更立刻回，单轮最多 25s）+ 60s 兜底轮询 +
 *     页面重新可见/获得焦点/网络恢复时各拉一次；
 *   * **关键**：拉到「别的设备」的改动后必须让应用重新装载数据 —— 运行中的页面
 *     把数据读进了内存，只改 IndexedDB 界面不会变；而且页面之后任何一次保存都会
 *     用内存里的旧快照覆盖服务端，把对方的改动抹掉。所以：
 *       页面不可见 → 先把本地待推的推完，再静默 location.reload()；
 *       页面可见   → 不打断（可能正在流式回复），徽标变成「另一台设备有新数据 · 点击刷新」。
 *
 * 挂钩方式：本文件必须在上游 data-services.js 之后、app.js 之前加载，
 * 它替换 window.RPHubStorage 为带同步的包装层（app.js 是加载时解构的）。
 * 后端地址固定同源 /sync，鉴权由 Caddy Basic Auth 负责。
 */
(function () {
    'use strict';

    const base = window.RPHubStorage;
    if (!base) { console.error('[rp-sync] RPHubStorage 未就绪，同步未启用'); return; }

    const API = '/sync';
    const PREFIX = 'rp_hub_';
    const LEGACY_PREFIX = 'silly_tavern_';
    const K_STATE = 'rphub_sync_state';
    const K_OUTBOX = 'rphub_sync_outbox';
    const K_DEVICE = 'rphub_sync_device';
    const WATCH_TIMEOUT = 25;          // 长轮询单轮上限（秒）
    const FALLBACK_PULL = 60000;       // 长轮询没起来时的兜底轮询
    const FLUSH_DEBOUNCE = 900;
    const MAX_ENTRIES_PER_PUSH = 100;
    const MAX_OUTBOX = 800;

    const readJson = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch (_) { return d; } };
    const writeJson = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); } catch (_) { } };
    const sleep = ms => new Promise(r => setTimeout(r, ms));

    /* 值的内容指纹（FNV-1a，够快够用）。
     * 用途：上游应用每 ~10 秒会把整套状态原样重存一遍（内容没变）。
     * 那种回写如果照推，两台设备就会轮流用各自内存里的快照互相覆盖 —— 必须按内容去重。 */
    function hashValue(v) {
        let s;
        try { s = JSON.stringify(v); } catch (_) { return null; }
        if (typeof s !== 'string') return null;
        let h = 0x811c9dc5;
        for (let i = 0; i < s.length; i++) {
            h ^= s.charCodeAt(i);
            h = (h * 0x01000193) >>> 0;
        }
        return s.length + ':' + h.toString(16);
    }

    const device = (() => {
        let d = null;
        try { d = localStorage.getItem(K_DEVICE); } catch (_) { }
        if (!d) {
            d = 'dev-' + Math.random().toString(36).slice(2, 10);
            try { localStorage.setItem(K_DEVICE, d); } catch (_) { }
        }
        return d;
    })();

    let state = readJson(K_STATE, { rev: 0, baseRevs: {}, confirmed: {} });
    if (!state.confirmed) state.confirmed = {};      // 服务端已确认的内容指纹，用于丢弃无变化的回写
    let outbox = readJson(K_OUTBOX, {});
    let flushing = false;
    let pulling = false;
    let seeded = false;
    let watching = false;
    let remoteDirty = false;      // 有别的设备的改动，需要重新装载数据
    let status = 'idle';
    let detail = '';
    let pullTimer = null;
    let flushTimer = null;
    const inflight = new Set();     // 正在推送中的键（用于忽略自己刚写的那条长轮询回包）

    const logical = key => String(key || '').startsWith(PREFIX) ? String(key).slice(PREFIX.length) : String(key || '');
    const physical = name => name.startsWith(PREFIX) ? name : PREFIX + name;
    const cloneValue = v => { try { return JSON.parse(JSON.stringify(v)); } catch (_) { return v; } };

    /* 同一台设备开多个标签页时，两个实例各持一份内存状态会互相覆盖
     * （实测：同一个站点两个页签，regex 会在两个版本之间来回震荡）。
     * 共享 localStorage 只能挡住「内容相同」的回写，内容不同就得靠广播让兄弟页签重新装载。 */
    let chan = null;
    try { chan = new BroadcastChannel('rphub-sync'); } catch (_) { chan = null; }
    function announce(keys) {
        if (!chan || !keys || !keys.length) return;
        try { chan.postMessage({ from: device, rev: state.rev, keys: keys.slice(0, 50) }); } catch (_) { }
    }

    /* ------------------------------------------------------------ 状态指示 */
    let badge = null;
    const STATUS_TEXT = {
        idle: '同步待命', syncing: '同步中…', ok: '已同步', offline: '离线（本地已存）',
        conflict: '已用服务端版本覆盖本地', error: '同步出错'
    };
    function paint() {
        if (!badge) return;
        const pending = Object.keys(outbox).length;
        if (remoteDirty) {
            badge.textContent = '⟳ 另一台设备有新数据 · 点击刷新';
            badge.style.opacity = '1';
        } else {
            badge.textContent = (status === 'syncing' ? '⇅ ' : status === 'ok' ? '✓ ' : '⚠ ') +
                (STATUS_TEXT[status] || status) + (pending ? ` · ${pending} 项待推` : '');
            badge.style.opacity = (status === 'ok' && !pending) ? '0.35' : '0.92';
        }
        badge.title = detail || 'RP-Hub 同步状态 · 点击立即同步';
    }
    function setStatus(s, d = '') { status = s; detail = d; paint(); }
    function mountBadge() {
        if (badge || !document.body) return;
        badge = document.createElement('div');
        badge.id = 'rphub-sync-badge';
        badge.style.cssText = 'position:fixed;left:10px;bottom:10px;z-index:2147483000;padding:4px 9px;' +
            'border-radius:9999px;font:12px/1.5 system-ui,-apple-system,"PingFang SC",sans-serif;' +
            'background:rgba(20,20,24,.82);color:#f2f2f5;cursor:pointer;user-select:none;' +
            'backdrop-filter:blur(6px);transition:opacity .3s;opacity:.92;';
        badge.addEventListener('click', async () => {
            if (remoteDirty) { remoteDirty = false; paint(); await reloadNow(); return; }
            await flush(true);
            await pull(true);
        });
        document.body.appendChild(badge);
        paint();
    }

    /* ---------------------------------------------------------------- 网络 */
    async function api(path, options = {}) {
        const res = await fetch(API + path, Object.assign({ cache: 'no-store', credentials: 'same-origin' }, options));
        if (res.status === 401) throw new Error('未通过站点鉴权（Basic Auth）');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
    }

    async function applyServerEntry(entry) {
        const name = logical(entry.key);
        if (entry.deleted) { try { await base.deleteStoredValue(name); } catch (_) { } return; }
        await base.setStoredValue(name, entry.value, { clone: false });
    }

    /* ------------------------------------------------------------ 推送队列 */
    function enqueue(name, value, deleted) {
        const key = physical(name);
        if (key.startsWith(LEGACY_PREFIX)) return;
        const digest = deleted ? 'deleted' : hashValue(value);
        // 服务端已经是这个内容 → 不推。上游每 ~10 秒整套回写一次，照推会两台设备互相覆盖。
        if (digest !== null && state.confirmed[key] === digest) {
            if (outbox[key]) { delete outbox[key]; writeJson(K_OUTBOX, outbox); }
            paint();
            return;
        }
        if (Object.keys(outbox).length >= MAX_OUTBOX && !outbox[key]) return;
        outbox[key] = { value: deleted ? null : value, deleted: !!deleted, ts: Date.now(), digest };
        writeJson(K_OUTBOX, outbox);
        scheduleFlush();
        paint();
    }

    function scheduleFlush() {
        if (flushTimer) return;
        flushTimer = setTimeout(() => { flushTimer = null; flush(); }, FLUSH_DEBOUNCE);
    }

    async function flush(force = false) {
        if (flushing) return;
        const keys = Object.keys(outbox);
        if (!keys.length && !force) return;
        if (!keys.length) { if (!remoteDirty) setStatus('ok'); return; }
        flushing = true;
        if (!remoteDirty) setStatus('syncing');
        let unresolved = 0;
        const appliedKeys = [];
        try {
            for (let i = 0; i < keys.length; i += MAX_ENTRIES_PER_PUSH) {
                const slice = keys.slice(i, i + MAX_ENTRIES_PER_PUSH);
                slice.forEach(k => inflight.add(k));
                try {
                    const entries = slice.map(k => ({
                        key: k,
                        value: outbox[k].deleted ? null : outbox[k].value,
                        deleted: !!outbox[k].deleted,
                        base_rev: state.baseRevs[k] || 0
                    }));
                    const out = await api('/push', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ device, entries })
                    });
                    for (const r of out.results || []) {
                        const sent = outbox[r.key] || {};
                        if (r.status === 'applied') {
                            appliedKeys.push(r.key);
                            delete outbox[r.key];
                            state.baseRevs[r.key] = r.rev;
                            state.confirmed[r.key] = sent.deleted ? 'deleted' : (sent.digest || hashValue(sent.value));
                        } else if (r.status === 'conflict') {
                            // 服务端为准：丢弃本地这次改动，用服务端版本覆盖本地
                            delete outbox[r.key];
                            state.baseRevs[r.key] = r.rev;
                            await applyServerEntry(r);
                            state.confirmed[r.key] = r.deleted ? 'deleted' : hashValue(r.value);
                            setStatus('conflict');
                        } else {
                            unresolved++;
                        }
                    }
                    if (typeof out.rev === 'number') state.rev = Math.max(state.rev, out.rev);
                } finally {
                    slice.forEach(k => inflight.delete(k));
                }
            }
            if (appliedKeys.length) announce(appliedKeys);
            writeJson(K_OUTBOX, outbox);
            writeJson(K_STATE, state);
            if (unresolved) setStatus('error', `${unresolved} 项服务端拒绝`);
            else if (!remoteDirty) setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
        } catch (e) {
            setStatus('offline', String(e && e.message || e));
        } finally {
            flushing = false;
            paint();
        }
    }

    /* ---------------------------------------- 应用远端改动 → 让页面重新装载 */
    async function ingest(out) {
        let changed = 0;
        for (const e of out.entries || []) {
            if (String(e.key).startsWith(LEGACY_PREFIX)) continue;
            // rev 等于我们已知的该键版本 = 就是我们自己刚推上去的那份，不是别人的改动
            if (state.baseRevs[e.key] === e.rev) continue;
            // 正在推送中的、本设备自己写的那条：响应还没回来就被长轮询看到，别当成别人的改动
            if (e.device === device && inflight.has(e.key)) continue;
            await applyServerEntry(e);
            state.baseRevs[e.key] = e.rev;
            state.confirmed[e.key] = e.deleted ? 'deleted' : hashValue(e.value);
            changed++;
        }
        if (typeof out.rev === 'number') state.rev = Math.max(state.rev, out.rev);
        if (changed) { writeJson(K_STATE, state); await onRemoteChange(changed); }
        return changed;
    }

    async function onRemoteChange(n) {
        remoteDirty = true;
        paint();
        await flush();          // 本地待推的先送出去，否则重载会把它们丢掉
        if (Object.keys(outbox).length) { setStatus('offline', '仍有本地改动未推，暂不刷新'); return; }
        if (document.hidden) { location.reload(); return; }   // 用户没在看，静默刷新
        setStatus('ok', `${n} 项来自其他设备的更新，点击徽标刷新`);
    }

    async function reloadNow() {
        await flush();
        if (Object.keys(outbox).length) { setStatus('offline', '仍有本地改动未推，暂不刷新'); remoteDirty = true; paint(); return; }
        location.reload();
    }

    /* ---------------------------------------------------------------- 拉取 */
    async function pull(force = false) {
        if (pulling) return;
        pulling = true;
        try {
            if (!remoteDirty) setStatus('syncing');
            let cursor = state.rev;
            for (let round = 0; round < 60; round++) {
                const out = await api(`/snapshot?since=${cursor}&limit=200`);
                await ingest(out);
                cursor = Math.max(cursor, out.rev || 0);
                if (!out.has_more || !(out.entries || []).length) break;
                if (remoteDirty && !document.hidden) break;   // 等用户点刷新，别把他拽走
            }
            if (!remoteDirty) setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
            if (force && !remoteDirty) setStatus(Object.keys(outbox).length ? 'offline' : 'ok', '手动同步完成');
        } catch (e) {
            setStatus('offline', String(e && e.message || e));
        } finally {
            pulling = false;
        }
    }

    async function watchLoop() {
        if (watching) return;
        watching = true;
        while (true) {
            try {
                // 长轮询：服务端一旦有别人的改动就立刻返回，不用等固定间隔
                const out = await api(`/watch?since=${state.rev}&timeout=${WATCH_TIMEOUT}&limit=200`);
                if (!remoteDirty) setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
                await ingest(out);
            } catch (e) {
                setStatus('offline', String(e && e.message || e));
                await sleep(5000);
            }
            if (!watching) return;
        }
    }

    /* ------------------------------------------------ 首次运行：本地上行种子 */
    async function seed() {
        if (seeded) return;
        seeded = true;
        const db = base.getMainDb && base.getMainDb();
        if (!db) return;
        const keys = (await base.readStorageKeys(db)).filter(k => k.startsWith(PREFIX) && !k.startsWith(LEGACY_PREFIX));
        if (!keys.length) return;
        const payload = [];
        await base.scanStorageEntries(db, null, (_s, key, value) => {
            if (keys.includes(key)) payload.push({ key, value, base_rev: 0 });
        });
        for (let i = 0; i < payload.length; i += MAX_ENTRIES_PER_PUSH) {
            const slice = payload.slice(i, i + MAX_ENTRIES_PER_PUSH).map(e => ({ ...e, value: cloneValue(e.value) }));
            try {
                const out = await api('/push', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ device, entries: slice })
                });
                for (const r of out.results || []) {
                    if (r.status === 'applied') {
                        state.baseRevs[r.key] = r.rev;
                        const src = payload.find(p => p.key === r.key);
                        state.confirmed[r.key] = src ? hashValue(src.value) : null;
                    } else if (r.status === 'conflict') {
                        await applyServerEntry(r);
                        state.baseRevs[r.key] = r.rev;
                        state.confirmed[r.key] = r.deleted ? 'deleted' : hashValue(r.value);
                    }
                }
                if (typeof out.rev === 'number') state.rev = Math.max(state.rev, out.rev);
            } catch (e) {
                setStatus('offline', '首次上送失败：' + String(e && e.message || e));
                return;
            }
        }
        writeJson(K_STATE, state);
    }

    /* ------------------------------------------------------------ 存储层包装 */
    function wrap(name, fn) {
        const original = base[name];
        return typeof original === 'function' ? fn(original.bind(base)) : original;
    }

    const patched = Object.create(null);
    for (const key of Object.keys(base)) patched[key] = base[key];

    patched.getStoredValue = wrap('getStoredValue', orig => async (name) => orig(name));
    patched.setStoredValue = wrap('setStoredValue', orig => async (name, value, options = {}) => {
        const result = await orig(name, value, options);
        enqueue(name, options && options.clone === false ? cloneValue(value) : value, false);
        return result;
    });
    patched.deleteStoredValue = wrap('deleteStoredValue', orig => async (name) => {
        const result = await orig(name);
        enqueue(name, null, true);
        return result;
    });
    patched.getScopedStoredValue = wrap('getScopedStoredValue', orig => async (name, id) => orig(name, id));
    patched.setScopedStoredValue = wrap('setScopedStoredValue', orig => async (name, id, value, options = {}) => {
        const result = await orig(name, id, value, options);
        enqueue(`${name}_${id}`, options && options.clone === false ? cloneValue(value) : value, false);
        return result;
    });
    patched.deleteScopedStoredValue = wrap('deleteScopedStoredValue', orig => async (name, id) => {
        const result = await orig(name, id);
        enqueue(`${name}_${id}`, null, true);
        return result;
    });
    patched.deleteStorageKeys = wrap('deleteStorageKeys', orig => async (db, keys) => {
        const result = await orig(db, keys);
        (keys || []).forEach(k => enqueue(logical(k), null, true));
        return result;
    });

    window.RPHubStorage = patched;
    window.RPHubSync = {
        get status() { return status; },
        state: () => ({ rev: state.rev, baseRevs: state.baseRevs, device, outbox: Object.keys(outbox).length, remoteDirty }),
        pull: () => pull(true),
        flush: () => flush(true),
        reload: reloadNow,
        seed
    };

    /* ---------------------------------------------------------------- 启动 */
    /* 关键顺序：**先拉服务端再让应用读数据**。
     * 否则页面会拿本地旧数据启动，然后照旧写回服务端，把别的设备（或服务端）的数据抹掉。
     * index.html 的加载器会等 window.RPHubSyncReady 之后再加载 app.js。 */
    async function prime() {
        if (typeof base.initDB === 'function') await base.initDB();
        mountBadge();
        setStatus('syncing');
        const probe = await api('/health');
        if ((probe.rev || 0) === 0 && !state.rev) await seed();
        await pull();
        await flush();
        if (!remoteDirty) setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
    }

    window.RPHubSyncReady = (async () => {
        try {
            // 后端不可达时不能把页面永久卡住：最多等 6 秒照常放行（离线可用优先）
            await Promise.race([prime(), sleep(6000)]);
        } catch (e) {
            setStatus('offline', String(e && e.message || e));
        }
    })();

    function startLoops() {
        watchLoop();
        // 同一台设备的另一个页签刚写入 → 本页内存已过期，走同样的「重新装载」路径
        if (chan) {
            chan.onmessage = (e) => {
                const d = e.data || {};
                if (d.from === device) return;
                if (typeof d.rev === 'number' && d.rev <= state.rev) return;
                onRemoteChange((d.keys || []).length);
            };
        }
        if (pullTimer) clearInterval(pullTimer);
        pullTimer = setInterval(() => { if (!watching) pull(); }, FALLBACK_PULL);
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) {
                flush();
                if (remoteDirty) { remoteDirty = false; paint(); reloadNow(); return; }
                if (!watching) pull();
            } else if (remoteDirty) {
                reloadNow();     // 切走时静默刷新，回来就是别的设备的数据
            }
        });
        window.addEventListener('online', () => { flush(); pull(); });
        window.addEventListener('focus', () => { if (!remoteDirty) { flush(); pull(); } });
    }

    if (document.readyState === 'loading') {
        // 循环必须等首次拉取完成再起，否则会把自己刚推上去的数据认成「别的设备的改动」
        document.addEventListener('DOMContentLoaded', () => { Promise.resolve(window.RPHubSyncReady).then(startLoops); });
    } else {
        Promise.resolve(window.RPHubSyncReady).then(startLoops);
    }
})();
