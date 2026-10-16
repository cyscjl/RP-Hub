/* RP-Hub remote sync (self-hosted backend, server-authoritative).
 *
 * 语义（本机部署定制，非上游功能）：
 *   * 服务端为准：服务端持有权威副本，本地 IndexedDB 只当缓存；
 *   * 写入先落本地（保证界面不卡），随后批量推服务端；
 *   * 服务端若判冲突（本地 base_rev 落后），**不做合并**，直接把服务端版本
 *     覆盖回本地，本地那次改动丢弃；
 *   * 拉取：启动时 + 每 30 秒（页面可见时）+ 页面重新可见/网络恢复时。
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
    const PULL_INTERVAL = 30000;
    const FLUSH_DEBOUNCE = 900;
    const MAX_ENTRIES_PER_PUSH = 100;
    const MAX_OUTBOX = 800;

    const readJson = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch (_) { return d; } };
    const writeJson = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); } catch (_) { } };

    const device = (() => {
        let d = null;
        try { d = localStorage.getItem(K_DEVICE); } catch (_) { }
        if (!d) {
            d = 'dev-' + Math.random().toString(36).slice(2, 10);
            try { localStorage.setItem(K_DEVICE, d); } catch (_) { }
        }
        return d;
    })();

    let state = readJson(K_STATE, { rev: 0, baseRevs: {} });
    let outbox = readJson(K_OUTBOX, {});
    let flushing = false;
    let pulling = false;
    let seeded = false;
    let status = 'idle';
    let detail = '';
    let pullTimer = null;
    let flushTimer = null;

    const logical = key => String(key || '').startsWith(PREFIX) ? String(key).slice(PREFIX.length) : String(key || '');
    const physical = name => name.startsWith(PREFIX) ? name : PREFIX + name;

    /* ------------------------------------------------------------ 状态指示 */
    let badge = null;
    const STATUS_TEXT = {
        idle: '同步待命', syncing: '同步中…', ok: '已同步', offline: '离线（本地已存）',
        conflict: '已用服务端版本覆盖本地', error: '同步出错', off: '同步未启用'
    };
    function paint() {
        if (!badge) return;
        const pending = Object.keys(outbox).length;
        const label = pending && status !== 'syncing' ? `${STATUS_TEXT[status] || status} · ${pending} 项待推` : (STATUS_TEXT[status] || status);
        badge.textContent = (status === 'syncing' ? '⇅ ' : status === 'ok' ? '✓ ' : '⚠ ') + label;
        badge.style.opacity = status === 'ok' && !pending ? '0.35' : '0.92';
        badge.title = detail || 'RP-Hub 同步状态 · 点击立即同步';
    }
    function setStatus(s, d = '') { status = s; detail = d; paint(); }
    function mountBadge() {
        if (badge || !document.body) return;
        badge = document.createElement('div');
        badge.id = 'rphub-sync-badge';
        badge.style.cssText = 'position:fixed;left:10px;bottom:10px;z-index:2147483000;padding:4px 9px;' +
            'border-radius:9999px;font:12px/1.5 system-ui,-apple-system,"PingFang SC",sans-serif;' +
            'background:rgba(20,20,24,.78);color:#f2f2f5;cursor:pointer;user-select:none;' +
            'backdrop-filter:blur(6px);transition:opacity .3s;opacity:.92;';
        badge.addEventListener('click', () => { flush(true); pull(true); });
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
        if (Object.keys(outbox).length >= MAX_OUTBOX && !outbox[key]) return;
        outbox[key] = { value: deleted ? null : value, deleted: !!deleted, ts: Date.now() };
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
        if (!keys.length) { setStatus('ok'); return; }
        flushing = true;
        setStatus('syncing');
        let unresolved = 0;
        try {
            for (let i = 0; i < keys.length; i += MAX_ENTRIES_PER_PUSH) {
                const slice = keys.slice(i, i + MAX_ENTRIES_PER_PUSH);
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
                    if (r.status === 'applied') {
                        delete outbox[r.key];
                        state.baseRevs[r.key] = r.rev;
                    } else if (r.status === 'conflict') {
                        // 服务端为准：丢弃本地这次改动，用服务端版本覆盖本地
                        delete outbox[r.key];
                        state.baseRevs[r.key] = r.rev;
                        await applyServerEntry(r);
                        setStatus('conflict');
                    } else {
                        unresolved++;
                    }
                }
                if (typeof out.rev === 'number') state.rev = Math.max(state.rev, out.rev);
            }
            writeJson(K_OUTBOX, outbox);
            writeJson(K_STATE, state);
            if (unresolved) setStatus('error', `${unresolved} 项服务端拒绝`);
            else setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
        } catch (e) {
            setStatus('offline', String(e && e.message || e));
        } finally {
            flushing = false;
            paint();
        }
    }

    /* ---------------------------------------------------------------- 拉取 */
    async function pull(force = false) {
        if (pulling) return;
        pulling = true;
        try {
            setStatus('syncing');
            let cursor = state.rev;
            for (let round = 0; round < 60; round++) {
                const out = await api(`/snapshot?since=${cursor}&limit=200`);
                for (const e of out.entries || []) {
                    if (String(e.key).startsWith(LEGACY_PREFIX)) continue;
                    await applyServerEntry(e);
                    state.baseRevs[e.key] = e.rev;
                    cursor = Math.max(cursor, e.rev);
                }
                state.rev = Math.max(state.rev, out.rev || 0);
                if (!out.has_more || !(out.entries || []).length) break;
            }
            writeJson(K_STATE, state);
            setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
            if (force) setStatus(Object.keys(outbox).length ? 'offline' : 'ok', '手动同步完成');
        } catch (e) {
            setStatus('offline', String(e && e.message || e));
        } finally {
            pulling = false;
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
            const slice = payload.slice(i, i + MAX_ENTRIES_PER_PUSH).map(e => ({ ...e, value: JSON.parse(JSON.stringify(e.value)) }));
            try {
                const out = await api('/push', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ device, entries: slice })
                });
                for (const r of out.results || []) {
                    if (r.status === 'applied') { state.baseRevs[r.key] = r.rev; }
                    else if (r.status === 'conflict') { await applyServerEntry(r); state.baseRevs[r.key] = r.rev; }
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
        enqueue(name, options && options.clone === false ? JSON.parse(JSON.stringify(value)) : value, false);
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
        enqueue(`${name}_${id}`, options && options.clone === false ? JSON.parse(JSON.stringify(value)) : value, false);
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
        state: () => ({ rev: state.rev, baseRevs: state.baseRevs, device, outbox: Object.keys(outbox).length }),
        pull: () => pull(true),
        flush: () => flush(true),
        seed
    };

    /* ---------------------------------------------------------------- 启动 */
    async function boot() {
        try {
            if (typeof base.initDB === 'function') await base.initDB();
            mountBadge();
            setStatus('syncing');
            const probe = await api('/health');
            if ((probe.rev || 0) === 0 && !state.rev) await seed();
            await pull();
            await flush();
            setStatus(Object.keys(outbox).length ? 'offline' : 'ok');
        } catch (e) {
            setStatus('offline', String(e && e.message || e));
        }
        if (pullTimer) clearInterval(pullTimer);
        pullTimer = setInterval(() => { if (!document.hidden) pull(); }, PULL_INTERVAL);
        document.addEventListener('visibilitychange', () => { if (!document.hidden) { flush(); pull(); } });
        window.addEventListener('online', () => { flush(); pull(); });
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
    else boot();
})();
