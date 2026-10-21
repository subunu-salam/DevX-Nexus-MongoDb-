/* ══════════════════════════════════════════════════════════════════════
   DURABLE STORAGE

   The app keeps all state in memory (the search index depends on it) and
   persists it behind this adapter. Two drivers:

     postgres — when DATABASE_URL is set. One row per top-level key, value as
                JSONB. Survives redeploys.
     mongodb  — when MONGODB_URI is set (e.g. MongoDB Atlas free M0). Each
                collection is stored in collection `nexus_state`, split into
                parts under 8 MB so the 16 MB document limit is never hit.
     file     — a single JSON document on disk. Fine locally, but on Render's
                free tier the filesystem is EPHEMERAL: every redeploy and the
                daily container recycle wipe it, taking orders, issued coupons
                and floor-plan edits with them. That is the reason this exists.

   Why a key/value table rather than real relational tables: the server already
   reads `db['devx-orders']` in fifty-odd places. Modelling each collection as
   a row keeps every one of those working untouched, so durability lands
   without a rewrite. Proper tables can come later, per collection, without
   changing the call sites again.

   Writes are debounced and dirty-tracked. The catalogue is ~30 MB at
   hypermarket scale, so it is deliberately excluded from the default save —
   an order must not rewrite the entire product list.
══════════════════════════════════════════════════════════════════════ */
const fs = require('fs');
const crypto = require('crypto');
const fingerprint = v => crypto.createHash('sha1').update(JSON.stringify(v ?? null)).digest('base64');
const path = require('path');

const CATALOG_KEY = 'devx-catalog';

class Store {
  constructor(keys, { dataDir, url } = {}) {
    this.keys = keys;
    this.dataDir = dataDir;
    this.file = path.join(dataDir, 'db.json');
    this.url = url || process.env.DATABASE_URL || '';
    /* SQL Server (MSSQL) — used when MSSQL_SERVER is set. Takes priority over
       DATABASE_URL so a store's own SQL Server is the source of truth. */
    this.mssqlCfg = process.env.MSSQL_SERVER ? {
      server: process.env.MSSQL_SERVER,
      port: parseInt(process.env.MSSQL_PORT, 10) || 1433,
      user: process.env.MSSQL_USER || 'sa',
      password: process.env.MSSQL_PASSWORD || '',
      database: process.env.MSSQL_DATABASE || 'DevXNexus',
      options: {
        encrypt: process.env.MSSQL_ENCRYPT === '1',
        trustServerCertificate: process.env.MSSQL_TRUST_CERT !== '0',
        enableArithAbort: true
      },
      pool: { max: 4, min: 0, idleTimeoutMillis: 30000 },
      connectionTimeout: 15000,
      requestTimeout: 30000
    } : null;
    /* MongoDB — used when MONGODB_URI is set and SQL Server is not. */
    this.mongoUri = process.env.MONGODB_URI || '';
    this.mongoDbName = process.env.MONGODB_DB || 'devxnexus';
    this.driver = this.mssqlCfg ? 'mssql' : (this.mongoUri ? 'mongodb' : (this.url ? 'postgres' : 'file'));
    this.mongo = null;
    this.mcol = null;
    this.mmeta = null;
    this.vers = {};      // k -> ver this instance last loaded or wrote (mongodb)
    this.prints = {};    // k -> fingerprint of what is in the database, to skip unchanged writes
    this.sql = null;
    this.pool = null;
    this.dirty = new Set();
    this.timer = null;
    this.data = null;
    this.writes = 0;
    this.lastError = null;
  }

  async init(seed) {
    this.data = seed;
    if (this.driver === 'mssql') {
      try { this.sql = require('mssql'); }
      catch (e) {
        console.error('[store] MSSQL_SERVER is set but the "mssql" package is missing — run: npm install');
        this.driver = this.url ? 'postgres' : 'file';
        this.lastError = 'mssql package missing';
      }
    }
    if (this.driver === 'mssql') {
      const cfg = this.mssqlCfg, db = cfg.database;
      if (!/^[A-Za-z0-9_]+$/.test(db)) throw new Error('MSSQL_DATABASE may only contain letters, digits and _');
      try {
        /* Create the database on first run, via master. */
        const boot = await new this.sql.ConnectionPool({ ...cfg, database: 'master' }).connect();
        await boot.request().query(`IF DB_ID(N'${db}') IS NULL CREATE DATABASE [${db}]`);
        await boot.close();

        this.pool = await new this.sql.ConnectionPool(cfg).connect();
        await this.pool.request().query(`IF OBJECT_ID(N'dbo.nexus_state', N'U') IS NULL
          CREATE TABLE dbo.nexus_state (
            k NVARCHAR(200) NOT NULL PRIMARY KEY,
            v NVARCHAR(MAX) NOT NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
          )`);
        const { recordset } = await this.pool.request().query('SELECT k, v FROM dbo.nexus_state');
        for (const r of recordset)
          if (this.keys.includes(r.k) || r.k === 'devx-order-count') {
            try { this.data[r.k] = JSON.parse(r.v); } catch (e) { console.error('[store] bad JSON in', r.k); }
          }
        console.log(`[store] mssql · ${cfg.server}/${db} · loaded ${recordset.length} collections`);
        return this.data;
      } catch (e) {
        console.error('[store] SQL Server unavailable, falling back to file:', e.message);
        this.lastError = e.message;
        this.driver = 'file';
        if (this.pool) { try { await this.pool.close(); } catch (_) {} this.pool = null; }
        return this._loadFile();
      }
    }
    if (this.driver === 'mongodb') {
      let MongoClient;
      try { ({ MongoClient } = require('mongodb')); }
      catch (e) {
        console.error('[store] MONGODB_URI is set but the "mongodb" package is missing — run: npm install');
        this.lastError = 'mongodb package missing';
        this.driver = this.url ? 'postgres' : 'file';
      }
      if (MongoClient) {
        try {
          // Some networks/Windows resolvers refuse the SRV lookup mongodb+srv needs
          if (this.mongoUri.startsWith('mongodb+srv://')) {
            try { require('dns').setServers(['8.8.8.8', '1.1.1.1']); } catch (_) {}
          }
          this.mongo = new MongoClient(this.mongoUri, { maxPoolSize: 4, serverSelectionTimeoutMS: 15000 });
          await this.mongo.connect();
          this.mcol = this.mongo.db(this.mongoDbName).collection('nexus_state');
          this.mmeta = this.mongo.db(this.mongoDbName).collection('nexus_meta');
          await this.mcol.createIndex({ k: 1, ver: 1, part: 1 });
          const docs = await this.mcol.find({}).sort({ k: 1, ver: -1, part: 1 }).toArray();
          const loaded = this._mongoAssemble(docs, this.vers);
          for (const [k, v] of Object.entries(loaded))
            if (this.keys.includes(k) || k === 'devx-order-count') { this.data[k] = v; this.prints[k] = fingerprint(v); }
          console.log(`[store] mongodb · ${this.mongoDbName} · loaded ${Object.keys(loaded).length} collections`);
          return this.data;
        } catch (e) {
          console.error('[store] MongoDB unavailable, falling back to file:', e.message);
          this.lastError = e.message;
          this.driver = 'file';
          if (this.mongo) { try { await this.mongo.close(); } catch (_) {} this.mongo = null; }
          return this._loadFile();
        }
      }
    }
    if (this.driver === 'postgres') {
      let Pool;
      try { ({ Pool } = require('pg')); }
      catch (e) {
        console.error('[store] DATABASE_URL is set but the "pg" package is missing — run: npm install pg');
        this.driver = 'file';
        return this._loadFile();
      }
      this.pool = new Pool({
        ssl: { rejectUnauthorized: false },
        connectionString: this.url,
        // Render's managed Postgres requires TLS but presents a chain Node
        // does not ship a root for; this is the documented connection style.
        ssl: /localhost|127\.0\.0\.1/.test(this.url) ? false : { rejectUnauthorized: false },
        max: 4, idleTimeoutMillis: 30000
      });
      try {
        await this.pool.query(`CREATE TABLE IF NOT EXISTS nexus_state (
          k TEXT PRIMARY KEY,
          v JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`);
        const { rows } = await this.pool.query('SELECT k, v FROM nexus_state');
        for (const r of rows) if (this.keys.includes(r.k) || r.k === 'devx-order-count') this.data[r.k] = r.v;
        console.log(`[store] postgres · loaded ${rows.length} collections`);
        return this.data;
      } catch (e) {
        // A database that is unreachable at boot must not take the shop down.
        console.error('[store] postgres unavailable, falling back to file:', e.message);
        this.lastError = e.message;
        this.driver = 'file';
        return this._loadFile();
      }
    }
    return this._loadFile();
  }

  /* ── MongoDB helpers ──
     A value is written as one or more documents { k, ver, part, parts, kind, items }.
       kind 'array'  — items is a slice of the array
       kind 'object' — items is a slice of Object.entries(value)
       kind 'value'  — items is [value] (numbers, strings, small things)
       kind 'json'   — items is [a piece of the JSON text]; used only when one
                       single element is itself too big for a document
     Every write gets a new `ver`; old versions are deleted only after the new
     one is fully written, so a crash mid-write never leaves half a collection. */
  _mongoParts(value) {
    const LIMIT = 8 * 1024 * 1024;
    const pack = (list, kind) => {
      const parts = []; let cur = [], size = 0;
      for (const it of list) {
        const n = Buffer.byteLength(JSON.stringify(it ?? null));
        if (n > LIMIT) return null;
        if (cur.length && size + n > LIMIT) { parts.push(cur); cur = []; size = 0; }
        cur.push(it); size += n;
      }
      parts.push(cur);
      return parts.map(items => ({ kind, items }));
    };
    let parts = null;
    if (Array.isArray(value)) parts = pack(value, 'array');
    else if (value && typeof value === 'object') parts = pack(Object.entries(value), 'object');
    else if (Buffer.byteLength(JSON.stringify(value ?? null)) <= LIMIT) parts = [{ kind: 'value', items: [value ?? null] }];
    if (parts) return parts;
    const text = JSON.stringify(value ?? null), out = [];
    for (let i = 0; i < text.length; i += 4 * 1024 * 1024) out.push({ kind: 'json', items: [text.slice(i, i + 4 * 1024 * 1024)] });
    return out;
  }

  _mongoAssemble(docs, versOut) {
    const byKey = {};
    for (const d of docs) ((byKey[d.k] ||= {})[d.ver] ||= []).push(d);
    const out = {};
    for (const [k, vers] of Object.entries(byKey)) {
      // newest complete version wins; an incomplete one is a crashed write
      const best = Object.values(vers)
        .filter(ps => ps.length === ps[0].parts)
        .sort((a, b) => b[0].ver - a[0].ver)[0];
      if (!best) { console.error('[store] no complete copy of', k); continue; }
      best.sort((a, b) => a.part - b.part);
      if (versOut) versOut[k] = best[0].ver;
      const kind = best[0].kind, all = best.flatMap(p => p.items);
      try {
        if (kind === 'array') out[k] = all;
        else if (kind === 'object') out[k] = Object.fromEntries(all);
        else if (kind === 'json') out[k] = JSON.parse(all.join(''));
        else out[k] = all[0];
      } catch (e) { console.error('[store] bad data in', k, e.message); }
    }
    return out;
  }

  async _mongoLatestVers(onlyKey) {
    const match = onlyKey ? [{ $match: { k: onlyKey } }] : [];
    const rows = await this.mcol.aggregate([...match, { $group: { _id: '$k', ver: { $max: '$ver' } } }]).toArray();
    return Object.fromEntries(rows.map(r => [r._id, r.ver]));
  }

  async _mongoLoadKey(k) {
    const docs = await this.mcol.find({ k }).sort({ ver: -1, part: 1 }).toArray();
    const vers = {};
    const out = this._mongoAssemble(docs, vers);
    return { has: k in out, value: out[k], ver: vers[k] };
  }

  /* Another running copy wrote k after we loaded it. Combine instead of
     overwriting: lists of records are merged by id (ours win on the same id),
     key/value objects are merged key by key, counters keep the higher value. */
  _merge(k, ours, theirs) {
    if (k === 'devx-order-count') return Math.max(Number(ours) || 0, Number(theirs) || 0);
    if (Array.isArray(ours) && Array.isArray(theirs)) {
      const idOf = x => (x && typeof x === 'object' && (x.id ?? x._id ?? x.code ?? x.phone)) ?? null;
      if (!ours.every(x => idOf(x) != null) || !theirs.every(x => idOf(x) != null)) return ours;
      const seen = new Set(ours.map(idOf));
      const extra = theirs.filter(x => !seen.has(idOf(x)));
      if (!extra.length) return ours;
      const merged = ours.concat(extra);
      const t = x => Date.parse((x && (x.createdAt || x.at || x.created || x.date)) || '') || 0;
      if (merged.some(x => t(x))) merged.sort((a, b) => t(b) - t(a));   // keep newest-first
      return merged;
    }
    if (ours && theirs && typeof ours === 'object' && typeof theirs === 'object' && !Array.isArray(ours))
      return Object.assign({}, theirs, ours);
    return ours;
  }

  async _mongoWrite(k, latestKnown) {
    const known = this.vers[k] || 0;
    const latest = latestKnown !== undefined ? latestKnown : ((await this._mongoLatestVers(k))[k] || 0);
    if (latest > known) {
      const theirs = await this._mongoLoadKey(k);
      if (theirs.has) this.data[k] = this._merge(k, this.data[k], theirs.value);
    }
    const ver = Date.now() * 1000 + Math.floor(Math.random() * 1000);
    const parts = this._mongoParts(this.data[k]);
    const at = new Date();
    await this.mcol.insertMany(parts.map((p, i) =>
      ({ k, ver, part: i, parts: parts.length, kind: p.kind, items: p.items, updated_at: at })), { ordered: false });
    await this.mcol.deleteMany({ k, ver: { $lt: ver } });
    this.vers[k] = ver;
    this.prints[k] = fingerprint(this.data[k]);
  }

  /* Pull in collections other running copies (e.g. Vercel instances) have
     written since this one loaded them. Keys with unsaved local changes are
     skipped — their write merges instead. Returns the keys that changed. */
  async refresh() {
    if (this.driver !== 'mongodb' || !this.mcol) return [];
    const latest = await this._mongoLatestVers();
    const changed = [];
    for (const [k, ver] of Object.entries(latest)) {
      if (!(this.keys.includes(k) || k === 'devx-order-count')) continue;
      if (this.dirty.has(k) || ver <= (this.vers[k] || 0)) continue;
      const r = await this._mongoLoadKey(k);
      if (r.has) { this.data[k] = r.value; this.vers[k] = r.ver; this.prints[k] = fingerprint(r.value); changed.push(k); }
    }
    return changed;
  }

  /* Next order number. With MongoDB this is one atomic counter shared by every
     running copy, so two orders can never get the same number. */
  async nextOrderNumber(current) {
    if (this.driver === 'mongodb' && this.mmeta) {
      const r = await this.mmeta.findOneAndUpdate(
        { _id: 'order_seq' },
        [{ $set: { n: { $add: [{ $max: [{ $ifNull: ['$n', 0] }, Number(current) || 0] }, 1] } } }],
        { upsert: true, returnDocument: 'after' });
      const doc = r && (r.value !== undefined ? r.value : r);
      if (doc && doc.n) return doc.n;
    }
    return (Number(current) || 0) + 1;
  }

  /* Write everything pending right now (serverless: before the reply is sent). */
  async flushNow() {
    clearTimeout(this.timer);
    await this.flush();
  }

  _loadFile() {
    try {
      if (fs.existsSync(this.file)) {
        Object.assign(this.data, JSON.parse(fs.readFileSync(this.file, 'utf8')));
        console.log('[store] file · loaded from', this.file);
      }
    } catch (e) { console.error('[store] file load failed:', e.message); }
    return this.data;
  }

  /* Mark keys for writing. No arguments means "everything that changes during
     normal trading" — which excludes the catalogue, because rewriting 30 MB of
     products every time someone places an order is what makes a store feel
     slow. Pass the catalogue key explicitly when it really has changed. */
  save(...keys) {
    if (!keys.length) keys = this.keys.filter(k => k !== CATALOG_KEY).concat('devx-order-count');
    for (const k of keys) this.dirty.add(k);
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.flush().catch(() => {}), 400);
  }
  saveAll() { return this.save(...this.keys, 'devx-order-count'); }

  async flush() {
    if (!this.dirty.size) return;
    const keys = [...this.dirty];
    this.dirty.clear();
    try {
      if (this.driver === 'mssql') {
        // all dirty keys in one transaction — either every collection lands or none
        const tx = new this.sql.Transaction(this.pool);
        await tx.begin();
        try {
          for (const k of keys) {
            await new this.sql.Request(tx)
              .input('k', this.sql.NVarChar(200), k)
              .input('v', this.sql.NVarChar(this.sql.MAX), JSON.stringify(this.data[k] ?? null))
              .query(`MERGE dbo.nexus_state WITH (HOLDLOCK) AS t
                      USING (SELECT @k AS k, @v AS v) AS s ON t.k = s.k
                      WHEN MATCHED THEN UPDATE SET v = s.v, updated_at = SYSUTCDATETIME()
                      WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v);`);
          }
          await tx.commit();
        } catch (e) { try { await tx.rollback(); } catch (_) {} throw e; }
      } else if (this.driver === 'mongodb') {
        // only collections whose content really changed; one version check; writes in parallel
        const todo = keys.filter(k => fingerprint(this.data[k]) !== this.prints[k]);
        if (todo.length) {
          const latest = await this._mongoLatestVers();
          await Promise.all(todo.map(k => this._mongoWrite(k, latest[k] || 0)));
        }
      } else if (this.driver === 'postgres') {
        // one statement, all dirty keys — atomic and avoids a round trip each
        const vals = keys.map((k, i) => `($${i * 2 + 1}, $${i * 2 + 2}::jsonb)`).join(',');
        const params = keys.flatMap(k => [k, JSON.stringify(this.data[k] ?? null)]);
        await this.pool.query(
          `INSERT INTO nexus_state (k, v) VALUES ${vals}
           ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = now()`, params);
      } else {
        fs.mkdirSync(this.dataDir, { recursive: true });
        const tmp = this.file + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(this.data));
        fs.renameSync(tmp, this.file);      // atomic swap, never a half file
      }
      this.writes++;
      this.lastError = null;
    } catch (e) {
      // put them back so the next save retries rather than losing the write
      for (const k of keys) this.dirty.add(k);
      this.lastError = e.message;
      console.error('[store] write failed:', e.message);
    }
  }

  /* Flush synchronously on shutdown so an in-flight order is never lost to a
     deploy. Render sends SIGTERM and waits before killing the container. */
  async close() {
    clearTimeout(this.timer);
    await this.flush();
    if (this.mongo) await this.mongo.close().catch(() => {});
    if (this.pool) await (this.driver === 'mssql' || this.sql ? this.pool.close() : this.pool.end()).catch(() => {});
  }

  status() {
    return {
      driver: this.driver,
      durable: this.driver === 'postgres' || this.driver === 'mssql' || this.driver === 'mongodb',
      writes: this.writes,
      pending: this.dirty.size,
      lastError: this.lastError
    };
  }
}

module.exports = { Store, CATALOG_KEY };
