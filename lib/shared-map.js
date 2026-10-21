/* ══════════════════════════════════════════════════════════════════════
   SHARED MAP — a Map whose contents live in the database state (db[key])
   instead of one process's memory.

   On Vercel the app runs as several short-lived copies at once. Logins,
   OTP codes and sessions kept in a plain Map exist only in the copy that
   created them, so the next request (served by another copy) gets
   "unauthorized" and the admin panel empties. Storing them in db[key] means
   they are saved to MongoDB and picked up by every copy.

   Same API as Map for what this app uses: get, set, has, delete, entries,
   size and for…of iteration. Values must be plain JSON (objects, numbers,
   strings). Works unchanged with `node server.js` on one server.
══════════════════════════════════════════════════════════════════════ */
const registry = [];
let getDb = null, save = null;

class SharedMap {
  constructor(key) { this.key = key; registry.push(key); }
  _obj() {
    if (!getDb) throw new Error('SharedMap used before SharedMap.bind()');
    const db = getDb();
    if (!db[this.key] || typeof db[this.key] !== 'object' || Array.isArray(db[this.key])) db[this.key] = {};
    return db[this.key];
  }
  _changed() { if (save) save(this.key); }
  get(k) { return this._obj()[k]; }
  has(k) { return Object.prototype.hasOwnProperty.call(this._obj(), k); }
  set(k, v) { this._obj()[k] = v; this._changed(); return this; }
  delete(k) { const o = this._obj(); if (!(k in o)) return false; delete o[k]; this._changed(); return true; }
  get size() { return Object.keys(this._obj()).length; }
  entries() { return Object.entries(this._obj())[Symbol.iterator](); }
  keys() { return Object.keys(this._obj())[Symbol.iterator](); }
  values() { return Object.values(this._obj())[Symbol.iterator](); }
  [Symbol.iterator]() { return this.entries(); }
}

/* server.js calls this once with access to its db object and save(). */
SharedMap.bind = (dbGetter, saver) => { getDb = dbGetter; save = saver; };
SharedMap.keys = () => registry.slice();

module.exports = { SharedMap };
