'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const zlib = require('zlib');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const initSqlJs = require('sql.js');
const cookieParser = require('cookie-parser');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const multer = require('multer');
const { Server } = require('socket.io');

async function main() {
const ROOT = __dirname;
let env = {};
// Read local .env only for development; Render environment variables take precedence.
try { for (const line of fs.readFileSync(path.join(ROOT,'.env'),'utf8').split(/\r?\n/)) { const m=line.match(/^\s*([A-Za-z_]+)\s*=\s*(.*)\s*$/); if(m) env[m[1]]=m[2].replace(/^['"]|['"]$/g,''); } } catch {}
const cfg = k => process.env[k] || env[k];
const PORT = Number(cfg('PORT') || 1074);
if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) throw new Error('PORT must be an integer from 1 to 65535');
const dataDir = path.dirname(path.resolve(ROOT, cfg('DATABASE_PATH') || path.join(ROOT,'data','dordor.sqlite')));
const dbFile = path.resolve(ROOT, cfg('DATABASE_PATH') || path.join(ROOT,'data','dordor.sqlite'));
const uploadsDir = path.resolve(ROOT, cfg('UPLOADS_DIR') || path.join(ROOT,'uploads'));
fs.mkdirSync(dataDir, {recursive:true}); fs.mkdirSync(uploadsDir, {recursive:true});
// File-backed session store: preserves login sessions across process restarts/deploys when dataDir is on persistent storage.
class DurableSessionStore extends session.Store {
  constructor(file) { super(); this.file=file; this.items={}; try { this.items=JSON.parse(fs.readFileSync(file,'utf8'))||{}; } catch {} }
  _save(){ try { const tmp=this.file+'.tmp'; fs.writeFileSync(tmp,JSON.stringify(this.items),{mode:0o600}); fs.renameSync(tmp,this.file); } catch(e){ console.error('Session store save failed:',e.message); } }
  get(sid,cb){ const row=this.items[sid]; if(!row){cb(null,null);return;} if(row.cookie?.expires && Date.parse(row.cookie.expires)<=Date.now()){delete this.items[sid];this._save();cb(null,null);return;} cb(null,JSON.parse(JSON.stringify(row))); }
  set(sid,sess,cb){this.items[sid]=JSON.parse(JSON.stringify(sess));this._save();cb&&cb(null);}
  destroy(sid,cb){delete this.items[sid];this._save();cb&&cb(null);}
  touch(sid,sess,cb){if(this.items[sid]){this.items[sid].cookie=sess.cookie;this._save();}cb&&cb(null);}
  clearExpired(){let changed=false;for(const [id,row] of Object.entries(this.items)){if(row.cookie?.expires&&Date.parse(row.cookie.expires)<=Date.now()){delete this.items[id];changed=true;}}if(changed)this._save();}
}
const sessionStore = new DurableSessionStore(path.join(dataDir,'sessions.json'));
setInterval(()=>sessionStore.clearExpired(),60*60*1000).unref();
if (process.env.NODE_ENV === 'production') {
  const secret = cfg('SESSION_SECRET');
  if (!secret || secret.length < 32 || /replace|example|sample|placeholder|change-me|your-secret|random-secret/i.test(secret)) {
    console.error('FATAL: Set a unique SESSION_SECRET (at least 32 characters) in the production environment.'); process.exit(1);
  }
}
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 8e6 });
const SQL = await initSqlJs();
let rawDb;
try { rawDb = fs.existsSync(dbFile) ? new SQL.Database(new Uint8Array(fs.readFileSync(dbFile))) : new SQL.Database(); }
catch (e) { console.error('Could not open database file; starting a new database:', e.message); rawDb = new SQL.Database(); }
const persistDb = () => {
  const bytes = rawDb.export();
  fs.mkdirSync(path.dirname(dbFile), {recursive:true});
  const tmp = dbFile + '.tmp';
  fs.writeFileSync(tmp, Buffer.from(bytes), {mode:0o600});
  fs.renameSync(tmp, dbFile);
};
const backupDir = path.join(path.dirname(dbFile), 'backups');
const createSafetyBackup = (label='snapshot') => {
  try {
    fs.mkdirSync(backupDir, {recursive:true});
    const safeLabel = String(label).replace(/[^a-z0-9_-]+/gi,'-').replace(/^-+|-+$/g,'') || 'snapshot';
    const stamp = new Date().toISOString().replace(/[:.]/g,'-');
    const target = path.join(backupDir, `dordor-${safeLabel}-${stamp}.sqlite`);
    fs.writeFileSync(target, Buffer.from(rawDb.export()), {mode:0o600, flag:'wx'});
    return target;
  } catch (e) {
    console.error('Safety backup failed:', e.message);
    return null;
  }
};
const normalizeParams = args => args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
const postsTableBeforeMigration = rawDb.exec("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'").length > 0;
if (!postsTableBeforeMigration && fs.existsSync(dbFile)) { const backup = createSafetyBackup('pre-posts-migration'); if (!backup) throw new Error('برای ایمنی، پشتیبان دیتابیس قبل از به‌روزرسانی ساخته نشد؛ سرور بدون تغییر دیتابیس متوقف شد.'); console.log('Pre-posts migration database backup created.'); }
const db = {
  exec(sql) { rawDb.exec(sql); persistDb(); },
  pragma(_sql) {},
  prepare(sql) {
    return {
      get(...args) {
        const stmt = rawDb.prepare(sql);
        try { const params = normalizeParams(args); if (params.length) stmt.bind(params); return stmt.step() ? stmt.getAsObject() : undefined; }
        finally { stmt.free(); }
      },
      all(...args) {
        const stmt = rawDb.prepare(sql), rows = [];
        try { const params = normalizeParams(args); if (params.length) stmt.bind(params); while (stmt.step()) rows.push(stmt.getAsObject()); return rows; }
        finally { stmt.free(); }
      },
      run(...args) {
        const params = normalizeParams(args); rawDb.run(sql, params);
        const changes = rawDb.getRowsModified();
        let lastInsertRowid = 0;
        try { const result = rawDb.exec('SELECT last_insert_rowid() AS id'); if (result[0]?.values?.[0]) lastInsertRowid = Number(result[0].values[0][0]) || 0; } catch {}
        persistDb();
        return { changes, lastInsertRowid };
      }
    };
  }
};
rawDb.run('PRAGMA foreign_keys = ON');
try { rawDb.run('ALTER TABLE users ADD COLUMN terms_accepted_at TEXT'); } catch {}

db.exec(`
CREATE TABLE IF NOT EXISTS users(
 id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, display_name TEXT NOT NULL,
 phone TEXT UNIQUE, password_hash TEXT NOT NULL, bio TEXT DEFAULT '', gender TEXT DEFAULT 'نامشخص',
 age INTEGER DEFAULT 18, province TEXT DEFAULT '', avatar TEXT DEFAULT '', coins REAL DEFAULT 100, diamonds INTEGER DEFAULT 0,
 xp INTEGER DEFAULT 0, level INTEGER DEFAULT 1, fame INTEGER DEFAULT 0, role TEXT DEFAULT 'user',
 is_banned INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, last_seen TEXT DEFAULT CURRENT_TIMESTAMP, terms_accepted_at TEXT
);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS activity_log(id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id INTEGER, target_user_id INTEGER, action TEXT NOT NULL, details TEXT DEFAULT '', actor_role TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP);\nCREATE TABLE IF NOT EXISTS user_permissions(user_id INTEGER NOT NULL, permission TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(user_id,permission));
CREATE TABLE IF NOT EXISTS admin_titles(user_id INTEGER PRIMARY KEY, title TEXT NOT NULL DEFAULT 'پشتیبانی', updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS pro_usage(user_id INTEGER NOT NULL, window_key TEXT NOT NULL, window_start TEXT NOT NULL, used INTEGER DEFAULT 0, PRIMARY KEY(user_id,window_key));
CREATE TABLE IF NOT EXISTS follows(id INTEGER PRIMARY KEY AUTOINCREMENT, follower_id INTEGER NOT NULL, followed_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(follower_id,followed_id));
CREATE TABLE IF NOT EXISTS likes(id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(from_id,to_id));
CREATE TABLE IF NOT EXISTS conversations(id INTEGER PRIMARY KEY AUTOINCREMENT, user_a INTEGER NOT NULL, user_b INTEGER NOT NULL, pinned_a INTEGER DEFAULT 0, pinned_b INTEGER DEFAULT 0, hidden_a INTEGER DEFAULT 0, hidden_b INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT, closed_by INTEGER, UNIQUE(user_a,user_b));
CREATE TABLE IF NOT EXISTS group_message_reads(group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,last_read_message_id INTEGER NOT NULL DEFAULT 0,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(group_id,user_id));
CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id INTEGER NOT NULL, sender_id INTEGER NOT NULL, type TEXT DEFAULT 'text', body TEXT DEFAULT '', media_url TEXT DEFAULT '', deleted INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS notifications(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, title TEXT NOT NULL, body TEXT DEFAULT '', read_at TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS user_notification_settings(user_id INTEGER PRIMARY KEY, preferences_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, amount REAL NOT NULL, kind TEXT NOT NULL, note TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS tickets(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, subject TEXT NOT NULL, status TEXT DEFAULT 'open', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS ticket_messages(id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER NOT NULL, sender_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS random_queue(user_id INTEGER PRIMARY KEY, mode TEXT DEFAULT 'random', gender_pref TEXT DEFAULT 'any', age_min INTEGER DEFAULT 18, age_max INTEGER DEFAULT 99, province TEXT DEFAULT '', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS random_blocks(user_id INTEGER NOT NULL, blocked_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id,blocked_id));
CREATE TABLE IF NOT EXISTS random_reports(id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_id INTEGER NOT NULL, reported_id INTEGER NOT NULL, room_id TEXT DEFAULT '', reason TEXT DEFAULT 'گزارش از چت ناشناس', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS reports(id INTEGER PRIMARY KEY AUTOINCREMENT, reporter_id INTEGER NOT NULL, reported_user_id INTEGER, category TEXT NOT NULL, target_type TEXT NOT NULL, target_id TEXT NOT NULL, reason TEXT NOT NULL, details TEXT DEFAULT '', status TEXT NOT NULL DEFAULT 'new', reviewed_by INTEGER, reviewed_at TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);

CREATE TABLE IF NOT EXISTS chat_groups(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, group_code TEXT UNIQUE NOT NULL, description TEXT DEFAULT '', avatar TEXT DEFAULT '', owner_id INTEGER NOT NULL, coins INTEGER NOT NULL DEFAULT 0, fame INTEGER NOT NULL DEFAULT 0, muted INTEGER NOT NULL DEFAULT 0, access_mode TEXT NOT NULL DEFAULT 'open', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS chat_group_members(group_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'member', muted INTEGER NOT NULL DEFAULT 0, joined_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(group_id,user_id));
CREATE TABLE IF NOT EXISTS chat_group_messages(id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL, sender_id INTEGER NOT NULL, body TEXT DEFAULT '', type TEXT NOT NULL DEFAULT 'text', media_url TEXT DEFAULT '', deleted INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS chat_group_reports(id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL, reporter_id INTEGER NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'new', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS chat_group_invites(id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(group_id,to_id));
CREATE TABLE IF NOT EXISTS group_reward_claims(id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL, user_id INTEGER NOT NULL, reward_date TEXT NOT NULL DEFAULT (date('now')), coins INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(group_id,user_id,reward_date));

CREATE TABLE IF NOT EXISTS wheel_claims(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, prize TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS friend_requests(id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, status TEXT DEFAULT 'pending', created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(from_id,to_id));
CREATE TABLE IF NOT EXISTS message_requests(id INTEGER PRIMARY KEY AUTOINCREMENT, from_id INTEGER NOT NULL, to_id INTEGER NOT NULL, status TEXT DEFAULT 'pending', created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(from_id,to_id));
CREATE TABLE IF NOT EXISTS blocks(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, blocked_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id,blocked_id));
CREATE TABLE IF NOT EXISTS profile_views(id INTEGER PRIMARY KEY AUTOINCREMENT, viewer_id INTEGER NOT NULL, viewed_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS game_rooms(id INTEGER PRIMARY KEY AUTOINCREMENT, game_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'waiting', created_by INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, started_at TEXT, finished_at TEXT, winner_id INTEGER, deadline INTEGER NOT NULL, state_json TEXT NOT NULL DEFAULT '{}', room_code TEXT);
CREATE TABLE IF NOT EXISTS game_players(room_id INTEGER NOT NULL, user_id INTEGER NOT NULL, position INTEGER NOT NULL DEFAULT 0, choice TEXT, roll INTEGER, joined_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(room_id,user_id));
CREATE TABLE IF NOT EXISTS game_results(id INTEGER PRIMARY KEY AUTOINCREMENT, room_id INTEGER NOT NULL, game_type TEXT NOT NULL, user_id INTEGER NOT NULL, result TEXT NOT NULL, fame_delta INTEGER NOT NULL DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS game_messages(id INTEGER PRIMARY KEY AUTOINCREMENT,room_id INTEGER NOT NULL,user_id INTEGER NOT NULL,body TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS game_admin_actions(id INTEGER PRIMARY KEY AUTOINCREMENT,room_id INTEGER NOT NULL,admin_id INTEGER NOT NULL,action TEXT NOT NULL,reason TEXT DEFAULT '',created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_game_messages_room ON game_messages(room_id,id);
CREATE TABLE IF NOT EXISTS group_promotion_views(group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(group_id,user_id));
CREATE INDEX IF NOT EXISTS idx_group_promotion_views_group ON group_promotion_views(group_id);
CREATE TABLE IF NOT EXISTS group_promotion_campaign_views(group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,campaign_id TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(group_id,user_id,campaign_id));
CREATE INDEX IF NOT EXISTS idx_group_promotion_campaign_views ON group_promotion_campaign_views(group_id,campaign_id);
CREATE TABLE IF NOT EXISTS game_reward_claims(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, period_key TEXT NOT NULL, place INTEGER NOT NULL, reward_type TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id,period_key));
CREATE TABLE IF NOT EXISTS level_rewards(user_id INTEGER NOT NULL, level INTEGER NOT NULL, diamonds INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id,level));
CREATE TABLE IF NOT EXISTS daily_presence_rewards(user_id INTEGER PRIMARY KEY, claimed_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS challenge_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,challenge_id TEXT NOT NULL,period_key TEXT NOT NULL,reward_coins INTEGER NOT NULL DEFAULT 0,reward_fame INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(user_id,challenge_id,period_key));
CREATE TABLE IF NOT EXISTS user_login_days(user_id INTEGER NOT NULL,day_key TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,day_key));
CREATE TABLE IF NOT EXISTS referrals(inviter_id INTEGER NOT NULL,invitee_id INTEGER NOT NULL UNIQUE,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(inviter_id,invitee_id));
CREATE TABLE IF NOT EXISTS daily_ranking_distributions(period_key TEXT PRIMARY KEY,distributed_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS fame_events(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,amount INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_fame_events_user_date ON fame_events(user_id,created_at);
CREATE TABLE IF NOT EXISTS daily_activity_diamond_distributions(period_key TEXT NOT NULL,user_id INTEGER NOT NULL,diamonds INTEGER NOT NULL,distributed_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(period_key,user_id));
CREATE TABLE IF NOT EXISTS daily_activity_coin_distributions(period_key TEXT NOT NULL,user_id INTEGER NOT NULL,coins INTEGER NOT NULL,distributed_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(period_key,user_id));
CREATE TABLE IF NOT EXISTS custom_challenges(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,category TEXT NOT NULL DEFAULT 'چالش اختصاصی مدیر',metric TEXT NOT NULL,target INTEGER NOT NULL,scope TEXT NOT NULL DEFAULT 'permanent',reward_coins INTEGER NOT NULL DEFAULT 0,reward_fame INTEGER NOT NULL DEFAULT 0,starts_at TEXT,ends_at TEXT,active INTEGER NOT NULL DEFAULT 1,created_by INTEGER,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS posts(
 id INTEGER PRIMARY KEY AUTOINCREMENT, post_code TEXT UNIQUE NOT NULL, user_id INTEGER NOT NULL,
 body TEXT NOT NULL DEFAULT '', media_url TEXT DEFAULT '', media_type TEXT DEFAULT '',
 allow_comments INTEGER NOT NULL DEFAULT 1, allow_download INTEGER NOT NULL DEFAULT 1,
 promoted INTEGER NOT NULL DEFAULT 0, reward_enabled INTEGER NOT NULL DEFAULT 0, reward_budget INTEGER NOT NULL DEFAULT 0,
 reward_winners INTEGER NOT NULL DEFAULT 0, reward_distributed INTEGER NOT NULL DEFAULT 0,
 display_limit INTEGER NOT NULL DEFAULT 50, support_completed INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'active', created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS post_views(id INTEGER PRIMARY KEY AUTOINCREMENT,post_id INTEGER NOT NULL,viewer_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(post_id,viewer_id));
CREATE TABLE IF NOT EXISTS post_likes(id INTEGER PRIMARY KEY AUTOINCREMENT,post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(post_id,user_id));
CREATE TABLE IF NOT EXISTS user_restrictions(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,restriction_type TEXT NOT NULL,starts_at TEXT NOT NULL,ends_at TEXT NOT NULL,reason TEXT NOT NULL,created_by INTEGER NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_user_restrictions_active ON user_restrictions(user_id,active,ends_at);
CREATE TABLE IF NOT EXISTS post_comments(id INTEGER PRIMARY KEY AUTOINCREMENT,post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,body TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'active',created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS post_reward_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,amount INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(post_id,user_id));
CREATE INDEX IF NOT EXISTS idx_posts_explore ON posts(status,promoted,reward_enabled,created_at);
CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id,created_at);
CREATE INDEX IF NOT EXISTS idx_post_comments ON post_comments(post_id,status,created_at);

`);
try { rawDb.run("ALTER TABLE chat_groups ADD COLUMN access_mode TEXT NOT NULL DEFAULT 'open'"); } catch {}
db.exec(`
CREATE TABLE IF NOT EXISTS group_join_requests(
 id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
 reviewed_at TEXT, reviewed_by INTEGER, UNIQUE(group_id,user_id)
);
CREATE INDEX IF NOT EXISTS idx_group_join_requests_pending ON group_join_requests(group_id,status,created_at);
CREATE TABLE IF NOT EXISTS group_user_settings(
 group_id INTEGER NOT NULL, user_id INTEGER NOT NULL, muted INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(group_id,user_id)
);
CREATE TABLE IF NOT EXISTS app_releases(
 id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT NOT NULL, filename TEXT NOT NULL UNIQUE,
 original_name TEXT NOT NULL, size_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL,
 uploaded_by INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
`);
try { rawDb.run("ALTER TABLE game_rooms ADD COLUMN game_code TEXT"); } catch {}
try { rawDb.run("ALTER TABLE game_rooms ADD COLUMN admin_close_reason TEXT DEFAULT ''"); } catch {}
try { rawDb.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_game_rooms_game_code ON game_rooms(game_code) WHERE game_code IS NOT NULL"); } catch {}
try { rawDb.run("UPDATE game_rooms SET game_code='GAME-' || printf('%08d',id) WHERE game_code IS NULL OR game_code=''"); } catch {}
const defaults = {signup_coins:'500', referral_coins:'50', daily_wheel:'1', wheel_cost:'0', pro_cost:'750', boost_cost:'250', unlock_cost:'10', diamond_coin_rate:'10', level_diamond_reward:'10', coin_pack_1_coins:'1000', coin_pack_1_diamonds:'10', coin_pack_2_coins:'5000', coin_pack_2_diamonds:'40', coin_pack_3_coins:'12000', coin_pack_3_diamonds:'90', coin_pack_4_coins:'30000', coin_pack_4_diamonds:'200', diamond_transfer_min:'20', direct_cost:'5', message_request_cost:'10', pro_view_quota:'50', pro_chat_requests:'2', pro_quota_8h:'40', pro_quota_24h:'100', pro_quota_7d:'400', pro_cost_enabled:'1', boost_enabled:'1', app_name:'ایس کلاب', game_daily_reward_1:'1000', game_daily_reward_2:'500', game_daily_reward_3:'250', game_win_fame:'250', game_loss_fame:'30', maintenance:'0'};
for(const [k,v] of Object.entries(defaults)) db.prepare('INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)').run(k,v);
// Balance updates requested for game fame rewards.
// Safe additive migrations for existing installations.
try { db.exec("ALTER TABLE activity_log ADD COLUMN actor_role TEXT DEFAULT ''"); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; }
for (const [col,def] of [['delivered_at','TEXT'],['read_at','TEXT'],['reply_to','INTEGER']]) { try { db.exec(`ALTER TABLE messages ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
for (const [col,def] of [['ban_reason','TEXT'],['ban_until','TEXT'],['message_pass_until','TEXT'],['diamonds','INTEGER DEFAULT 0'],['face_verified','INTEGER DEFAULT 0'],['boost_until','TEXT'],['pro_until','TEXT'],['device_id','TEXT'],['is_game_banned','INTEGER DEFAULT 0']]) { try { db.exec(`ALTER TABLE users ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
for (const [col,def] of [['game_exit_streak','INTEGER NOT NULL DEFAULT 0'],['game_ban_until','TEXT']]) { try { db.exec(`ALTER TABLE users ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
for (const [col,def] of [['action','TEXT'],['from_id','INTEGER'],['request_id','INTEGER']]) { try { db.exec(`ALTER TABLE notifications ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
for (const [col,def] of [['promoted_remaining','INTEGER NOT NULL DEFAULT 0'],['promoted_until','TEXT']]) { try { db.exec(`ALTER TABLE chat_groups ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
try { db.exec("ALTER TABLE chat_groups ADD COLUMN promoted_campaign_id TEXT DEFAULT ''"); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; }

try { db.exec("ALTER TABLE posts ADD COLUMN cover_url TEXT DEFAULT ''"); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; }
try { db.exec('ALTER TABLE chat_group_members ADD COLUMN notifications_muted INTEGER NOT NULL DEFAULT 0'); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; }
for (const [col,def] of [['closed_at','TEXT'],['closed_by','INTEGER']]) { try { db.exec(`ALTER TABLE conversations ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
// Legacy one-sided hidden conversations represent a prior delete/hide action; migrate them to the new shared closed state without deleting messages.
try { db.exec("UPDATE conversations SET hidden_a=1,hidden_b=1,closed_at=COALESCE(closed_at,CURRENT_TIMESTAMP) WHERE (hidden_a=1 OR hidden_b=1) AND closed_at IS NULL"); } catch(e) { console.warn('conversation close-state migration:',e.message); }
try { db.exec("INSERT OR IGNORE INTO group_message_reads(group_id,user_id,last_read_message_id) SELECT m.group_id,m.user_id,COALESCE((SELECT MAX(gm.id) FROM chat_group_messages gm WHERE gm.group_id=m.group_id),0) FROM chat_group_members m"); } catch(e) { console.warn('group read-state migration:',e.message); }
for (const [col,def] of [['type',"TEXT NOT NULL DEFAULT 'text'"],['media_url',"TEXT DEFAULT ''"],['deleted','INTEGER NOT NULL DEFAULT 0'],['pinned','INTEGER NOT NULL DEFAULT 0']]) { try { db.exec(`ALTER TABLE chat_group_messages ADD COLUMN ${col} ${def}`); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; } }
const getSetting = k => db.prepare('SELECT value FROM settings WHERE key=?').get(k)?.value ?? defaults[k] ?? '';
db.exec('CREATE TABLE IF NOT EXISTS fame_like_awards(from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(from_id,to_id))');
db.exec('CREATE TABLE IF NOT EXISTS post_creation_requests(user_id INTEGER NOT NULL,request_id TEXT NOT NULL,post_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,request_id))');
db.exec('CREATE TABLE IF NOT EXISTS post_reward_draws(post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,won INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(post_id,user_id))');
db.exec('CREATE TABLE IF NOT EXISTS user_cosmetics(user_id INTEGER NOT NULL,item_id TEXT NOT NULL,owned_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,item_id))');
db.exec('CREATE TABLE IF NOT EXISTS user_cosmetic_equips(user_id INTEGER PRIMARY KEY,frame_id TEXT,updated_at TEXT DEFAULT CURRENT_TIMESTAMP)');
const setSetting = (k,v) => db.prepare('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run(k,String(v));
setSetting('app_name','ایس کلاب');setSetting('signup_coins','500');
if(!getSetting('admin_panel_password_hash')){
  const initialAdminPassword=String(cfg('ADMIN_PANEL_PASSWORD')||'0P9O8I7U');
  setSetting('admin_panel_password_hash',bcrypt.hashSync(initialAdminPassword,12));
}
// Core database helpers. Keep these before startup ownership migration and route registration.
const addTx = (uid, amount, kind, note='') => { db.prepare('UPDATE users SET coins=ROUND(coins+?,2) WHERE id=?').run(amount,uid); db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(uid,amount,kind,note); };
function atomic(work){ const snapshot=rawDb.export(); try { const result=work(); persistDb(); return result; } catch(e) { try { rawDb.close(); } catch {} rawDb=new SQL.Database(snapshot); persistDb(); throw e; } }
function recordLoginDay(uid){const key=iranKey(iranClock());db.prepare('INSERT OR IGNORE INTO user_login_days(user_id,day_key) VALUES(?,?)').run(uid,key);}

function logActivity(actor,target,action,details=''){const role=actor?String(userById(actor)?.role||''):'';db.prepare('INSERT INTO activity_log(actor_id,target_user_id,action,details,actor_role) VALUES(?,?,?,?,?)').run(actor||null,target||null,action,String(details).slice(0,500),role);}
const notificationCategory = (title='',body='',meta={}) => { const a=String(meta.action||''); if(a==='creator_broadcast')return 'important'; if(a==='direct_message'||a==='message_request'||/پیام|گفتگو/.test(title))return 'messages'; if(a==='like'||/لایک/.test(title))return 'likes'; if(a==='friend'||/دوست|دنبال/.test(title))return 'friends'; if(/سکه|الماس|تراکنش|جایزه/.test(title+' '+body))return 'coins'; if(/بازدید|پروفایل/.test(title))return 'profile_views'; if(/بازی|شهرت|رتبه/.test(title+' '+body))return 'games'; if(/تیکت|پشتیبانی/.test(title))return 'support'; return 'system'; };
const notify = (uid,title,body='',meta={}) => { const category=notificationCategory(title,body,meta); const row=db.prepare('SELECT preferences_json FROM user_notification_settings WHERE user_id=?').get(uid); let prefs={};try{prefs=JSON.parse(row?.preferences_json||'{}')}catch{} if(prefs[category]===false)return; db.prepare('INSERT INTO notifications(user_id,title,body,action,from_id,request_id) VALUES(?,?,?,?,?,?)').run(uid,title,body,meta.action||null,meta.fromId||null,meta.requestId||null); io.to('user:'+uid).emit('notification',{title,body,...meta,category}); };
const safeUser = u => u && ({id:u.id,username:u.username,display_name:u.display_name,bio:u.bio,gender:u.gender,age:u.age,province:u.province,avatar:u.avatar,coins:u.coins,diamonds:Number(u.diamonds||0),xp:u.xp,level:u.level,fame:u.fame,level_diamond_reward:Number(getSetting('level_diamond_reward'))||10,role:u.role,face_verified:!!u.face_verified,boost_until:u.boost_until,pro_until:u.pro_until,message_pass_until:u.message_pass_until,equipped_frame:db.prepare('SELECT frame_id FROM user_cosmetic_equips WHERE user_id=?').get(u.id)?.frame_id||null,equipped_frame_css:(()=>{const id=db.prepare('SELECT frame_id FROM user_cosmetic_equips WHERE user_id=?').get(u.id)?.frame_id;return (typeof ACE_COSMETICS!=='undefined'?ACE_COSMETICS.find(x=>x.id===id)?.css:null)||id?.replace('frame_','')||''})(),equipped_name_color:(()=>{const id=db.prepare("SELECT item_id FROM user_cosmetic_loadout WHERE user_id=? AND kind='namecolor'").get(u.id)?.item_id;return (typeof ACE_COSMETICS!=='undefined'?ACE_COSMETICS.find(x=>x.id===id)?.css:null)||''})(),equipped_badge:(()=>{const id=db.prepare("SELECT item_id FROM user_cosmetic_loadout WHERE user_id=? AND kind='badge'").get(u.id)?.item_id;return (typeof ACE_COSMETICS!=='undefined'?ACE_COSMETICS.find(x=>x.id===id)?.css:null)||''})(),created_at:u.created_at,last_seen:u.last_seen,online:!!(io.sockets.adapter.rooms.get('user:'+u.id)?.size)});
const userById = id => db.prepare('SELECT * FROM users WHERE id=?').get(Number(id));
const userByName = name => db.prepare('SELECT * FROM users WHERE username=? COLLATE NOCASE').get(String(name||''));
// A server-side signing key stays outside the database archive and signs creator recovery backups.
const recoveryKeyFile=path.join(dataDir,'.ace-creator-recovery-key');
let creatorRecoveryKey;
try { creatorRecoveryKey=fs.readFileSync(recoveryKeyFile); if(creatorRecoveryKey.length<32) throw new Error('short key'); }
catch { creatorRecoveryKey=crypto.randomBytes(48); try { fs.writeFileSync(recoveryKeyFile,creatorRecoveryKey,{mode:0o600,flag:'wx'}); } catch(e) { if(e.code==='EEXIST') creatorRecoveryKey=fs.readFileSync(recoveryKeyFile); else throw e; } }
// Existing installs: make the oldest real user the sole owner, not the old technical OWNER account.
{ const firstReal=db.prepare("SELECT * FROM users WHERE username<>'OWNER' COLLATE NOCASE ORDER BY id LIMIT 1").get();
  if(firstReal){db.prepare("UPDATE users SET role='user' WHERE role IN ('owner','creator') AND id<>?").run(firstReal.id);db.prepare("UPDATE users SET role='owner' WHERE id=?").run(firstReal.id);}
  else if(!userByName('OWNER')){const first=db.prepare('SELECT * FROM users ORDER BY id LIMIT 1').get();if(first)db.prepare("UPDATE users SET role='owner' WHERE id=?").run(first.id);}
}
if(['1','10'].includes(getSetting('game_win_fame')))setSetting('game_win_fame','250');if(['1','3'].includes(getSetting('game_loss_fame')))setSetting('game_loss_fame','30');
const isStaff = u => u && ['owner','creator','admin','moderator','support'].includes(u.role);
const isAdmin = u => u && ['owner','creator','admin'].includes(u.role);
// ACE CLUB requested fixes: persist group-message reply references.
try { db.exec('ALTER TABLE chat_group_messages ADD COLUMN reply_to INTEGER'); } catch(e) { if (!String(e.message).includes('duplicate column')) throw e; }
// Ensure the official ACE CLUB community group exists and every existing/new account is a member.
let officialGroup=db.prepare("SELECT * FROM chat_groups WHERE group_code='ACECLUB_OFFICIAL' OR name='گروه ایس کلاب' ORDER BY CASE WHEN group_code='ACECLUB_OFFICIAL' THEN 0 ELSE 1 END,id LIMIT 1").get();
if(!officialGroup){const owner=db.prepare("SELECT * FROM users WHERE role='owner' ORDER BY id LIMIT 1").get();if(owner){const code='ACECLUB_OFFICIAL';const r=db.prepare('INSERT INTO chat_groups(name,group_code,description,owner_id) VALUES(?,?,?,?)').run('گروه ایس کلاب',code,'گروه همگانی رسمی ایس کلاب',owner.id);officialGroup=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(Number(r.lastInsertRowid));}}
if(officialGroup){for(const u of db.prepare('SELECT id FROM users').all())db.prepare("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')").run(officialGroup.id,u.id);const owner=db.prepare("SELECT * FROM users WHERE role='owner' ORDER BY id LIMIT 1").get();if(owner){db.prepare("UPDATE chat_groups SET owner_id=? WHERE id=?").run(owner.id,officialGroup.id);db.prepare("UPDATE chat_group_members SET role='member' WHERE group_id=? AND role='owner' AND user_id<>?").run(officialGroup.id,owner.id);db.prepare("UPDATE chat_group_members SET role='owner' WHERE group_id=? AND user_id=?").run(officialGroup.id,owner.id);}}

if (process.env.NODE_ENV === 'production') app.set('trust proxy', 1);
app.use(helmet({contentSecurityPolicy:false}));
app.use(express.json({limit:'2mb'}));
app.use(express.urlencoded({extended:true}));
app.use(cookieParser());
app.use(session({store:sessionStore,secret:cfg('SESSION_SECRET')||'local-development-only-secret-change-before-deploying-please',resave:false,saveUninitialized:false,name:'dor.sid',cookie:{httpOnly:true,sameSite:'lax',secure:process.env.NODE_ENV === 'production',maxAge:30*24*3600*1000}}));
const requireAuth = (req,res,next) => { if(!req.session?.uid)return res.status(401).json({error:'ابتدا وارد حساب شوید'}); let u=userById(Number(req.session.uid)); if(u?.is_banned && u.ban_until && Date.parse(u.ban_until)<=Date.now()){db.prepare('UPDATE users SET is_banned=0,ban_reason=NULL,ban_until=NULL WHERE id=?').run(u.id);u=userById(u.id);} if(!u || u.is_banned) return res.status(401).json({error:u?.ban_reason?`حساب مسدود است: ${u.ban_reason}`:'ابتدا وارد حساب شوید'}); req.user=u; const did=String(req.get('X-ACE-Device-ID')||'').trim().slice(0,100); if(did && /^[A-Za-z0-9._:-]+$/.test(did) && u.device_id!==did){try{db.prepare('UPDATE users SET device_id=? WHERE id=?').run(did,u.id);u.device_id=did;}catch{}} next(); };

const enforceRestriction=(type)=> (req,res,next)=>{if(!req.user)return next();const r=db.prepare("SELECT restriction_type,ends_at,reason FROM user_restrictions WHERE user_id=? AND active=1 AND ends_at>CURRENT_TIMESTAMP AND restriction_type IN (?, 'app') ORDER BY CASE restriction_type WHEN 'app' THEN 0 ELSE 1 END LIMIT 1").get(req.user.id,type);if(r)return res.status(403).json({error:`این قابلیت تا ${new Date(r.ends_at).toLocaleString('fa-IR')} محدود شده است. علت: ${r.reason}`,restriction:true,endsAt:r.ends_at});next();};
// Maintenance mode is enforced server-side as well as in the client. Owner/creator retain full access.
app.use('/api',(req,res,next)=>{if(getSetting('maintenance')!=='1')return next();const allowed=['/config','/me','/maintenance/status','/auth/login','/auth/register','/logo'];if(allowed.includes(req.path))return next();const u=req.session?.uid?userById(Number(req.session.uid)):null;if(u&&['owner','creator'].includes(u.role))return next();return res.status(503).json({error:'ایس کلاب در حال بروزرسانی است؛ لطفاً کمی بعد دوباره تلاش کنید.',maintenance:true});});
app.use('/api',(req,res,next)=>{const allowed=['/config','/me','/auth/logout','/maintenance/status'];if(allowed.includes(req.path)||!req.session?.uid)return next();const r=db.prepare("SELECT ends_at,reason FROM user_restrictions WHERE user_id=? AND restriction_type='app' AND active=1 AND ends_at>CURRENT_TIMESTAMP ORDER BY id DESC LIMIT 1").get(Number(req.session.uid));if(r)return res.status(403).json({error:`کل برنامه تا ${new Date(r.ends_at+'Z').toLocaleString('fa-IR')} محدود شده است. علت: ${r.reason}`,restriction:true,endsAt:r.ends_at});next();});

// Block list and block enforcement; all mutations are authenticated and target IDs are validated.
app.post('/api/groups/:id/mute',requireAuth,(req,res)=>{const gid=Number(req.params.id);if(!groupMember(gid,req.user.id))return res.status(404).json({error:'عضو این گروه نیستید'});const old=db.prepare('SELECT muted FROM group_user_settings WHERE group_id=? AND user_id=?').get(gid,req.user.id);const muted=old?.muted?0:1;db.prepare('INSERT INTO group_user_settings(group_id,user_id,muted,updated_at) VALUES(?,?,?,CURRENT_TIMESTAMP) ON CONFLICT(group_id,user_id) DO UPDATE SET muted=excluded.muted,updated_at=CURRENT_TIMESTAMP').run(gid,req.user.id,muted);res.json({ok:true,muted:!!muted});});
app.get('/api/blocks',requireAuth,(req,res)=>{const users=db.prepare('SELECT u.id,u.username,u.display_name,u.avatar FROM blocks b JOIN users u ON u.id=b.blocked_id WHERE b.user_id=? ORDER BY b.created_at DESC').all(req.user.id);res.json({users});});
app.post('/api/users/:id/block',requireAuth,(req,res)=>{const id=Number(req.params.id);if(!Number.isInteger(id)||id===req.user.id||!userById(id))return res.status(400).json({error:'کاربر نامعتبر است'});db.prepare('INSERT OR IGNORE INTO blocks(user_id,blocked_id) VALUES(?,?)').run(req.user.id,id);db.prepare("UPDATE friend_requests SET status='rejected' WHERE status='pending' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").run(req.user.id,id,id,req.user.id);db.prepare("UPDATE message_requests SET status='rejected' WHERE status='pending' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").run(req.user.id,id,id,req.user.id);logActivity(req.user.id,id,'block_user','مسدودسازی از پروفایل');res.json({ok:true,blocked:true});});
app.delete('/api/users/:id/block',requireAuth,(req,res)=>{const id=Number(req.params.id);const result=db.prepare('DELETE FROM blocks WHERE user_id=? AND blocked_id=?').run(req.user.id,id);logActivity(req.user.id,id,'unblock_user','رفع مسدودی');res.json({ok:true,blocked:false,unblocked:result.changes>0});});

const requireStaff = (req,res,next) => !isStaff(req.user) ? res.status(403).json({error:'دسترسی ندارید'}) : next();
const requireAdmin = (req,res,next) => !isAdmin(req.user) ? res.status(403).json({error:'دسترسی ندارید'}) : next();
const requireGameAccess = (req,res,next) => { const restriction=db.prepare("SELECT ends_at,reason FROM user_restrictions WHERE user_id=? AND active=1 AND ends_at>CURRENT_TIMESTAMP AND restriction_type IN ('games','app') ORDER BY CASE restriction_type WHEN 'app' THEN 0 ELSE 1 END LIMIT 1").get(req.user.id); if(restriction)return res.status(403).json({error:`دسترسی بازی تا ${new Date(restriction.ends_at).toLocaleString('fa-IR')} محدود است. علت: ${restriction.reason}`}); if(Number(req.user?.is_game_banned||0)) return res.status(403).json({error:'دسترسی شما به بازی‌ها توسط مدیریت محدود شده است'}); const until=req.user?.game_ban_until; if(until && Date.parse(until)>Date.now()) return res.status(403).json({error:'به علت خروج از بازی امتیازی، حساب شما به مدت ۶ ساعت از بازی مسدود می‌باشد.'}); if(until && Date.parse(until)<=Date.now()) db.prepare('UPDATE users SET game_ban_until=NULL,game_exit_streak=0 WHERE id=?').run(req.user.id); return next(); };
const requirePermission = permission => (req,res,next) => { if(req.user && ['owner','creator'].includes(req.user.role)) return next(); const row=db.prepare('SELECT enabled FROM user_permissions WHERE user_id=? AND permission=?').get(req.user?.id,permission); return row&&Number(row.enabled)===1 ? next() : res.status(403).json({error:'مجوز لازم را ندارید'}); };
const apkDir=path.join(dataDir,'apk');
fs.mkdirSync(apkDir,{recursive:true});
const apkUpload=multer({storage:multer.memoryStorage(),limits:{fileSize:150*1024*1024},fileFilter:(req,file,cb)=>{
 const ext=path.extname(String(file.originalname||'')).toLowerCase();
 if(ext!=='.apk')return cb(new Error('فقط فایل APK مجاز است'));
 cb(null,true);
}});
app.get('/api/app-release',(req,res)=>{
 const r=db.prepare('SELECT id,version,original_name,size_bytes,sha256,created_at FROM app_releases WHERE active=1 ORDER BY id DESC LIMIT 1').get();
 if(!r)return res.status(404).json({error:'هنوز نسخه قابل دانلودی ثبت نشده است'});
 res.json({...r,downloadUrl:`/api/app-release/${r.id}/download`,sizeLabel:`${(r.size_bytes/1024/1024).toFixed(2)} MB`});
});
app.get('/api/app-release/:id/download',(req,res)=>{
 const r=db.prepare('SELECT * FROM app_releases WHERE id=? AND active=1').get(Number(req.params.id));
 if(!r)return res.status(404).json({error:'فایل نصب پیدا نشد'});
 const file=path.join(apkDir,path.basename(r.filename));
 if(!fs.existsSync(file))return res.status(410).json({error:'فایل روی سرور موجود نیست'});
 res.setHeader('Content-Type','application/vnd.android.package-archive');
 res.setHeader('Content-Disposition',`attachment; filename="${path.basename(r.filename)}"`);
 res.setHeader('Content-Length',String(r.size_bytes));
 fs.createReadStream(file).pipe(res);
});
app.post('/api/admin/app-release',requireAuth,requirePermission('roles.manage'),apkUpload.single('apk'),(req,res)=>{
 if(!req.file)return res.status(400).json({error:'فایل APK انتخاب نشده است'});
 const buf=req.file.buffer;
 if(buf.length<4||buf[0]!==0x50||buf[1]!==0x4b)return res.status(400).json({error:'ساختار فایل APK معتبر نیست'});
 const version=String(req.body.version||'').trim().slice(0,40);
 if(!version)return res.status(400).json({error:'نسخه برنامه را وارد کن'});
 const hash=crypto.createHash('sha256').update(buf).digest('hex');
 const filename=`ace-club-${Date.now()}-${hash.slice(0,12)}.apk`;
 const target=path.join(apkDir,filename);
 try{
  fs.writeFileSync(target,buf,{flag:'wx'});
  atomic(()=>{
   db.prepare('UPDATE app_releases SET active=0 WHERE active=1').run();
   db.prepare('INSERT INTO app_releases(version,filename,original_name,size_bytes,sha256,uploaded_by,active) VALUES(?,?,?,?,?,?,1)').run(version,filename,path.basename(req.file.originalname),buf.length,hash,req.user.id);
  });
 }catch(e){try{fs.unlinkSync(target)}catch{};return res.status(500).json({error:'ذخیره نسخه انجام نشد'});}
 const release=db.prepare('SELECT id,version,original_name,size_bytes,sha256,created_at FROM app_releases WHERE filename=?').get(filename);
 res.json({ok:true,release,downloadUrl:`/api/app-release/${release.id}/download`});
});
app.delete('/api/admin/app-release/:id',requireAuth,requirePermission('roles.manage'),(req,res)=>{
 const r=db.prepare('SELECT * FROM app_releases WHERE id=?').get(Number(req.params.id));
 if(!r)return res.status(404).json({error:'نسخه پیدا نشد'});
 db.prepare('UPDATE app_releases SET active=0 WHERE id=?').run(r.id);
 try{fs.unlinkSync(path.join(apkDir,path.basename(r.filename)))}catch{}
 res.json({ok:true});
});
const isPrivilegedManager=u=>!!(u&&['owner','creator'].includes(u.role));
const userFieldPermission=(uid,action,field)=>db.prepare('SELECT 1 FROM user_permissions WHERE user_id=? AND permission=? AND enabled=1').get(uid,`users.field.${action}.${field}`);
const canUserField=(u,action,field)=>isPrivilegedManager(u)||!!userFieldPermission(u.id,action,field);
const ADMIN_UNLOCK_MS=30*60*1000;
const adminPanelUnlocked=req=>Number(req.session?.admin_unlocked_until||0)>Date.now();
app.use('/api/admin',(req,res,next)=>{
  if(['/unlock','/lock','/my-permissions'].includes(req.path)) return next();
  if(!req.session?.uid) return res.status(401).json({error:'ابتدا وارد حساب شوید'});
  if(!adminPanelUnlocked(req)) return res.status(423).json({error:'برای ورود به پنل مدیریت، رمز مدیریت را وارد کنید',adminLocked:true});
  if(['POST','PUT','PATCH','DELETE'].includes(req.method)){
    res.on('finish',()=>{if(res.statusCode>=200&&res.statusCode<400&&req.session?.uid){const actorId=Number(req.session.uid);const actor=userById(actorId);if(actor){const target=Number(req.params?.id||req.body?.userId||req.body?.targetUserId||0)||null;const action=`admin_${req.method.toLowerCase()} ${req.path}`;let details='پنل مدیریت';try{details=JSON.stringify(req.body||{}).slice(0,450)}catch{}logActivity(actorId,target,action,details)}}});
  }
  next();
});
app.post('/api/admin/unlock',requireAuth,rateLimit({windowMs:10*60*1000,limit:10,standardHeaders:true,legacyHeaders:false}),(req,res)=>{
  const password=String(req.body?.password||'');
  const hash=String(getSetting('admin_panel_password_hash')||'');
  if(!hash||!bcrypt.compareSync(password,hash)) return res.status(401).json({error:'رمز مدیریت اشتباه است'});
  req.session.admin_unlocked_until=Date.now()+ADMIN_UNLOCK_MS;
  logActivity(req.user.id,req.user.id,'admin_unlock','ورود به پنل مدیریت با رمز دوم');
  res.json({ok:true,expiresAt:req.session.admin_unlocked_until});
});
app.post('/api/admin/lock',requireAuth,(req,res)=>{delete req.session.admin_unlocked_until;res.json({ok:true});});
app.use('/api/auth',rateLimit({windowMs:15*60*1000,limit:60,standardHeaders:true,legacyHeaders:false}));

// ACE CLUB: Search engine files
app.get('/sitemap.xml', (req, res) => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://aceclub.ir/</loc>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>`;
  res.status(200)
    .set('Content-Type', 'application/xml; charset=utf-8')
    .send(xml);
});

app.get('/robots.txt', (req, res) => {
  res.status(200)
    .type('text/plain')
    .send('User-agent: *\nAllow: /\nSitemap: https://aceclub.ir/sitemap.xml\n');
});

app.use('/uploads',express.static(uploadsDir, {dotfiles:'deny', index:false, fallthrough:false, maxAge:'1h'}));
app.get('/install',(req,res)=>res.sendFile(path.join(ROOT,'public','install.html')));
app.get('/grop/:code',(req,res)=>res.sendFile(path.join(ROOT,'public','index.html')));
app.use(express.static(path.join(ROOT,'public')));
const safeUploadExtension = mime => ({'image/jpeg':'.jpg','image/png':'.png','image/webp':'.webp','image/gif':'.gif','video/mp4':'.mp4','video/webm':'.webm','video/quicktime':'.mov','audio/webm':'.webm','audio/ogg':'.ogg','audio/mpeg':'.mp3','audio/mp4':'.m4a','audio/wav':'.wav','audio/x-wav':'.wav','audio/aac':'.aac','audio/flac':'.flac','application/octet-stream':'.bin'}[String(mime||'').toLowerCase()] || '.bin');
const storage=multer.diskStorage({destination:(req,file,cb)=>cb(null,uploadsDir),filename:(req,file,cb)=>{const mimeExt=safeUploadExtension(file.mimetype);const originalExt=path.extname(String(file.originalname||'')).toLowerCase();const allowedExt=['.jpg','.jpeg','.png','.webp','.gif','.heic','.heif','.avif','.mp4','.m4v','.webm','.mov','.3gp','.3gpp','.mkv','.avi','.mpeg','.mpg','.mp3','.m4a','.wav','.ogg','.aac','.flac'];cb(null,crypto.randomBytes(16).toString('hex')+(mimeExt!=='.bin'?mimeExt:(allowedExt.includes(originalExt)?originalExt:'.bin')))}});
const upload=multer({storage,limits:{fileSize:8*1024*1024},fileFilter:(req,file,cb)=>cb(null,/^(image\/(jpeg|png|webp|gif)|audio\/(webm|ogg|mpeg|mp4|wav))$/.test(file.mimetype))});
const postUpload=multer({storage,limits:{fileSize:2*1024*1024},fileFilter:(req,file,cb)=>{const mime=String(file.mimetype||'').toLowerCase();const ext=path.extname(String(file.originalname||'')).toLowerCase();const imageExt=['.jpg','.jpeg','.png','.webp','.gif','.heic','.heif','.avif','.bmp','.tif','.tiff'];const videoExt=['.mp4','.m4v','.webm','.mov','.3gp','.3gpp','.mkv','.avi','.mpeg','.mpg','.ogv'];const imageOk=/^image\//.test(mime)||imageExt.includes(ext);const videoOk=/^video\//.test(mime)||videoExt.includes(ext);if(imageOk||videoOk)return cb(null,true);return cb(new Error('فرمت فایل پشتیبانی نمی‌شود؛ یک عکس یا ویدئو انتخاب کن.'))}});
const databaseUpload=multer({storage:multer.memoryStorage(),limits:{fileSize:300*1024*1024}});
app.post('/api/security/creator-recover',requireAuth,databaseUpload.single('file'),(req,res)=>{
 let candidate;
 try {
  if(!req.file)throw new Error('فایل پشتیبان کامل .tar.gz را انتخاب کن');
  if(!/\.tar\.gz$/i.test(String(req.file.originalname||'')))throw new Error('فقط فایل پشتیبان کامل .tar.gz قابل قبول است؛ فایل SQLite ساده امضای بازیابی ندارد');
  const files=readTarGz(req.file.buffer),dbEntry=files.find(f=>f.name==='database/dordor.sqlite'),manifestEntry=files.find(f=>f.name==='recovery/manifest.json');
  if(!dbEntry||!manifestEntry)throw new Error('این فایل امضای بازیابی سازنده ندارد؛ بعد از این بروزرسانی یک پشتیبان کامل تازه بگیر');
  const manifest=JSON.parse(manifestEntry.data.toString('utf8'));
  const digest=crypto.createHash('sha256').update(dbEntry.data).digest('hex');
  if(manifest.format!=='ACECLUB_CREATOR_BACKUP_V1'||manifest.db_sha256!==digest||!/^[a-f0-9]{64}$/i.test(String(manifest.signature||'')))throw new Error('امضای فایل پشتیبان معتبر نیست');
  const signedName=String(manifest.creator_username||'').toLowerCase();
  const expected=crypto.createHmac('sha256',creatorRecoveryKey).update('ACECLUB_CREATOR_BACKUP_V1|'+digest+'|'+signedName).digest('hex');
  if(!crypto.timingSafeEqual(Buffer.from(expected,'hex'),Buffer.from(String(manifest.signature),'hex')))throw new Error('این فایل توسط سرور فعلی ACE CLUB امضا نشده یا کلید بازیابی در دسترس نیست');
  if(signedName!==String(req.user.username||'').toLowerCase())throw new Error('این پشتیبان برای حساب فعلی نیست؛ با همان نام کاربری که سازنده بوده وارد شو');
  candidate=new SQL.Database(new Uint8Array(dbEntry.data));
  const userCols=candidate.exec('PRAGMA table_info(users)')[0]?.values?.map(r=>r[1])||[];
  if(!['username','role'].every(c=>userCols.includes(c)))throw new Error('ساختار حساب‌های فایل پشتیبان معتبر نیست');
  const stmt=candidate.prepare('SELECT role FROM users WHERE username=? COLLATE NOCASE');let oldRole='';try{stmt.bind([req.user.username]);if(stmt.step())oldRole=String(stmt.getAsObject().role||'')}finally{stmt.free()}
  if(!['owner','creator'].includes(oldRole))throw new Error('در این پشتیبان، حساب شما سازنده نبوده است');
  db.prepare("UPDATE users SET role='user' WHERE role IN ('owner','creator') AND id<>?").run(req.user.id);
  db.prepare("UPDATE users SET role='owner' WHERE id=?").run(req.user.id);
  logActivity(req.user.id,req.user.id,'creator_recovered','بازیابی نقش سازنده با فایل پشتیبان امضاشده');
  if(officialGroup){db.prepare("UPDATE chat_group_members SET role='member' WHERE group_id=? AND role='owner' AND user_id<>?").run(officialGroup.id,req.user.id);db.prepare("INSERT INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'owner') ON CONFLICT(group_id,user_id) DO UPDATE SET role='owner'").run(officialGroup.id,req.user.id);}
  res.json({ok:true,message:'نقش سازنده برای همین حساب بازیابی شد. صفحه را تازه‌سازی کن.'});
 }catch(e){res.status(400).json({error:e.message||'بازیابی سازندگی ناموفق بود'});}finally{try{candidate?.close()}catch{}}
});

const musicUpload=multer({storage,limits:{fileSize:50*1024*1024},fileFilter:(req,file,cb)=>cb(null,/^(audio\/(mpeg|mp3|mp4|wav|ogg|webm|aac|x-wav|flac)|application\/octet-stream)$/.test(file.mimetype))});
const logoUpload=multer({storage,limits:{fileSize:8*1024*1024},fileFilter:(req,file,cb)=>cb(null,/^image\/(jpeg|png|webp|gif)$/.test(file.mimetype))});
function tarHeader(name,size,mtime=Math.floor(Date.now()/1000)){const h=Buffer.alloc(512);const put=(v,o,n)=>h.write(String(v),o,n,'utf8');put(name.slice(0,100),0,100);put('0000644',100,8);put('0000000',108,8);put('0000000',116,8);put(size.toString(8).padStart(11,'0')+'\0',124,12);put(mtime.toString(8).padStart(11,'0')+'\0',136,12);h.fill(32,148,156);put('0',156,1);put('ustar\0',257,6);put('00',263,2);let sum=0;for(const b of h)sum+=b;put(sum.toString(8).padStart(6,'0')+'\0 ',148,8);return h;}
function makeTar(entries){const parts=[];for(const e of entries){parts.push(tarHeader(e.name,e.data.length),e.data);const pad=(512-e.data.length%512)%512;if(pad)parts.push(Buffer.alloc(pad));}parts.push(Buffer.alloc(1024));return Buffer.concat(parts);}
function readTarGz(buffer){
 const tar=zlib.gunzipSync(buffer,{maxOutputLength:500*1024*1024});
 const files=[];let off=0,pax={},longName='';
 const parsePax=buf=>{const out={};let i=0;while(i<buf.length){const sp=buf.indexOf(32,i);if(sp<0)break;const len=Number(buf.toString('utf8',i,sp));if(!Number.isSafeInteger(len)||len<5||i+len>buf.length)break;const rec=buf.toString('utf8',sp+1,i+len-1);const eq=rec.indexOf('=');if(eq>0)out[rec.slice(0,eq)]=rec.slice(eq+1);i+=len;}return out};
 while(off+512<=tar.length){const h=tar.subarray(off,off+512);if(h.every(b=>b===0))break;
  const headerName=h.toString('utf8',0,100).replace(/\0.*$/s,'');
  const sizeText=h.toString('utf8',124,136).replace(/\0.*$/s,'').trim()||'0';
  const size=parseInt(sizeText,8);if(!Number.isSafeInteger(size)||size<0||off+512+size>tar.length)throw new Error('ساختار آرشیو پشتیبان معتبر نیست');
  const type=h[156];const data=Buffer.from(tar.subarray(off+512,off+512+size));
  if(type===120){pax={...pax,...parsePax(data)};off+=512+Math.ceil(size/512)*512;continue;}
  if(type===76){longName=data.toString('utf8').replace(/\0.*$/s,'').trim();off+=512+Math.ceil(size/512)*512;continue;}
  const name=pax.path||longName||headerName;
  if(type===0||type===48)files.push({name,data});
  pax={};longName='';off+=512+Math.ceil(size/512)*512;
 }
 return files;
}
function readZip(buffer){const files=[];let eocd=-1;for(let i=Math.max(0,buffer.length-65557);i<=buffer.length-22;i++){if(buffer.readUInt32LE(i)===0x06054b50)eocd=i;}if(eocd<0)throw new Error('ساختار ZIP پشتیبان معتبر نیست');const count=buffer.readUInt16LE(eocd+10),centralSize=buffer.readUInt32LE(eocd+12),centralOffset=buffer.readUInt32LE(eocd+16);if(count>10000||centralOffset+centralSize>buffer.length)throw new Error('فهرست فایل‌های ZIP معتبر نیست');let off=centralOffset;for(let i=0;i<count;i++){if(off+46>buffer.length||buffer.readUInt32LE(off)!==0x02014b50)throw new Error('ساختار فایل ZIP ناقص است');const method=buffer.readUInt16LE(off+10),compressedSize=buffer.readUInt32LE(off+20),size=buffer.readUInt32LE(off+24),nameLen=buffer.readUInt16LE(off+28),extraLen=buffer.readUInt16LE(off+30),commentLen=buffer.readUInt16LE(off+32),localOffset=buffer.readUInt32LE(off+42);const name=buffer.toString('utf8',off+46,off+46+nameLen);off+=46+nameLen+extraLen+commentLen;if(name.endsWith('/'))continue;if(!safeArchivePath(name))throw new Error('مسیر فایل داخل ZIP نامعتبر است');if(size>500*1024*1024||compressedSize>buffer.length)throw new Error('حجم فایل داخل ZIP غیرمجاز است');if(localOffset+30>buffer.length||buffer.readUInt32LE(localOffset)!==0x04034b50)throw new Error('فایل ZIP ناقص است');const ln=buffer.readUInt16LE(localOffset+26),le=buffer.readUInt16LE(localOffset+28),start=localOffset+30+ln+le,end=start+compressedSize;if(end>buffer.length)throw new Error('داده فایل ZIP ناقص است');const packed=buffer.subarray(start,end);let data;if(method===0)data=Buffer.from(packed);else if(method===8)data=zlib.inflateRawSync(packed,{maxOutputLength:500*1024*1024});else throw new Error('روش فشرده‌سازی ZIP پشتیبانی نمی‌شود');if(data.length!==size)throw new Error('اندازه فایل ZIP با فهرست آن مطابقت ندارد');files.push({name,data});}return files;}
function listFiles(dir,prefix=''){if(!fs.existsSync(dir))return [];const out=[];for(const ent of fs.readdirSync(dir,{withFileTypes:true})){const full=path.join(dir,ent.name),rel=prefix?prefix+'/'+ent.name:ent.name;if(ent.isSymbolicLink())continue;if(ent.isDirectory())out.push(...listFiles(full,rel));else if(ent.isFile())out.push({name:rel,data:fs.readFileSync(full)});}return out;}
function safeArchivePath(name){return typeof name==='string'&&!name.startsWith('/')&&!name.includes('\\')&&!name.split('/').some(p=>!p||p==='.'||p==='..');}
function conversationFor(a,b){const x=Math.min(a,b),y=Math.max(a,b);let c=db.prepare('SELECT * FROM conversations WHERE user_a=? AND user_b=?').get(x,y);if(!c){const r=db.prepare('INSERT INTO conversations(user_a,user_b) VALUES(?,?)').run(x,y);c=db.prepare('SELECT * FROM conversations WHERE id=?').get(r.lastInsertRowid);}return c;}
function convMember(c,uid){return c && (c.user_a===uid||c.user_b===uid);}
function conversationClosed(c){return !!(c&&c.closed_at);}
function closedConversationResponse(res){return res.status(410).json({error:'این گفت‌وگو توسط یکی از طرفین حذف شده و پیام‌رسانی برای هر دو طرف قطع است'});}
function markGroupRead(groupId,uid){const last=Number(db.prepare('SELECT COALESCE(MAX(id),0) n FROM chat_group_messages WHERE group_id=? AND deleted=0').get(groupId)?.n||0);db.prepare('INSERT INTO group_message_reads(group_id,user_id,last_read_message_id,updated_at) VALUES(?,?,?,CURRENT_TIMESTAMP) ON CONFLICT(group_id,user_id) DO UPDATE SET last_read_message_id=MAX(group_message_reads.last_read_message_id,excluded.last_read_message_id),updated_at=CURRENT_TIMESTAMP').run(groupId,uid,last);return last;}
function unreadSummary(uid){const privateUnread=Number(db.prepare("SELECT COUNT(*) n FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.sender_id<>? AND m.deleted=0 AND m.read_at IS NULL AND c.closed_at IS NULL AND ((c.user_a=? AND c.hidden_a=0) OR (c.user_b=? AND c.hidden_b=0))").get(uid,uid,uid)?.n||0);const groupUnread=Number(db.prepare("SELECT COUNT(*) n FROM chat_group_messages gm JOIN chat_group_members cm ON cm.group_id=gm.group_id AND cm.user_id=? LEFT JOIN group_message_reads gr ON gr.group_id=gm.group_id AND gr.user_id=cm.user_id WHERE gm.deleted=0 AND gm.sender_id<>? AND gm.id>COALESCE(gr.last_read_message_id,0)").get(uid,uid)?.n||0);return {private:privateUnread,groups:groupUnread,total:privateUnread+groupUnread};}
function blockedBetween(a,b){return !!db.prepare('SELECT 1 FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(Number(a),Number(b),Number(b),Number(a));}
function rejectBlocked(req,res,otherId){if(blockedBetween(req.user.id,otherId)){res.status(403).json({error:'این کاربر در یکی از دو طرف مسدود شده است؛ گفتگو و درخواست‌ها در دسترس نیستند'});return true;}return false;}
function otherId(c,uid){return c.user_a===uid?c.user_b:c.user_a;}
function sendMessage(c,uid,type,body,media='',replyTo=null){const other=otherId(c,uid),online=!!(io.sockets.adapter.rooms.get('user:'+other)?.size);const r=online?db.prepare('INSERT INTO messages(conversation_id,sender_id,type,body,media_url,reply_to,delivered_at) VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)').run(c.id,uid,type,body,media,replyTo):db.prepare('INSERT INTO messages(conversation_id,sender_id,type,body,media_url,reply_to,delivered_at) VALUES(?,?,?,?,?,?,NULL)').run(c.id,uid,type,body,media,replyTo);const msg=db.prepare('SELECT * FROM messages WHERE id=?').get(r.lastInsertRowid);notify(other,'پیام مستقیم',type==='text'?String(body).slice(0,80):'یک فایل برای شما فرستاده شد',{action:'direct_message',fromId:uid,requestId:c.id});io.to('user:'+other).emit('message',msg);io.to('user:'+uid).emit('message',msg);io.to('user:'+other).emit('unread:updated',unreadSummary(other));io.to('user:'+uid).emit('unread:updated',unreadSummary(uid));return msg;}
function consumeProQuota(uid, includeChatRequest=false){const now=Date.now();const windows=[['8h',8*3600000,Number(getSetting('pro_quota_8h')||40)],['24h',86400000,Number(getSetting('pro_quota_24h')||100)],['7d',604800000,Number(getSetting('pro_quota_7d')||400)]];for(const [key,ms,total] of windows){const row=db.prepare('SELECT * FROM pro_usage WHERE user_id=? AND window_key=?').get(uid,key);if(row&&now-Date.parse(row.window_start)<ms&&row.used>=total)return 'سهمیه مصرف منصفانه این بازه تمام شده است';}if(includeChatRequest){const row=db.prepare('SELECT * FROM pro_usage WHERE user_id=? AND window_key=?').get(uid,'chat24');if(row&&now-Date.parse(row.window_start)<86400000&&row.used>=Number(getSetting('pro_chat_requests')||2))return 'سهمیه درخواست چت پرو تمام شده است';}for(const [key,ms] of windows){const row=db.prepare('SELECT * FROM pro_usage WHERE user_id=? AND window_key=?').get(uid,key);if(!row||now-Date.parse(row.window_start)>=ms)db.prepare('INSERT INTO pro_usage(user_id,window_key,window_start,used) VALUES(?,?,CURRENT_TIMESTAMP,1) ON CONFLICT(user_id,window_key) DO UPDATE SET window_start=CURRENT_TIMESTAMP,used=1').run(uid,key);else db.prepare('UPDATE pro_usage SET used=used+1 WHERE user_id=? AND window_key=?').run(uid,key);}if(includeChatRequest){const row=db.prepare('SELECT * FROM pro_usage WHERE user_id=? AND window_key=?').get(uid,'chat24');if(!row||now-Date.parse(row.window_start)>=86400000)db.prepare('INSERT INTO pro_usage(user_id,window_key,window_start,used) VALUES(?,?,CURRENT_TIMESTAMP,1) ON CONFLICT(user_id,window_key) DO UPDATE SET window_start=CURRENT_TIMESTAMP,used=1').run(uid,'chat24');else db.prepare('UPDATE pro_usage SET used=used+1 WHERE user_id=? AND window_key=?').run(uid,'chat24');}return null;}

function consumeFreeMessageQuota(uid){
 const now=Date.now(),quota=Math.max(0,Number(getSetting('free_message_quota')||10));
 const row=db.prepare("SELECT * FROM pro_usage WHERE user_id=? AND window_key='free24'").get(uid);
 if(row&&now-Date.parse(row.window_start)<86400000&&Number(row.used)>=quota)return false;
 if(!row||now-Date.parse(row.window_start)>=86400000)db.prepare("INSERT INTO pro_usage(user_id,window_key,window_start,used) VALUES(?,'free24',CURRENT_TIMESTAMP,1) ON CONFLICT(user_id,window_key) DO UPDATE SET window_start=CURRENT_TIMESTAMP,used=1").run(uid);
 else db.prepare("UPDATE pro_usage SET used=used+1 WHERE user_id=? AND window_key='free24'").run(uid);
 return true;
}
// Friend requests are free; accepting one unlocks regular messaging.
app.post('/api/users/:id/friend-request',requireAuth,(req,res)=>{const id=Number(req.params.id);if(!userById(id)||id===req.user.id)return res.status(400).json({error:'کاربر نامعتبر است'});if(blockedBetween(req.user.id,id))return res.status(403).json({error:'به‌دلیل مسدودسازی امکان درخواست وجود ندارد'});const accepted=db.prepare("SELECT 1 FROM friend_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,id,id,req.user.id);if(accepted)return res.status(409).json({error:'از قبل دنبال‌کننده دوطرفه هستید'});const existing=db.prepare('SELECT * FROM friend_requests WHERE from_id=? AND to_id=?').get(req.user.id,id);if(existing?.status==='pending')return res.status(409).json({error:'درخواست دنبال کردن قبلاً ارسال شده است'});if(existing)db.prepare("UPDATE friend_requests SET status='pending',created_at=CURRENT_TIMESTAMP WHERE id=?").run(existing.id);else db.prepare("INSERT INTO friend_requests(from_id,to_id,status) VALUES(?,?,'pending')").run(req.user.id,id);const fr=db.prepare("SELECT id FROM friend_requests WHERE from_id=? AND to_id=?").get(req.user.id,id);notify(id,'درخواست دنبال کردن',req.user.display_name+' درخواست دنبال کردن فرستاد',{action:'friend',fromId:req.user.id,requestId:fr?.id});res.json({ok:true,requestId:fr?.id});});
app.post('/api/friend-requests/:id/respond',requireAuth,(req,res)=>{const id=Number(req.params.id),r=db.prepare("SELECT * FROM friend_requests WHERE id=? AND to_id=? AND status='pending'").get(id,req.user.id);if(!r)return res.status(404).json({error:'درخواست پیدا نشد'});if(rejectBlocked(req,res,r.from_id))return;const accept=req.body.accept===true;db.prepare('UPDATE friend_requests SET status=? WHERE id=?').run(accept?'accepted':'rejected',id);if(accept){db.prepare('INSERT OR IGNORE INTO follows(follower_id,followed_id) VALUES(?,?),(?,?)').run(r.from_id,r.to_id,r.to_id,r.from_id);}notify(r.from_id,accept?'درخواست دنبال کردن پذیرفته شد':'درخواست دنبال کردن رد شد',req.user.display_name+(accept?' درخواست دنبال کردن شما را پذیرفت':' درخواست دنبال کردن شما را رد کرد'));res.json({ok:true});});
app.post('/api/users/:id/message-request',requireAuth,(req,res)=>{const id=Number(req.params.id),target=userById(id);if(!target||id===req.user.id)return res.status(400).json({error:'کاربر نامعتبر است'});if(blockedBetween(req.user.id,id))return res.status(403).json({error:'به‌دلیل مسدودسازی امکان ارسال درخواست وجود ندارد'});const pairClosed=db.prepare('SELECT closed_at FROM conversations WHERE user_a=? AND user_b=?').get(Math.min(req.user.id,id),Math.max(req.user.id,id));if(pairClosed?.closed_at)return closedConversationResponse(res);const friends=db.prepare("SELECT 1 FROM friend_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,id,id,req.user.id);if(friends){const c=conversationFor(req.user.id,id);if(conversationClosed(c))return closedConversationResponse(res);return res.json({ok:true,conversationId:c.id,user:safeUser(target)});}const pending=db.prepare("SELECT id FROM message_requests WHERE from_id=? AND to_id=? AND status='pending'").get(req.user.id,id);if(pending)return res.status(409).json({error:'درخواست پیام قبلاً ارسال شده و هنوز پاسخ داده نشده است'});const messagePass=!!(req.user.message_pass_until&&Date.parse(req.user.message_pass_until)>Date.now());const pro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());if(messagePass){/* پیام‌رسانی رایگان با اشتراک پیام */}else if(pro){const quotaError=consumeProQuota(req.user.id,true);if(quotaError)return res.status(429).json({error:quotaError});}else{const cost=Math.max(0,Number(getSetting('message_request_cost'))||0);if(req.user.coins<cost)return res.status(400).json({error:`برای درخواست پیام ${cost} سکه لازم است`});addTx(req.user.id,-cost,'message_request','هزینه درخواست پیام');}db.prepare("INSERT OR IGNORE INTO message_requests(from_id,to_id,status) VALUES(?,?,'pending')").run(req.user.id,id);const mr=db.prepare("SELECT id FROM message_requests WHERE from_id=? AND to_id=?").get(req.user.id,id);notify(id,'درخواست پیام',req.user.display_name+' درخواست گفتگو فرستاد',{action:'message_request',fromId:req.user.id,requestId:mr?.id});res.json({ok:true});});
app.post('/api/message-requests/:id/respond',requireAuth,(req,res)=>{const r=db.prepare("SELECT * FROM message_requests WHERE id=? AND to_id=? AND status='pending'").get(Number(req.params.id),req.user.id);if(!r)return res.status(404).json({error:'درخواست پیدا نشد'});if(rejectBlocked(req,res,r.from_id))return;if(req.body.accept!==true){db.prepare("UPDATE message_requests SET status='rejected' WHERE id=?").run(r.id);notify(r.from_id,'درخواست پیام رد شد',req.user.display_name+' درخواست پیام را رد کرد');return res.json({ok:true});}const c=conversationFor(req.user.id,r.from_id);if(conversationClosed(c))return closedConversationResponse(res);db.prepare("UPDATE message_requests SET status='accepted' WHERE id=?").run(r.id);notify(r.from_id,'درخواست پیام پذیرفته شد',req.user.display_name+' درخواست شما را پذیرفت');res.json({ok:true,conversationId:c.id,user:safeUser(userById(r.from_id))});});
app.post('/api/users/:id/unblock',requireAuth,(req,res)=>{const id=Number(req.params.id);if(!db.prepare('SELECT 1 FROM blocks WHERE user_id=? AND blocked_id=?').get(req.user.id,id))return res.json({ok:true});const pro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());const cost=pro?0:Math.max(0,Number(getSetting('unlock_cost'))||0);if(req.user.coins<cost)return res.status(400).json({error:`برای آنبلاک ${cost} سکه لازم است`});if(cost)addTx(req.user.id,-cost,'unblock','آنبلاک کاربر');db.prepare('DELETE FROM blocks WHERE user_id=? AND blocked_id=?').run(req.user.id,id);res.json({ok:true});});
app.post('/api/profile/avatar',requireAuth,enforceRestriction('profile'),upload.single('file'),(req,res)=>{if(!req.file||!req.file.mimetype.startsWith('image/'))return res.status(400).json({error:'یک عکس معتبر انتخاب کنید'});const url='/uploads/'+req.file.filename;db.prepare('UPDATE users SET avatar=? WHERE id=?').run(url,req.user.id);res.json({user:safeUser(userById(req.user.id)),avatar:url});});

// ACE CLUB posts: promoted feed, reward feed, comments, reports, and creator-owned deletion.
const postSelect = `SELECT p.*,u.username,u.display_name,u.avatar,
 (SELECT COUNT(*) FROM post_views v WHERE v.post_id=p.id) AS views_count,
 (SELECT COUNT(*) FROM post_likes l WHERE l.post_id=p.id) AS likes_count,
 (SELECT COUNT(*) FROM post_comments c WHERE c.post_id=p.id AND c.status='active') AS comments_count
 FROM posts p JOIN users u ON u.id=p.user_id`;
function postRow(id, viewerId=0){
 const p=db.prepare(postSelect+' WHERE p.id=?').get(Number(id));
 if(!p)return null;
 p.liked=!!db.prepare('SELECT 1 FROM post_likes WHERE post_id=? AND user_id=?').get(p.id,Number(viewerId)||0);
 p.reward_received=!!db.prepare('SELECT 1 FROM post_reward_claims WHERE post_id=? AND user_id=?').get(p.id,Number(viewerId)||0);
 p.owner=Number(p.user_id)===Number(viewerId);
 const colorId=db.prepare("SELECT item_id FROM user_cosmetic_loadout WHERE user_id=? AND kind='namecolor'").get(p.user_id)?.item_id;
 p.equipped_name_color=(typeof ACE_COSMETICS!=='undefined'?ACE_COSMETICS.find(x=>x.id===colorId)?.css:null)||'';
 return p;
}
function recordPostView(postId, viewerId){
 const p=db.prepare('SELECT id,user_id,status,display_limit,promoted FROM posts WHERE id=?').get(Number(postId));
 if(!p||p.status!=='active'||!p.promoted||Number(p.user_id)===Number(viewerId))return false;
 const before=db.prepare('SELECT COUNT(*) n FROM post_views WHERE post_id=?').get(p.id).n;
 db.prepare('INSERT OR IGNORE INTO post_views(post_id,viewer_id) VALUES(?,?)').run(p.id,viewerId);
 const after=db.prepare('SELECT COUNT(*) n FROM post_views WHERE post_id=?').get(p.id).n;
 if(after>=Number(p.display_limit||50)){
  // Views only complete the promotion. Reward results are decided and paid at like time.
  // Never award a user later from view tracking, because that could contradict the instant win/loss message.
  db.prepare('UPDATE posts SET support_completed=1 WHERE id=?').run(p.id);
 }
 return after>before;
}
function issuePostReward(postId, userId){
 const p=db.prepare('SELECT * FROM posts WHERE id=?').get(Number(postId));
 if(!p||!p.reward_enabled||!p.promoted||!p.reward_budget||!p.reward_winners)return false;
 if(db.prepare('SELECT 1 FROM post_reward_claims WHERE post_id=? AND user_id=?').get(p.id,userId))return false;
 const current=db.prepare('SELECT COUNT(*) n FROM post_reward_claims WHERE post_id=?').get(p.id).n;
 if(current>=p.reward_winners)return false;
 const amount=10;
 db.prepare('INSERT OR IGNORE INTO post_reward_claims(post_id,user_id,amount) VALUES(?,?,?)').run(p.id,userId,amount);
 const claim=db.prepare('SELECT id FROM post_reward_claims WHERE post_id=? AND user_id=?').get(p.id,userId);
 if(!claim)return false;
 addTx(userId,amount,'post_like_reward','جایزه لایک پست #'+p.post_code);
 notify(userId,'جایزه سکه گرفتی!','برای لایک پست، ۱۰ سکه جایزه گرفتی.',{action:'post_reward',requestId:p.id});
 const now=db.prepare('SELECT COUNT(*) n FROM post_reward_claims WHERE post_id=?').get(p.id).n;
 if(now>=p.reward_winners)db.prepare('UPDATE posts SET reward_distributed=1 WHERE id=?').run(p.id);
 return true;
}
app.get('/api/posts',requireAuth,(req,res)=>{
 const mode=String(req.query.mode||'explore'); const limit=Math.min(100,Math.max(1,Number(req.query.limit)||18));
 let rows=[];
 if(mode==='own'||mode==='profile'){
  const uid=mode==='own'?Number(req.user.id):(Number(req.query.userId)||Number(req.user.id));
  rows=db.prepare(postSelect+` WHERE p.user_id=? AND p.status ${mode==='profile'&&uid!==req.user.id?"= 'active'":"IN ('active','hidden')"} ORDER BY p.created_at DESC LIMIT ?`).all(uid,limit);
 } else if(mode==='rewards'){
  rows=db.prepare(postSelect+` WHERE p.status='active' AND p.promoted=1 AND p.reward_enabled=1 AND p.reward_distributed=0 AND (SELECT COUNT(*) FROM post_reward_claims rc WHERE rc.post_id=p.id)<p.reward_winners ORDER BY RANDOM() LIMIT ?`).all(limit);
 } else {
  rows=db.prepare(postSelect+` WHERE p.status='active' AND p.promoted=1 AND p.support_completed=0 AND p.user_id<>? AND (SELECT COUNT(*) FROM post_views v WHERE v.post_id=p.id)<p.display_limit AND NOT EXISTS(SELECT 1 FROM blocks b WHERE (b.user_id=? AND b.blocked_id=p.user_id) OR (b.user_id=p.user_id AND b.blocked_id=?)) ORDER BY RANDOM() LIMIT ?`).all(req.user.id,req.user.id,req.user.id,limit);
 }
 rows=rows.map(p=>{const colorId=db.prepare("SELECT item_id FROM user_cosmetic_loadout WHERE user_id=? AND kind='namecolor'").get(p.user_id)?.item_id;return {...p,equipped_name_color:(typeof ACE_COSMETICS!=='undefined'?ACE_COSMETICS.find(x=>x.id===colorId)?.css:null)||'',liked:!!db.prepare('SELECT 1 FROM post_likes WHERE post_id=? AND user_id=?').get(p.id,req.user.id),reward_received:!!db.prepare('SELECT 1 FROM post_reward_claims WHERE post_id=? AND user_id=?').get(p.id,req.user.id),owner:Number(p.user_id)===req.user.id}});
 res.json({posts:rows,coins:Number(userById(req.user.id)?.coins||0)});
});
app.get('/api/posts/:id',requireAuth,(req,res)=>{
 const id=Number(req.params.id);let p=db.prepare('SELECT * FROM posts WHERE id=? OR post_code=?').get(Number.isFinite(id)?id:-1,String(req.params.id));
 if(!p)return res.status(404).json({error:'پست پیدا نشد'});
 if(p.status==='deleted'||(p.status!=='active'&&Number(p.user_id)!==req.user.id))return res.status(404).json({error:'این پست در دسترس نیست'});
 if(Number(p.user_id)!==req.user.id)recordPostView(p.id,req.user.id);
 p=postRow(p.id,req.user.id);
 const comments=db.prepare(`SELECT c.id,c.post_id,c.user_id,c.body,c.created_at,u.username,u.display_name,u.avatar FROM post_comments c JOIN users u ON u.id=c.user_id WHERE c.post_id=? AND c.status='active' ORDER BY c.id ASC LIMIT 300`).all(p.id).map(c=>{const colorId=db.prepare("SELECT item_id FROM user_cosmetic_loadout WHERE user_id=? AND kind='namecolor'").get(c.user_id)?.item_id;return {...c,equipped_name_color:(typeof ACE_COSMETICS!=='undefined'?ACE_COSMETICS.find(x=>x.id===colorId)?.css:null)||''}});
 res.json({post:p,comments,coins:Number(userById(req.user.id)?.coins||0)});
});
app.get('/api/posts/:id/adjacent',requireAuth,(req,res)=>{
 const current=db.prepare("SELECT id,user_id,created_at FROM posts WHERE (id=? OR post_code=?) AND status='active'").get(Number(req.params.id)||-1,String(req.params.id));
 if(!current)return res.status(404).json({error:'پست پیدا نشد'});
 const source=String(req.query.source||'global');let where="p.status='active'",args=[];
 if(source==='profile'){const uid=Number(req.query.userId)||Number(current.user_id);where+=' AND p.user_id=?';args.push(uid);}
 else if(source==='explore'){where+=" AND p.promoted=1 AND p.user_id<>? AND ((p.support_completed=0 AND (SELECT COUNT(*) FROM post_views v WHERE v.post_id=p.id)<p.display_limit) OR p.id=?)";args.push(req.user.id,current.id);}
 const posts=db.prepare(`SELECT p.id,p.post_code,p.user_id,p.created_at FROM posts p WHERE ${where} ORDER BY p.created_at DESC,p.id DESC LIMIT 500`).all(...args);
 const index=posts.findIndex(x=>Number(x.id)===Number(current.id));
 if(index<0)return res.json({previous:null,next:null});
 res.json({previous:index>0?posts[index-1]:null,next:index<posts.length-1?posts[index+1]:null});
});
app.post('/api/posts',requireAuth,enforceRestriction('posts'),postUpload.fields([{name:'file',maxCount:1},{name:'cover',maxCount:1}]),(req,res)=>{
 const body=String(req.body.body||'').trim().slice(0,2000);const f=req.files?.file?.[0],cover=req.files?.cover?.[0];
 const removeOversized=()=>{for(const x of [f,cover])if(x?.path){try{fs.unlinkSync(x.path)}catch{}}};
 if(f){const mime=String(f.mimetype||'').toLowerCase(),ext=path.extname(String(f.originalname||f.filename||'')).toLowerCase();const isVideo=mime.startsWith('video/')||['.mp4','.m4v','.webm','.mov','.3gp','.3gpp','.mkv','.avi','.mpeg','.mpg','.ogv'].includes(ext);const max=isVideo?2*1024*1024:1024*1024;if(Number(f.size)>=max){removeOversized();return res.status(413).json({error:isVideo?'حجم ویدئو باید کمتر از ۲ مگابایت باشد':'حجم عکس باید کمتر از ۱ مگابایت باشد'});}}
 if(cover&&Number(cover.size)>=1024*1024){removeOversized();return res.status(413).json({error:'حجم کاور عکس باید کمتر از ۱ مگابایت باشد'});}
 if(cover&&!String(cover.mimetype||'').toLowerCase().startsWith('image/')&&!['.jpg','.jpeg','.png','.webp','.gif','.heic','.heif','.avif','.bmp','.tif','.tiff'].includes(path.extname(String(cover.originalname||'')).toLowerCase())){for(const x of [f,cover])if(x?.path){try{fs.unlinkSync(x.path)}catch{}}return res.status(400).json({error:'کاور باید یک فایل تصویری باشد'});}
 const requestId=String(req.body.requestId||'').trim().slice(0,80);
 const cleanupUpload=()=>{for(const x of [f,cover])if(x?.path){try{fs.unlinkSync(x.path)}catch{}}};
 if(!body&&!f){cleanupUpload();return res.status(400).json({error:'متن یا فایل عکس/ویدئو انتخاب کن'});}
 const selectedIsVideo=!!f&&(String(f.mimetype||'').toLowerCase().startsWith('video/')||['.mp4','.m4v','.webm','.mov','.3gp','.3gpp','.mkv','.avi','.mpeg','.mpg'].includes(path.extname(String(f.originalname||f.filename||'')).toLowerCase()));
 if((selectedIsVideo||!body)&&!cover){cleanupUpload();return res.status(400).json({error:'برای انتشار ویدئو یا پست بدون متن، انتخاب کاور الزامی است.'});}
 // Always ensure this table exists before any route query, including older databases
 // and requests that arrive without an idempotency key. CREATE IF NOT EXISTS is additive.
 db.exec('CREATE TABLE IF NOT EXISTS post_creation_requests(user_id INTEGER NOT NULL,request_id TEXT NOT NULL,post_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,request_id))');
 if(requestId){
  const previous=db.prepare('SELECT post_id FROM post_creation_requests WHERE user_id=? AND request_id=?').get(req.user.id,requestId);
  if(previous){cleanupUpload();const existing=postRow(previous.post_id,req.user.id);if(existing)return res.json({ok:true,duplicate:true,post:existing,url:'https://aceclub.ir/p/'+existing.post_code});}
 }
 const todayCount=Number(db.prepare("SELECT COUNT(*) n FROM posts WHERE user_id=? AND created_at>=datetime('now','start of day') AND status<>'deleted'").get(req.user.id)?.n||0);
 if(todayCount>=10){cleanupUpload();return res.status(429).json({error:'سهمیه امروزت کامل شده؛ روزانه حداکثر ۱۰ پست می‌توانی منتشر کنی.'});}
 const code=String(crypto.randomInt(10000000,99999999));const fMime=String(f?.mimetype||'').toLowerCase(),fExt=path.extname(String(f?.originalname||f?.filename||'')).toLowerCase();const mediaType=f?((fMime.startsWith('video/')||['.mp4','.m4v','.webm','.mov','.3gp','.3gpp','.mkv','.avi','.mpeg','.mpg'].includes(fExt))?'video':'image'):'';const mediaUrl=f?'/uploads/'+f.filename:'';
 const coverUrl=cover?'/uploads/'+cover.filename:'';const result=db.prepare('INSERT INTO posts(post_code,user_id,body,media_url,media_type,cover_url,allow_comments,allow_download) VALUES(?,?,?,?,?,?,?,?)').run(code,req.user.id,body,mediaUrl,mediaType,coverUrl,([false,0,'0','false'].includes(req.body.allowComments)?0:1),([false,0,'0','false'].includes(req.body.allowDownload)?0:1));
 if(requestId)db.prepare('INSERT OR IGNORE INTO post_creation_requests(user_id,request_id,post_id) VALUES(?,?,?)').run(req.user.id,requestId,result.lastInsertRowid);
 res.json({ok:true,post:postRow(result.lastInsertRowid,req.user.id),url:'https://aceclub.ir/p/'+code});
});
app.patch('/api/posts/:id',requireAuth,(req,res)=>{const id=Number(req.params.id);const p=db.prepare('SELECT * FROM posts WHERE id=?').get(id);if(!p||p.status==='deleted')return res.status(404).json({error:'پست پیدا نشد'});if(Number(p.user_id)!==Number(req.user.id))return res.status(403).json({error:'فقط صاحب پست می‌تواند آن را ویرایش کند'});const body=String(req.body.body??'').trim();if(body.length>2000)return res.status(400).json({error:'متن پست حداکثر ۲۰۰۰ نویسه است'});if(!body&&!p.media_url)return res.status(400).json({error:'پست باید متن یا رسانه داشته باشد'});db.prepare('UPDATE posts SET body=? WHERE id=? AND user_id=?').run(body,id,req.user.id);logActivity(req.user.id,req.user.id,'post_edited',`ویرایش متن پست #${p.post_code}`);res.json({ok:true,post:postRow(id,req.user.id)});});
app.post('/api/posts/:id/support',requireAuth,(req,res)=>{
 const id=Number(req.params.id);const p=db.prepare('SELECT * FROM posts WHERE id=? AND user_id=?').get(id,req.user.id);
 if(!p||p.status!=='active')return res.status(404).json({error:'پست پیدا نشد'});
 if(p.promoted||p.support_completed)return res.status(409).json({error:'این پست قبلاً حمایت شده است'});
 const rewardEnabled=[true,1,'1','true','on'].includes(req.body.rewardEnabled);const budget=rewardEnabled?Number(req.body.rewardBudget||100):0;
 const presets={50:5,100:10,200:20};if(rewardEnabled&&!presets[budget])return res.status(400).json({error:'بودجه جایزه باید ۵۰، ۱۰۰ یا ۲۰۰ سکه باشد'});
 const total=50+budget;if(Number(req.user.coins)<total)return res.status(400).json({error:`برای این حمایت ${total} سکه لازم داری`});
 atomic(()=>{addTx(req.user.id,-50,'post_promotion','حمایت پایه از پست #'+p.post_code);if(rewardEnabled)addTx(req.user.id,-budget,'post_reward_pool','بودجه جایزه پست #'+p.post_code);db.prepare('UPDATE posts SET promoted=1,reward_enabled=?,reward_budget=?,reward_winners=?,display_limit=50,support_completed=0 WHERE id=?').run(rewardEnabled?1:0,budget,rewardEnabled?presets[budget]:0,id);});
 res.json({ok:true,post:postRow(id,req.user.id),charged:total,coins:Number(userById(req.user.id)?.coins||0)});
});
app.post('/api/posts/:id/view',requireAuth,(req,res)=>{const id=Number(req.params.id),p=db.prepare("SELECT * FROM posts WHERE id=? AND status='active' AND promoted=1").get(id);if(!p)return res.status(404).json({error:'پست حمایت‌شده پیدا نشد'});recordPostView(id,req.user.id);res.json({ok:true,post:postRow(id,req.user.id),coins:Number(userById(req.user.id)?.coins||0)});});
app.get('/api/posts/:id/insights',requireAuth,(req,res)=>{const id=Number(req.params.id),p=db.prepare('SELECT id,user_id FROM posts WHERE id=?').get(id);if(!p)return res.status(404).json({error:'پست پیدا نشد'});if(Number(p.user_id)!==Number(req.user.id))return res.status(403).json({error:'فقط صاحب پست می‌تواند آمار آن را ببیند'});const isPro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());const views=db.prepare('SELECT v.created_at,u.id,u.username,u.display_name,u.avatar FROM post_views v JOIN users u ON u.id=v.viewer_id WHERE v.post_id=? ORDER BY v.id DESC LIMIT 100').all(id);const likes=db.prepare('SELECT l.created_at,u.id,u.username,u.display_name,u.avatar FROM post_likes l JOIN users u ON u.id=l.user_id WHERE l.post_id=? ORDER BY l.id DESC LIMIT 100').all(id);res.json({viewsCount:views.length,likesCount:likes.length,views,likes:isPro?likes.map(x=>({...x})):[],isPro,locked:!isPro});});
app.post('/api/posts/:id/like',requireAuth,(req,res)=>{
 const id=Number(req.params.id);
 // Declare in route scope so every path has a defined reward result.
 let rewardResult='not_applicable';
 try {
  // Additive self-healing for databases created by older releases.
  db.exec('CREATE TABLE IF NOT EXISTS post_likes(id INTEGER PRIMARY KEY AUTOINCREMENT,post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(post_id,user_id))');
  db.exec('CREATE TABLE IF NOT EXISTS post_fame_like_awards(post_id INTEGER NOT NULL,from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(post_id,from_id))');
  db.exec('CREATE TABLE IF NOT EXISTS post_reward_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,amount INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(post_id,user_id))');
  db.exec('CREATE TABLE IF NOT EXISTS post_reward_draws(post_id INTEGER NOT NULL,user_id INTEGER NOT NULL,won INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(post_id,user_id))');
  const p=db.prepare("SELECT * FROM posts WHERE id=? AND status='active'").get(id);
  if(!p)return res.status(404).json({error:'پست پیدا نشد'});
  if(Number(p.user_id)===Number(req.user.id))return res.status(400).json({error:'نمی‌توانی پست خودت را لایک کنی'});
  if(db.prepare('SELECT 1 FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(req.user.id,p.user_id,p.user_id,req.user.id))return res.status(403).json({error:'تعامل با این کاربر ممکن نیست'});
  const existing=db.prepare('SELECT id FROM post_likes WHERE post_id=? AND user_id=?').get(id,req.user.id);
  if(existing){
   db.prepare('DELETE FROM post_likes WHERE post_id=? AND user_id=?').run(id,req.user.id);
   return res.json({ok:true,liked:false,rewardResult:'not_applicable',post:postRow(id,req.user.id),coins:Number(userById(req.user.id)?.coins||0)});
  }
  db.prepare('INSERT OR IGNORE INTO post_likes(post_id,user_id) VALUES(?,?)').run(id,req.user.id);
  // Fame is independent from the reward draw. A reward failure must never cancel a like or fame award.
  let fameAwarded=false;
  let fameAwardRowInserted=false;
  try {
   const award=db.prepare('INSERT OR IGNORE INTO post_fame_like_awards(post_id,from_id,to_id) VALUES(?,?,?)').run(id,req.user.id,Number(p.user_id));
   fameAwardRowInserted=Number(award.changes)>0;
   if(fameAwardRowInserted){
    const owner=userById(Number(p.user_id));
    if(!owner)throw new Error('Post owner account not found');
    const current=Math.max(0,Number(owner.fame)||0,Number(owner.xp)||0);
    syncUserFameAndLevel(Number(p.user_id),{fame:current+500});
    fameAwarded=true;
   }
  } catch(fameErr) {
   // Do not leave a permanent award marker if the balance update failed; a later retry can repair it.
   if(fameAwardRowInserted){try{db.prepare('DELETE FROM post_fame_like_awards WHERE post_id=? AND from_id=?').run(id,req.user.id);}catch(rollbackErr){console.error('Post like fame marker rollback failed:',rollbackErr);}}
   console.error('Post like fame award failed:',fameErr);
  }
  if(fameAwarded){try{notify(p.user_id,'۵۰۰ شهرت بابت لایک پست','یکی از کاربران پستت را لایک کرد؛ ۵۰۰ شهرت به حسابت اضافه شد.',{action:'post_like',requestId:id});}catch(notifyErr){console.error('Post like fame notification failed:',notifyErr);}}
  rewardResult='not_applicable';
  if(Number(p.reward_enabled)===1&&Number(p.promoted)===1){
   try {
    const priorDraw=db.prepare('SELECT won FROM post_reward_draws WHERE post_id=? AND user_id=?').get(id,req.user.id);
    if(priorDraw){rewardResult=Number(priorDraw.won)?'won':'lost';}
    else {
     const claims=Number(db.prepare('SELECT COUNT(*) n FROM post_reward_claims WHERE post_id=?').get(id).n||0);
     const draws=Number(db.prepare('SELECT COUNT(*) n FROM post_reward_draws WHERE post_id=?').get(id).n||0);
     const remaining=Math.max(0,Number(p.reward_winners)-claims);
     const opportunities=Math.max(1,Number(p.display_limit||50)-draws);
     const chance=remaining>0?Math.min(1,remaining/opportunities):0;
     const won=remaining>0&&Math.random()<chance;
     db.prepare('INSERT OR IGNORE INTO post_reward_draws(post_id,user_id,won) VALUES(?,?,?)').run(id,req.user.id,won?1:0);
     rewardResult=won&&issuePostReward(id,req.user.id)?'won':'lost';
    }
   } catch(rewardErr) { console.error('Post like reward draw failed:',rewardErr); rewardResult='unavailable'; }
  }
  res.json({ok:true,liked:true,rewardResult,post:postRow(id,req.user.id),coins:Number(userById(req.user.id)?.coins||0)});
 } catch(err) {
  console.error('Post like request failed:',err);
  return res.status(500).json({error:'ثبت لایک انجام نشد؛ لطفاً دوباره تلاش کن'});
 }
});
app.post('/api/posts/:id/comments',requireAuth,enforceRestriction('comments'),(req,res)=>{
 const id=Number(req.params.id),body=String(req.body.body||'').trim().slice(0,1000);const p=db.prepare("SELECT * FROM posts WHERE id=? AND status='active'").get(id);if(!p)return res.status(404).json({error:'پست پیدا نشد'});if(!p.allow_comments)return res.status(403).json({error:'نظر دادن برای این پست غیرفعال است'});if(!body)return res.status(400).json({error:'متن نظر را بنویس'});if(Number(p.user_id)!==req.user.id&&db.prepare('SELECT 1 FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(req.user.id,p.user_id,p.user_id,req.user.id))return res.status(403).json({error:'امکان نظر دادن وجود ندارد'});
 const r=db.prepare('INSERT INTO post_comments(post_id,user_id,body) VALUES(?,?,?)').run(id,req.user.id,body);if(Number(p.user_id)!==req.user.id)notify(p.user_id,'نظر جدید روی پست','برای یکی از پست‌هایت نظر جدید ثبت شد.',{action:'post_comment',requestId:id});const comment=db.prepare('SELECT c.*,u.username,u.display_name,u.avatar FROM post_comments c JOIN users u ON u.id=c.user_id WHERE c.id=?').get(r.lastInsertRowid);res.json({ok:true,comment});
});
app.post('/api/posts/:id/report',requireAuth,(req,res)=>{const id=Number(req.params.id),reason=String(req.body.reason||'').trim().slice(0,200);const p=db.prepare('SELECT * FROM posts WHERE id=?').get(id);if(!p||Number(p.user_id)===req.user.id)return res.status(404).json({error:'پست برای گزارش معتبر نیست'});if(!reason)return res.status(400).json({error:'علت گزارش را انتخاب کن'});const dupe=db.prepare("SELECT id FROM reports WHERE reporter_id=? AND target_type='post' AND target_id=? AND status='new'").get(req.user.id,String(id));if(dupe)return res.status(409).json({error:'این پست را قبلاً گزارش کرده‌ای'});db.prepare("INSERT INTO reports(reporter_id,reported_user_id,category,target_type,target_id,reason,details) VALUES(?,?,'post','post',?,?,?)").run(req.user.id,p.user_id,String(id),reason,'پست #'+p.post_code+' · '+String(p.body||'').slice(0,300));const count=db.prepare("SELECT COUNT(DISTINCT reporter_id) n FROM reports WHERE target_type='post' AND target_id=? AND status IN ('new','reviewing')").get(String(id)).n;const views=db.prepare('SELECT COUNT(*) n FROM post_views WHERE post_id=?').get(id).n;const threshold=Math.max(2,Math.ceil(views/2));let hidden=false;if(views>0&&count>=threshold&&p.status==='active'){db.prepare("UPDATE posts SET status='hidden' WHERE id=?").run(id);notify(p.user_id,'پست شما از اکسپلور برداشته شد','به علت گزارش‌های زیاد، پست '+p.post_code+' موقتاً از اکسپلور برداشته شد تا مدیر بررسی کند.',{action:'post_hidden',requestId:id});hidden=true;}res.json({ok:true,hidden});
});
app.delete('/api/post-comments/:id',requireAuth,(req,res)=>{const id=Number(req.params.id);const c=db.prepare('SELECT c.*,p.user_id post_owner FROM post_comments c JOIN posts p ON p.id=c.post_id WHERE c.id=?').get(id);if(!c)return res.status(404).json({error:'نظر پیدا نشد'});if(Number(c.user_id)!==req.user.id&&Number(c.post_owner)!==req.user.id&&!isAdmin(req.user))return res.status(403).json({error:'اجازه حذف این نظر را نداری'});db.prepare("UPDATE post_comments SET status='deleted' WHERE id=?").run(id);res.json({ok:true});});
app.post('/api/post-comments/:id/report',requireAuth,(req,res)=>{const id=Number(req.params.id),reason=String(req.body.reason||'').trim().slice(0,200);const c=db.prepare("SELECT c.*,p.user_id post_owner FROM post_comments c JOIN posts p ON p.id=c.post_id WHERE c.id=? AND c.status='active'").get(id);if(!c||Number(c.user_id)===req.user.id)return res.status(404).json({error:'نظر برای گزارش معتبر نیست'});if(!reason)return res.status(400).json({error:'علت گزارش را انتخاب کن'});const dupe=db.prepare("SELECT id FROM reports WHERE reporter_id=? AND target_type='comment' AND target_id=? AND status='new'").get(req.user.id,String(id));if(dupe)return res.status(409).json({error:'این نظر را قبلاً گزارش کرده‌ای'});db.prepare("INSERT INTO reports(reporter_id,reported_user_id,category,target_type,target_id,reason,details) VALUES(?,?, 'comment','comment',?,?,?)").run(req.user.id,c.user_id,String(id),reason,'نظر پست #'+c.post_id+' · '+c.body.slice(0,300));res.json({ok:true});
});
app.delete('/api/posts/:id',requireAuth,(req,res)=>{const id=Number(req.params.id),p=db.prepare('SELECT * FROM posts WHERE id=?').get(id);if(!p)return res.status(404).json({error:'پست پیدا نشد'});if(Number(p.user_id)!==req.user.id&&!isAdmin(req.user))return res.status(403).json({error:'فقط صاحب پست یا مدیر می‌تواند آن را حذف کند'});if(Number(p.user_id)===req.user.id&&p.promoted){const views=db.prepare('SELECT COUNT(*) n FROM post_views WHERE post_id=?').get(id).n;const remaining=Math.max(0,Number(p.display_limit)-views);const baseRefund=p.support_completed?0:Math.floor(50*remaining/Math.max(1,Number(p.display_limit)));const claims=db.prepare('SELECT COALESCE(SUM(amount),0) n FROM post_reward_claims WHERE post_id=?').get(id).n;const poolRefund=Math.max(0,Number(p.reward_budget)-Number(claims));const refund=baseRefund+poolRefund;if(refund>0)addTx(req.user.id,refund,'post_refund','بازپرداخت حذف پست #'+p.post_code+' · '+remaining+' نمایش باقی‌مانده');}atomic(()=>{const tables=db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(x=>x.name);for(const table of tables){if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)||table==='posts')continue;let cols=[];try{cols=db.prepare(`PRAGMA table_info("${table}")`).all().map(c=>c.name)}catch{}if(cols.includes('post_id'))db.prepare(`DELETE FROM "${table}" WHERE post_id=?`).run(id);}db.prepare('DELETE FROM posts WHERE id=?').run(id);});if(p.media_url){const name=path.basename(p.media_url);try{fs.unlinkSync(path.join(uploadsDir,name))}catch{}}logActivity(req.user.id,p.user_id,'post_deleted',`حذف کامل پست #${p.post_code}`);res.json({ok:true,coins:Number(userById(req.user.id)?.coins||0)});});
app.get('/api/posts/:id/download',requireAuth,(req,res)=>{const id=Number(req.params.id);const p=db.prepare("SELECT * FROM posts WHERE id=? AND status='active'").get(id);if(!p||!p.media_url)return res.status(404).json({error:'فایل پست پیدا نشد'});if(!p.allow_download&&Number(p.user_id)!==req.user.id)return res.status(403).json({error:'دانلود این پست غیرفعال است'});res.json({url:p.media_url,filename:path.basename(p.media_url)});});
app.get('/api/users/promoted',requireAuth,(req,res)=>{const users=db.prepare("SELECT u.id,u.username,u.display_name,u.avatar,u.bio,u.level,u.boost_until FROM users u WHERE u.id<>? AND u.boost_until IS NOT NULL AND datetime(u.boost_until)>datetime('now') AND NOT EXISTS(SELECT 1 FROM blocks b WHERE (b.user_id=? AND b.blocked_id=u.id) OR (b.user_id=u.id AND b.blocked_id=?)) ORDER BY RANDOM() LIMIT 40").all(req.user.id,req.user.id,req.user.id);res.json({users});});

app.post('/api/chats/start',requireAuth,(req,res)=>{const id=Number(req.body.userId);if(!Number.isInteger(id)||id===req.user.id||!userById(id))return res.status(400).json({error:'کاربر نامعتبر است'});if(db.prepare('SELECT 1 FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(req.user.id,id,id,req.user.id))return res.status(403).json({error:'به‌دلیل مسدودسازی امکان گفتگو وجود ندارد'});const friend=db.prepare("SELECT 1 FROM friend_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,id,id,req.user.id);const accepted=db.prepare("SELECT 1 FROM message_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,id,id,req.user.id);const senderPro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());if(!friend&&!accepted&&!senderPro)return res.status(403).json({error:'ابتدا درخواست دنبال کردن یا درخواست پیام بفرستید'});const c=conversationFor(req.user.id,id);if(conversationClosed(c))return closedConversationResponse(res);const other=userById(id);res.json({conversationId:c.id,user:safeUser(other)});});
app.patch('/api/profile',requireAuth,enforceRestriction('profile'),(req,res)=>{const {display_name,bio,avatar,province}=req.body||{};db.prepare('UPDATE users SET display_name=?,bio=?,avatar=?,province=? WHERE id=?').run(String(display_name||req.user.display_name).slice(0,40),String(bio||'').slice(0,300),String(avatar===undefined?req.user.avatar:(avatar||'')).slice(0,500),String(province===undefined?req.user.province:(province||'')).slice(0,80),req.user.id);res.json({user:safeUser(userById(req.user.id))});});

app.get('/api/rewards/daily',requireAuth,(req,res)=>{const row=db.prepare('SELECT claimed_at FROM daily_presence_rewards WHERE user_id=?').get(req.user.id);const nowIran=iranClock(),today=iranKey(nowIran),claimedIran=row?iranKey(new Date(Number(row.claimed_at)*1000+210*60000)):'';const nextMidnight=Date.UTC(nowIran.getUTCFullYear(),nowIran.getUTCMonth(),nowIran.getUTCDate()+1)-210*60000;const remaining=claimedIran===today?Math.max(0,Math.ceil((nextMidnight-Date.now())/1000)):0;res.json({available:remaining===0,remainingSeconds:remaining,amount:100,fame:100});});
app.post('/api/rewards/daily/claim',requireAuth,(req,res)=>{const now=Math.floor(Date.now()/1000);const row=db.prepare('SELECT claimed_at FROM daily_presence_rewards WHERE user_id=?').get(req.user.id);const nowIran=iranClock(),today=iranKey(nowIran),claimedIran=row?iranKey(new Date(Number(row.claimed_at)*1000+210*60000)):'';if(claimedIran===today)return res.status(429).json({error:'جایزه ورود امروز را قبلاً دریافت کرده‌ای؛ فردا دوباره سر بزن'});db.prepare('INSERT INTO daily_presence_rewards(user_id,claimed_at) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET claimed_at=excluded.claimed_at').run(req.user.id,now);addTx(req.user.id,100,'presence_reward','جایزه ورود روزانه');syncUserFameAndLevel(req.user.id,{fame:Math.max(0,Number(userById(req.user.id)?.fame||0))+100});notify(req.user.id,'جایزه ورود روزانه','۱۰۰ سکه و ۱۰۰ شهرت جایزه ورود روزانه به حسابت اضافه شد.');res.json({ok:true,amount:100,fame:100,user:safeUser(userById(req.user.id))});});
app.get('/api/config',(req,res)=>res.json({rtc:{iceServers:[{urls:'stun:stun.l.google.com:19302'},...(cfg('TURN_URL')&&cfg('TURN_USERNAME')&&cfg('TURN_CREDENTIAL')?[{urls:cfg('TURN_URL'),username:cfg('TURN_USERNAME'),credential:cfg('TURN_CREDENTIAL')}]:[])]},appName:getSetting('app_name'),maintenance:getSetting('maintenance')==='1',economy:{pro_cost:Number(getSetting('pro_cost')),boost_cost:Number(getSetting('boost_cost')),unlock_cost:Number(getSetting('unlock_cost')),direct_cost:Number(getSetting('direct_cost')),message_request_cost:Number(getSetting('message_request_cost')),pro_chat_requests:Number(getSetting('pro_chat_requests')),pro_quota_8h:Number(getSetting('pro_quota_8h')),pro_quota_24h:Number(getSetting('pro_quota_24h')),pro_quota_7d:Number(getSetting('pro_quota_7d')),free_message_quota:Number(getSetting('free_message_quota')||10),diamond_coin_rate:Number(getSetting('diamond_coin_rate')),diamond_transfer_min:Number(getSetting('diamond_transfer_min')),level_diamond_reward:Number(getSetting('level_diamond_reward')),game_daily_reward_1:Number(getSetting('game_daily_reward_1')),game_daily_reward_2:Number(getSetting('game_daily_reward_2')),game_daily_reward_3:Number(getSetting('game_daily_reward_3')),coin_packs:[1,2,3,4].map(i=>({id:i,coins:Number(getSetting('coin_pack_'+i+'_coins')),diamonds:Number(getSetting('coin_pack_'+i+'_diamonds'))}))}}));
app.post('/api/auth/register', (req,res)=>{
 const {username,password,display_name,phone,invite_code,gender,age,province}=req.body||{};
 const phoneNormalized=String(phone||'').replace(/[۰-۹٠-٩]/g,d=>String('۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩'.indexOf(d)%10)).replace(/[\s()-]/g,'').slice(0,30)||null;
 if(req.body?.acceptedTerms!==true)return res.status(400).json({error:'برای ثبت‌نام باید قوانین و راهنما را مطالعه و تأیید کنید'});
 if(!/^[a-zA-Z0-9_]{3,24}$/.test(String(username||'')))return res.status(400).json({error:'نام کاربری ۳ تا ۲۴ حرف انگلیسی، عدد یا _ باشد'});
 if(String(password||'').length<8 || !/[A-Za-z]/.test(String(password||'')) || !/[0-9]/.test(String(password||'')))return res.status(400).json({error:'رمز عبور باید حداقل ۸ کاراکتر و شامل حداقل یک حرف انگلیسی و یک عدد باشد'});
 if(display_name!==undefined && (String(display_name).trim().length<2 || String(display_name).trim().length>40))return res.status(400).json({error:'نام نمایشی باید بین ۲ تا ۴۰ نویسه باشد'});
 if(String(username).toLowerCase()==='owner')return res.status(400).json({error:'این نام کاربری رزرو شده است؛ نام دیگری انتخاب کن'});
 if(userByName(username))return res.status(409).json({error:'این نام کاربری قبلاً گرفته شده'});
 if(invite_code){const inviter=userByName(String(invite_code).trim());if(!inviter)return res.status(400).json({error:'کد دعوت معتبر نیست'});if(String(inviter.username).toLowerCase()===String(username).toLowerCase())return res.status(400).json({error:'نمی‌توانی از کد دعوت خودت استفاده کنی'});}

 if(phone && db.prepare('SELECT id FROM users WHERE phone=?').get(phone))return res.status(409).json({error:'این شماره قبلاً ثبت شده'});
 const hash=bcrypt.hashSync(password,12);
 const priorReal=db.prepare("SELECT id FROM users WHERE username<>'OWNER' COLLATE NOCASE ORDER BY id LIMIT 1").get();
 const makeOwner=!priorReal;
 const r=db.prepare('INSERT INTO users(username,display_name,phone,password_hash,gender,age,province,coins,role) VALUES(?,?,?,?,?,?,?,?,?)').run(username,String(display_name||username).slice(0,40),phoneNormalized,hash,gender||'نامشخص',Math.max(18,Math.min(99,Number(age)||18)),String(province||'').slice(0,50),500,makeOwner?'owner':'user');
 const uid=Number(r.lastInsertRowid);if(makeOwner)db.prepare("UPDATE users SET role='user' WHERE id<>? AND role IN ('owner','creator')").run(uid);if(makeOwner)db.prepare("UPDATE chat_groups SET owner_id=? WHERE name='گروه ایس کلاب'").run(uid);
 let official=db.prepare("SELECT id FROM chat_groups WHERE group_code='ACECLUB_OFFICIAL' OR name='گروه ایس کلاب' ORDER BY CASE WHEN group_code='ACECLUB_OFFICIAL' THEN 0 ELSE 1 END,id LIMIT 1").get();if(!official&&makeOwner){const gr=db.prepare('INSERT INTO chat_groups(name,group_code,description,owner_id) VALUES(?,?,?,?)').run('گروه ایس کلاب','ACECLUB_OFFICIAL','گروه همگانی رسمی ایس کلاب',uid);official={id:Number(gr.lastInsertRowid)};}if(official)db.prepare('INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,?)').run(official.id,uid,makeOwner?'owner':'member');if(makeOwner&&official){db.prepare("UPDATE chat_group_members SET role='member' WHERE group_id=? AND user_id<>? AND role='owner'").run(official.id,uid);db.prepare("UPDATE chat_group_members SET role='owner' WHERE group_id=? AND user_id=?").run(official.id,uid);}db.prepare('UPDATE users SET terms_accepted_at=CURRENT_TIMESTAMP WHERE id=?').run(uid);logActivity(uid,uid,'register',makeOwner?'ثبت‌نام؛ اولین حساب و سازنده سایت':'ثبت‌نام و تأیید قوانین');addTx(uid,0,'signup','ثبت نام');
 if(invite_code){const inviter=userByName(String(invite_code).trim());if(inviter&&inviter.id!==uid){const referral=db.prepare('INSERT OR IGNORE INTO referrals(inviter_id,invitee_id) VALUES(?,?)').run(inviter.id,uid);if(referral.changes){addTx(inviter.id,500,'referral','پاداش دعوت دوست '+username);addTx(uid,250,'referral_welcome','پاداش خوش‌آمدگویی از دعوت '+inviter.username);notify(inviter.id,'پاداش دعوت','۵۰۰ سکه به‌خاطر دعوت دوستت به حسابت اضافه شد.');notify(uid,'پاداش دعوت','۲۵۰ سکه به‌خاطر دعوت شدن به ایس کلاب دریافت کردی.');}}}
 recordLoginDay(uid);req.session.uid=uid;res.json({user:safeUser(userById(uid))});
});
app.get('/api/auth/device-accounts',(req,res)=>{const did=String(req.get('X-ACE-Device-ID')||'').trim().slice(0,100);if(!did||!/^[A-Za-z0-9._:-]+$/.test(did))return res.json({accounts:[]});const accounts=db.prepare("SELECT id,username,display_name,avatar FROM users WHERE device_id=? AND is_banned=0 ORDER BY last_seen DESC,id DESC LIMIT 10").all(did);res.json({accounts});});
app.post('/api/auth/device-login',(req,res)=>{const did=String(req.get('X-ACE-Device-ID')||'').trim().slice(0,100),id=Number(req.body.userId);if(!did||!/^[A-Za-z0-9._:-]+$/.test(did))return res.status(400).json({error:'شناسهٔ دستگاه معتبر نیست'});const u=db.prepare('SELECT * FROM users WHERE id=? AND device_id=?').get(id,did);if(!u)return res.status(403).json({error:'این حساب به شناسهٔ همین دستگاه متصل نیست'});if(u.is_banned)return res.status(403).json({error:'این حساب مسدود است'});req.session.uid=u.id;db.prepare('UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE id=?').run(u.id);recordLoginDay(u.id);logActivity(u.id,u.id,'device_quick_login','ورود سریع از فهرست حساب‌های همین دستگاه');res.json({user:safeUser(userById(u.id))});});
app.post('/api/auth/login',(req,res)=>{const {username,password}=req.body||{};const u=userByName(String(username||''));if(!u||!bcrypt.compareSync(String(password||''),u.password_hash))return res.status(400).json({error:'نام کاربری یا رمز عبور اشتباه است'});if(u.is_banned && u.ban_until && Date.parse(u.ban_until)<=Date.now()){db.prepare('UPDATE users SET is_banned=0,ban_reason=NULL,ban_until=NULL WHERE id=?').run(u.id);u=userById(u.id);}if(u.is_banned)return res.status(403).json({error:u.ban_reason?`حساب شما مسدود شده است: ${u.ban_reason}`:'حساب شما مسدود شده است'});req.session.uid=u.id;db.prepare('UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE id=?').run(u.id);recordLoginDay(u.id);logActivity(u.id,u.id,'login','ورود به حساب');res.json({user:safeUser(userById(u.id))});});
app.post('/api/auth/logout',requireAuth,(req,res)=>{if(!String(req.user.phone||'').trim())return res.status(400).json({error:'برای خروج از حساب، ابتدا شماره تلفن خود را در بخش امنیت ثبت کنید'});req.session.destroy(err=>{if(err)return res.status(500).json({error:'خروج از حساب انجام نشد'});res.clearCookie('dor.sid',{httpOnly:true,sameSite:'lax',secure:process.env.NODE_ENV==='production'});res.json({ok:true});});});
app.get('/api/security/status',requireAuth,(req,res)=>res.json({hasPhone:!!req.user.phone,maskedPhone:req.user.phone?String(req.user.phone).replace(/.(?=.{4})/g,'•'):''}));
app.patch('/api/security/phone',requireAuth,(req,res)=>{const phone=String(req.body.phone||'').trim();if(!/^\+?[0-9۰-۹٠-٩][0-9۰-۹٠-٩\s()-]{7,19}$/.test(phone))return res.status(400).json({error:'شماره تلفن معتبر وارد کنید'});const normalized=phone.replace(/[\s()-]/g,'');const exists=db.prepare('SELECT id FROM users WHERE phone=? AND id<>?').get(normalized,req.user.id);if(exists)return res.status(409).json({error:'این شماره قبلاً برای حساب دیگری ثبت شده است'});db.prepare('UPDATE users SET phone=? WHERE id=?').run(normalized,req.user.id);res.json({ok:true,maskedPhone:normalized.replace(/.(?=.{4})/g,'•')});});
app.post('/api/security/password',requireAuth,(req,res)=>{const {currentPassword,newPassword,phone}=req.body||{};const u=userById(req.user.id);if(!u.phone)return res.status(400).json({error:'ابتدا شماره تلفن خود را ثبت کنید'});if(String(phone||'').replace(/[\s()-]/g,'')!==String(u.phone).replace(/[\s()-]/g,''))return res.status(400).json({error:'شماره تلفن با شماره ثبت‌شده مطابقت ندارد'});if(!bcrypt.compareSync(String(currentPassword||''),u.password_hash))return res.status(400).json({error:'رمز عبور فعلی درست نیست'});if(String(newPassword||'').length<8)return res.status(400).json({error:'رمز جدید باید حداقل ۸ کاراکتر باشد'});if(String(newPassword)===String(currentPassword))return res.status(400).json({error:'رمز جدید باید با رمز فعلی متفاوت باشد'});db.prepare('UPDATE users SET password_hash=? WHERE id=?').run(bcrypt.hashSync(String(newPassword),12),u.id);res.json({ok:true});});
app.get('/api/me',requireAuth,(req,res)=>{syncUserFameAndLevel(req.user.id);db.prepare('UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE id=?').run(req.user.id);res.json({user:safeUser(userById(req.user.id))});});
app.get('/api/explore',requireAuth,(req,res)=>{const q=String(req.query.q||'').slice(0,50);const viewerPro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());const users=db.prepare(`SELECT u.*,(SELECT COUNT(*) FROM likes l WHERE l.to_id=u.id) likes FROM users u WHERE u.id!=? AND u.is_banned=0 AND ((u.pro_until IS NOT NULL AND datetime(u.pro_until)>datetime('now')) OR (u.boost_until IS NOT NULL AND datetime(u.boost_until)>datetime('now'))) AND (u.username LIKE ? OR u.display_name LIKE ?) ORDER BY CASE WHEN (u.boost_until IS NOT NULL AND datetime(u.boost_until)>datetime('now')) THEN 0 ELSE 1 END, RANDOM() LIMIT 10`).all(req.user.id,'%'+q+'%','%'+q+'%').map(u=>({...safeUser(u),age:viewerPro?u.age:null,province:viewerPro?u.province:'',likes:u.likes,boosted:!!(u.boost_until&&Date.parse(u.boost_until)>Date.now()),pro:!!(u.pro_until&&Date.parse(u.pro_until)>Date.now()),liked:!!db.prepare('SELECT 1 FROM likes WHERE from_id=? AND to_id=?').get(req.user.id,u.id),following:!!db.prepare('SELECT 1 FROM follows WHERE follower_id=? AND followed_id=?').get(req.user.id,u.id)}));res.json({users});});
app.get('/api/likes/incoming',requireAuth,(req,res)=>{const pro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());if(!pro)return res.status(403).json({error:'مشاهده افرادی که لایک کرده‌اند مخصوص اشتراک پرو است'});const users=db.prepare('SELECT u.* FROM likes l JOIN users u ON u.id=l.from_id WHERE l.to_id=? ORDER BY l.id DESC LIMIT 100').all(req.user.id).map(safeUser);res.json({users});});
app.get('/api/active-users',requireAuth,(req,res)=>{const onlineIds=[];for(const [room,sockets] of io.sockets.adapter.rooms){if(room.startsWith('user:')&&sockets.size)onlineIds.push(Number(room.slice(5)));}const ids=[...new Set(onlineIds)].filter(id=>id!==req.user.id);if(!ids.length)return res.json({users:[]});const placeholders=ids.map(()=>'?').join(',');const users=db.prepare(`SELECT * FROM users WHERE id IN (${placeholders}) AND is_banned=0 ORDER BY CASE WHEN (pro_until IS NOT NULL AND datetime(pro_until)>datetime('now')) OR (boost_until IS NOT NULL AND datetime(boost_until)>datetime('now')) THEN (ABS(RANDOM()) % 3) ELSE (3 + ABS(RANDOM()) % 3) END, RANDOM() LIMIT 10`).all(...ids).map(u=>({...safeUser(u),pro:!!(u.pro_until&&Date.parse(u.pro_until)>Date.now()),boosted:!!(u.boost_until&&Date.parse(u.boost_until)>Date.now())}));res.json({users});});
app.get('/api/friends',requireAuth,(req,res)=>{const users=db.prepare("SELECT u.* FROM users u WHERE u.id!=? AND u.is_banned=0 AND u.boost_until IS NOT NULL AND datetime(u.boost_until)>datetime('now') ORDER BY datetime(u.boost_until) DESC LIMIT 100").all(req.user.id).map(u=>{const v=safeUser(u);if(!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now())){v.age=null;v.province='';}return v;});res.json({users});});
app.get('/api/follows/:kind',requireAuth,(req,res)=>{const kind=String(req.params.kind);if(!['followers','following'].includes(kind))return res.status(400).json({error:'نوع فهرست نامعتبر است'});const column=kind==='followers'?'followed_id':'follower_id',other=kind==='followers'?'follower_id':'followed_id';const users=db.prepare(`SELECT u.id,u.username,u.display_name,u.avatar,u.level FROM follows f JOIN users u ON u.id=f.${other} WHERE f.${column}=? AND u.is_banned=0 ORDER BY f.rowid DESC LIMIT 5000`).all(req.user.id);res.json({users});});
app.delete('/api/users/:id/follow',requireAuth,(req,res)=>{const id=Number(req.params.id);const result=db.prepare('DELETE FROM follows WHERE follower_id=? AND followed_id=?').run(req.user.id,id);res.json({ok:true,unfollowed:!!result.changes});});
app.post('/api/users/:id/follow',requireAuth,(req,res)=>{const id=Number(req.params.id);if(id===req.user.id)return res.status(400).json({error:'نمی‌توانید خودتان را دنبال کنید'});db.prepare('INSERT OR IGNORE INTO follows(follower_id,followed_id) VALUES(?,?)').run(req.user.id,id);notify(id,'دنبال‌کننده جدید',req.user.display_name+' شما را دنبال کرد');res.json({ok:true});});
app.post('/api/users/:id/like',requireAuth,(req,res)=>{const id=Number(req.params.id);if(id===req.user.id)return res.status(400).json({error:'نمی‌توانید خودتان را لایک کنید'});const existing=db.prepare('SELECT id FROM likes WHERE from_id=? AND to_id=?').get(req.user.id,id);if(existing)db.prepare('DELETE FROM likes WHERE id=?').run(existing.id);else{db.prepare('INSERT OR IGNORE INTO likes(from_id,to_id) VALUES(?,?)').run(req.user.id,id);const award=db.prepare('INSERT OR IGNORE INTO fame_like_awards(from_id,to_id) VALUES(?,?)').run(req.user.id,id);if(award.changes)syncUserFameAndLevel(id,{fame:Math.max(0,Number(userById(id)?.fame||0),Number(userById(id)?.xp||0))+500});notify(id,'لایک جدید',req.user.display_name+' پروفایل شما را پسندید',{action:'like',fromId:req.user.id});}res.json({liked:!existing});});
app.get('/api/stats',requireAuth,(req,res)=>{const uid=req.user.id;const days=Math.max(1,Math.min(365,Number(req.query.days)||30));const since=`-${days} days`;const count=(sql,...args)=>db.prepare(sql).get(...args).n||0;const visits=count('SELECT COUNT(*) n FROM notifications WHERE user_id=? AND title=? AND created_at>=datetime("now",?)',uid,'بازدید پروفایل',since);const viewed=count('SELECT COUNT(*) n FROM notifications WHERE user_id=? AND title=? AND created_at>=datetime("now",?)',uid,'پروفایل بازدید شد',since);const likesIn=count('SELECT COUNT(*) n FROM likes WHERE to_id=? AND created_at>=datetime("now",?)',uid,since);const likesOut=count('SELECT COUNT(*) n FROM likes WHERE from_id=? AND created_at>=datetime("now",?)',uid,since);const posts=count('SELECT COUNT(*) n FROM messages WHERE sender_id=? AND created_at>=datetime("now",?)',uid,since);const views=Number(viewed);res.json({stats:{views,visits,likesIn,likesOut,matches:0,attractiveness:Math.min(100,likesIn*5),selectivity:Math.min(100,Math.max(0,100-likesOut*3)),messages:posts,coins:req.user.coins,xp:req.user.xp,level:req.user.level,lastSeen:req.user.last_seen}});});
app.get('/api/profile/:id',requireAuth,(req,res)=>{const u=userById(Number(req.params.id));if(!u||u.is_banned)return res.status(404).json({error:'کاربر پیدا نشد'});const isSelf=u.id===req.user.id;const isPro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());const messagePass=!!(req.user.message_pass_until&&Date.parse(req.user.message_pass_until)>Date.now());const friends=!!db.prepare("SELECT 1 FROM friend_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,u.id,u.id,req.user.id);const visible=safeUser(u);if(isSelf)visible.phone=u.phone||'';if(!isSelf&&!isPro){visible.age=null;visible.province='';}if(!isSelf){const isPro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());const row=db.prepare("SELECT COUNT(*) n FROM profile_views WHERE viewer_id=? AND created_at>=datetime('now','-8 hours')").get(req.user.id);const cap=isPro?100:50;if(Number(row?.n||0)>=cap)return res.status(429).json({error:`سهمیه مشاهده شما برای ۸ ساعت تمام شده است (${cap})`});db.prepare('INSERT INTO profile_views(viewer_id,viewed_id) VALUES(?,?)').run(req.user.id,u.id);notify(u.id,'بازدید پروفایل',req.user.display_name+' از پروفایل شما دیدن کرد');}const rank=1+db.prepare('SELECT COUNT(*) n FROM users WHERE is_banned=0 AND (xp>? OR (xp=? AND id<?))').get(Number(u.xp||0),Number(u.xp||0),u.id).n;res.json({user:visible,rank,likes:db.prepare('SELECT COUNT(*) n FROM likes WHERE to_id=?').get(u.id).n,followers:db.prepare('SELECT COUNT(*) n FROM follows WHERE followed_id=?').get(u.id).n,following:db.prepare('SELECT COUNT(*) n FROM follows WHERE follower_id=?').get(u.id).n,posts:db.prepare("SELECT COUNT(*) n FROM posts WHERE user_id=? AND status IN ('active','hidden')").get(u.id).n,liked:!!db.prepare('SELECT 1 FROM likes WHERE from_id=? AND to_id=?').get(req.user.id,u.id),isBlockedByMe:!isSelf&&!!db.prepare('SELECT 1 FROM blocks WHERE user_id=? AND blocked_id=?').get(req.user.id,u.id),isSelf,isPro,friends,canViewLikers:isPro});});
// Group chat APIs: group messages never deduct coins.
function groupMember(gid,uid){return db.prepare('SELECT * FROM chat_group_members WHERE group_id=? AND user_id=?').get(gid,uid)}
function groupCanManage(gid,uid){const m=groupMember(gid,uid);return m&&['owner','admin'].includes(m.role)}
app.get('/api/groups',requireAuth,(req,res)=>{const official=db.prepare("SELECT id FROM chat_groups WHERE group_code='ACECLUB_OFFICIAL' OR name='گروه ایس کلاب' ORDER BY CASE WHEN group_code='ACECLUB_OFFICIAL' THEN 0 ELSE 1 END,id LIMIT 1").get();if(official){db.prepare("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')").run(official.id,req.user.id);db.prepare("INSERT OR IGNORE INTO group_message_reads(group_id,user_id,last_read_message_id) SELECT ?,?,COALESCE(MAX(id),0) FROM chat_group_messages WHERE group_id=?").run(official.id,req.user.id,official.id);}const q=String(req.query.q||'').trim().slice(0,80);const groups=db.prepare(`SELECT g.*,u.username owner_username,(SELECT COUNT(*) FROM chat_group_members m WHERE m.group_id=g.id) member_count FROM chat_groups g LEFT JOIN users u ON u.id=g.owner_id WHERE (?='' OR g.name LIKE ? OR g.group_code LIKE ? OR CAST(g.id AS TEXT)=?) ORDER BY CASE WHEN g.group_code='ACECLUB_OFFICIAL' THEN 0 ELSE 1 END,COALESCE((SELECT MAX(gm.created_at) FROM chat_group_messages gm WHERE gm.group_id=g.id),g.created_at) DESC,g.id DESC LIMIT 100`).all(q,'%'+q+'%','%'+q+'%',q);const mine=groups.filter(g=>groupMember(g.id,req.user.id));res.json({groups:groups.map(g=>{const member=groupMember(g.id,req.user.id);const lastRead=Number(db.prepare('SELECT last_read_message_id FROM group_message_reads WHERE group_id=? AND user_id=?').get(g.id,req.user.id)?.last_read_message_id||0);const unread=member?Number(db.prepare('SELECT COUNT(*) n FROM chat_group_messages WHERE group_id=? AND deleted=0 AND sender_id<>? AND id>?').get(g.id,req.user.id,lastRead)?.n||0):0;return {...g,joined:!!member,myRole:member?.role||null,unread};}),mine:mine.map(g=>g.id)});});
app.post('/api/groups',requireAuth,(req,res)=>{const name=String(req.body.name||'').trim().slice(0,60),description=String(req.body.description||'').trim().slice(0,500),code=String(req.body.group_code||'').trim();const avatar=String(req.body.avatar||''),accessMode=String(req.body.access_mode||'open')==='request'?'request':'open';if(name.length<2)return res.status(400).json({error:'نام گروه حداقل ۲ حرف باشد'});if(!/^[A-Za-z0-9_]{3,24}$/.test(code))return res.status(400).json({error:'شناسه باید ۳ تا ۲۴ حرف انگلیسی، عدد یا زیرخط باشد'});if(db.prepare('SELECT id FROM chat_groups WHERE lower(group_code)=lower(?)').get(code))return res.status(409).json({error:'این شناسه گروه قبلاً گرفته شده است'});if(avatar&&(!/^data:image\/(png|jpeg|jpg|webp|gif);base64,/i.test(avatar)||avatar.length>1450000))return res.status(400).json({error:'عکس نامعتبر است یا حجم آن بیشتر از ۱ مگابایت است'});if(Number(req.user.coins)<500)return res.status(400).json({error:'ساخت گروه ۵۰۰ سکه هزینه دارد'});let groupId;try{atomic(()=>{const fresh=userById(req.user.id);if(!fresh||Number(fresh.coins)<500)throw new Error('سکه کافی نداری');if(db.prepare('SELECT id FROM chat_groups WHERE lower(group_code)=lower(?)').get(code))throw new Error('این شناسه گروه قبلاً گرفته شده است');addTx(req.user.id,-500,'group_create','هزینه ساخت گروه');const r=db.prepare('INSERT INTO chat_groups(name,group_code,description,avatar,owner_id,access_mode) VALUES(?,?,?,?,?,?)').run(name,code,description,avatar,req.user.id,accessMode);groupId=Number(r.lastInsertRowid);db.prepare("INSERT INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'owner')").run(groupId,req.user.id);logActivity(req.user.id,null,'group_create',`ساخت گروه #${groupId}`);});}catch(e){return res.status(400).json({error:e.message||'ساخت گروه انجام نشد؛ موجودی تغییر نکرد'});}res.json({ok:true,id:groupId,group_code:code,user:safeUser(userById(req.user.id))});});
app.post('/api/groups/:id/join',requireAuth,(req,res)=>{
 const id=Number(req.params.id),g=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id);
 if(!g)return res.status(404).json({error:'گروه پیدا نشد'});
 if(groupMember(id,req.user.id))return res.json({ok:true,joined:true});
 if(g.access_mode==='request'){
  const prior=db.prepare('SELECT status FROM group_join_requests WHERE group_id=? AND user_id=?').get(id,req.user.id);
  if(prior?.status==='pending')return res.json({ok:true,requested:true,status:'pending'});
  if(prior?.status==='approved'){
   db.prepare("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')").run(id,req.user.id);
   return res.json({ok:true,joined:true});
  }
  if(prior?.status==='rejected')db.prepare("UPDATE group_join_requests SET status='pending',created_at=CURRENT_TIMESTAMP,reviewed_at=NULL,reviewed_by=NULL WHERE group_id=? AND user_id=?").run(id,req.user.id);
  else db.prepare("INSERT OR IGNORE INTO group_join_requests(group_id,user_id,status) VALUES(?,?,'pending')").run(id,req.user.id);
  const owners=db.prepare("SELECT user_id FROM chat_group_members WHERE group_id=? AND role IN ('owner','admin')").all(id);
  for(const o of owners)notify(o.user_id,'درخواست عضویت جدید',`${req.user.display_name||req.user.username} درخواست پیوستن به گروه «${g.name}» را داده است.`,{action:'group_join_request',groupId:id});
  return res.json({ok:true,requested:true,status:'pending'});
 }
 db.prepare("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')").run(id,req.user.id);
 db.prepare("INSERT OR IGNORE INTO group_message_reads(group_id,user_id,last_read_message_id) SELECT ?,?,COALESCE(MAX(id),0) FROM chat_group_messages WHERE group_id=?").run(id,req.user.id,id);
 res.json({ok:true,joined:true});
});
app.get('/api/groups/:id/join-requests',requireAuth,(req,res)=>{
 const id=Number(req.params.id),m=groupMember(id,req.user.id);
 if(!m||!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط مدیران گروه به درخواست‌ها دسترسی دارند'});
 const rows=db.prepare(`SELECT r.id,r.user_id,r.status,r.created_at,u.username,u.display_name,u.avatar
 FROM group_join_requests r JOIN users u ON u.id=r.user_id
 WHERE r.group_id=? AND r.status='pending' ORDER BY r.created_at ASC`).all(id);
 res.json({requests:rows,count:rows.length});
});
app.post('/api/groups/:id/join-requests/:requestId/respond',requireAuth,(req,res)=>{
 const id=Number(req.params.id),rid=Number(req.params.requestId),action=String(req.body.action||'');
 const m=groupMember(id,req.user.id);
 if(!m||!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط مدیران گروه می‌توانند درخواست را بررسی کنند'});
 if(!['approve','reject'].includes(action))return res.status(400).json({error:'عملیات نامعتبر است'});
 const r=db.prepare("SELECT * FROM group_join_requests WHERE id=? AND group_id=?").get(rid,id);
 if(!r)return res.status(404).json({error:'درخواست پیدا نشد'});
 if(r.status!=='pending')return res.status(409).json({error:'این درخواست قبلاً بررسی شده است'});
 try{atomic(()=>{
  const changed=db.prepare("UPDATE group_join_requests SET status=?,reviewed_at=CURRENT_TIMESTAMP,reviewed_by=? WHERE id=? AND status='pending'").run(action==='approve'?'approved':'rejected',req.user.id,rid);
  if(!changed.changes)throw new Error('درخواست قبلاً بررسی شده است');
  if(action==='approve'){
   db.prepare("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')").run(id,r.user_id);
   db.prepare("INSERT OR IGNORE INTO group_message_reads(group_id,user_id,last_read_message_id) SELECT ?,?,COALESCE(MAX(id),0) FROM chat_group_messages WHERE group_id=?").run(id,r.user_id,id);
  }
 })()}catch(e){return res.status(409).json({error:e.message||'بررسی درخواست انجام نشد'})}
 notify(r.user_id,action==='approve'?'درخواست عضویت تأیید شد':'درخواست عضویت رد شد',action==='approve'?'درخواستت تأیید شد؛ اکنون می‌توانی وارد گروه شوی.':'درخواست عضویتت توسط مدیر گروه رد شد.',{action:'group_join_response',groupId:id});
 res.json({ok:true,status:action==='approve'?'approved':'rejected',count:db.prepare("SELECT COUNT(*) n FROM group_join_requests WHERE group_id=? AND status='pending'").get(id).n});
});

app.get('/api/groups/by-code/:code',requireAuth,(req,res)=>{
 const code=String(req.params.code||'').trim();
 const g=db.prepare('SELECT g.id,g.name,g.group_code,g.description,g.avatar,g.owner_id,g.access_mode,g.created_at,u.display_name owner_name,(SELECT COUNT(*) FROM chat_group_members m WHERE m.group_id=g.id) member_count FROM chat_groups g LEFT JOIN users u ON u.id=g.owner_id WHERE lower(g.group_code)=lower(?)').get(code);
 if(!g)return res.status(404).json({error:'لینک گروه معتبر نیست یا گروه حذف شده است'});
 const member=!!groupMember(g.id,req.user.id);
 const request=db.prepare('SELECT status,created_at FROM group_join_requests WHERE group_id=? AND user_id=?').get(g.id,req.user.id);
 res.json({group:g,member,request:request||null,joinUrl:`https://aceclub.ir/grop/${encodeURIComponent(g.group_code)}`});
});
app.get('/api/groups/promoted',requireAuth,(req,res)=>{
 // Self-heal the campaign-view table for databases created by older builds.
 try { db.exec('CREATE TABLE IF NOT EXISTS group_promotion_campaign_views(group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,campaign_id TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(group_id,user_id,campaign_id))'); } catch(e) { return res.status(500).json({error:'بارگذاری ترویج‌های گروه فعلاً ممکن نیست'}); }
 // The owner must be able to see their own active promotion without consuming one
 // of the 100 unique-user impressions. Only other users create a campaign view.
 const own=db.prepare(`SELECT g.*,u.username owner_username,(SELECT COUNT(*) FROM chat_group_members m WHERE m.group_id=g.id) member_count FROM chat_groups g LEFT JOIN users u ON u.id=g.owner_id WHERE g.owner_id=? AND g.promoted_until IS NOT NULL AND datetime(g.promoted_until)>datetime('now') AND g.promoted_campaign_id<>'' ORDER BY datetime(g.promoted_until) DESC,g.id DESC`).all(req.user.id);
 // Keep a promoted group visible to the same user after they navigate away and return.
 // A campaign impression is unique per user and campaign; merely reloading the home
 // screen or the promoted-groups page must not hide the group or consume another view.
 // Once the 100 unique-user impressions are used, only users who already saw this
 // campaign can still see it; it remains visible until its campaign expires.
 const candidates=db.prepare(`SELECT g.*,u.username owner_username,(SELECT COUNT(*) FROM chat_group_members m WHERE m.group_id=g.id) member_count FROM chat_groups g LEFT JOIN users u ON u.id=g.owner_id WHERE g.owner_id<>? AND (g.promoted_until IS NULL OR datetime(g.promoted_until)>datetime('now')) AND g.promoted_campaign_id<>'' AND (g.promoted_remaining>0 OR EXISTS(SELECT 1 FROM group_promotion_campaign_views pv WHERE pv.group_id=g.id AND pv.user_id=? AND pv.campaign_id=g.promoted_campaign_id)) ORDER BY RANDOM() LIMIT 10`).all(req.user.id,req.user.id);
 const groups=[...own.map(g=>({...g,owner_view:true})),...candidates];
 for(const g of candidates){
   try{ atomic(()=>{const impression=db.prepare('INSERT OR IGNORE INTO group_promotion_campaign_views(group_id,user_id,campaign_id) VALUES(?,?,?)').run(g.id,req.user.id,g.promoted_campaign_id);if(impression.changes>0)db.prepare('UPDATE chat_groups SET promoted_remaining=MAX(0,promoted_remaining-1) WHERE id=? AND promoted_campaign_id=? AND promoted_remaining>0').run(g.id,g.promoted_campaign_id);}); }catch{}
 }
 res.json({groups:groups.map(g=>({...g,joined:!!groupMember(g.id,req.user.id)}))});
});
app.post('/api/groups/:id/promote',requireAuth,(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});if(!groupMember(id,req.user.id)||g.owner_id!==req.user.id)return res.status(403).json({error:'فقط سازنده گروه می‌تواند گروه را ترویج کند'});if(Number(req.user.coins)<50)return res.status(400).json({error:'ترویج گروه ۵۰ سکه هزینه دارد'});const campaign=crypto.randomUUID();try{atomic(()=>{const fresh=userById(req.user.id);if(Number(fresh.coins)<50)throw new Error('سکه کافی نداری');addTx(req.user.id,-50,'group_promote',`ترویج گروه #${id} برای ۱۰۰ کاربر متفاوت`);db.prepare("UPDATE chat_groups SET promoted_remaining=100,promoted_until=datetime('now','+30 days'),promoted_campaign_id=? WHERE id=?").run(campaign,id);});}catch(e){return res.status(400).json({error:e.message||'ترویج انجام نشد'});}res.json({ok:true,campaign,user:safeUser(userById(req.user.id)),message:'گروه برای نمایش به ۱۰۰ کاربر متفاوت ترویج شد'});});
app.get('/api/users/search',requireAuth,(req,res)=>{const q=String(req.query.q||'').trim().replace(/^@/,'').slice(0,40);if(q.length<1)return res.json({users:[]});const numeric=/^\d+$/.test(q);const users=db.prepare(`SELECT id,username,display_name,avatar FROM users WHERE id<>? AND is_banned=0 AND (${numeric?'id=? OR ':''}username LIKE ? OR display_name LIKE ?) ORDER BY CASE WHEN username=? THEN 0 ELSE 1 END LIMIT 20`).all(...(numeric?[req.user.id,Number(q),'%'+q+'%','%'+q+'%',q]:[req.user.id,'%'+q+'%','%'+q+'%',q]));res.json({users});});app.get('/api/directory/search',requireAuth,(req,res)=>{const q=String(req.query.q||'').trim().replace(/^@/,'').slice(0,40);const type=['all','users','groups'].includes(String(req.query.type||'all'))?String(req.query.type||'all'):'all';if(q.length<2)return res.json({users:[],groups:[]});const isNumeric=/^\d+$/.test(q);let users=[],groups=[];if(type==='all'||type==='users'){users=db.prepare(`SELECT id,username,display_name,avatar FROM users WHERE id<>? AND is_banned=0 AND (${isNumeric?'id=? OR ':''}username LIKE ?) ORDER BY CASE WHEN username=? THEN 0 ELSE 1 END LIMIT 30`).all(...(isNumeric?[req.user.id,Number(q),'%'+q+'%',q]:[req.user.id,'%'+q+'%',q]));}if(type==='all'||type==='groups'){groups=db.prepare(`SELECT g.id,g.name,g.group_code,g.avatar,(SELECT COUNT(*) FROM chat_group_members m WHERE m.group_id=g.id) member_count FROM chat_groups g WHERE ${isNumeric?'g.id=? OR ':''}g.group_code LIKE ? ORDER BY CASE WHEN lower(g.group_code)=lower(?) THEN 0 ELSE 1 END,g.id DESC LIMIT 30`).all(...(isNumeric?[Number(q),'%'+q+'%',q]:['%'+q+'%',q]));}res.json({users,groups});});
app.get('/api/groups/:id/invite-candidates',requireAuth,(req,res)=>{
 const gid=Number(req.params.id),q=String(req.query.q||'').trim().slice(0,80);
 const limit=Math.min(Math.max(Number(req.query.limit)||100,1),200),offset=Math.max(Number(req.query.offset)||0,0);
 if(!db.prepare('SELECT 1 FROM chat_groups WHERE id=?').get(gid))return res.status(404).json({error:'گروه پیدا نشد'});
 const gm=groupMember(gid,req.user.id);
 if(!gm||!['owner','admin'].includes(gm.role))return res.status(403).json({error:'فقط سازنده یا مدیر گروه می‌تواند دعوت بفرستد'});
 const like='%'+q.replace(/[%_]/g,'\\$&')+'%';
 const users=db.prepare(`SELECT u.id,u.username,u.display_name,u.avatar
   FROM follows f JOIN users u ON u.id=f.followed_id
   WHERE f.follower_id=? AND u.id<>? AND u.is_banned=0
     AND NOT EXISTS(SELECT 1 FROM chat_group_members m WHERE m.group_id=? AND m.user_id=u.id)
     AND (?='' OR u.display_name LIKE ? ESCAPE '\\' OR u.username LIKE ? ESCAPE '\\')
   ORDER BY f.id DESC LIMIT ? OFFSET ?`).all(req.user.id,req.user.id,gid,q,like,like,limit+1,offset);
 const hasMore=users.length>limit;
 if(hasMore)users.pop();
 res.json({users,offset,nextOffset:hasMore?offset+users.length:null,hasMore});
});
app.post('/api/groups/:id/invites',requireAuth,(req,res)=>{const gid=Number(req.params.id),uid=Number(req.body.userId),g=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(gid);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});if(!groupMember(gid,req.user.id)||!['owner','admin'].includes(groupMember(gid,req.user.id).role))return res.status(403).json({error:'فقط سازنده یا مدیر گروه می‌تواند دعوت بفرستد'});if(!userById(uid)||uid===req.user.id)return res.status(400).json({error:'کاربر انتخاب‌شده معتبر نیست'});if(!db.prepare('SELECT 1 FROM follows WHERE follower_id=? AND followed_id=?').get(req.user.id,uid))return res.status(403).json({error:'فقط افرادی را می‌توانی دعوت کنی که دنبال کرده‌ای'});if(groupMember(gid,uid))return res.status(409).json({error:'این کاربر از قبل عضو گروه است'});const prev=db.prepare('SELECT * FROM chat_group_invites WHERE group_id=? AND to_id=?').get(gid,uid);if(prev?.status==='pending')return res.status(409).json({error:'برای این کاربر قبلاً درخواست فرستاده‌ای'});let inviteId;if(prev){db.prepare("UPDATE chat_group_invites SET from_id=?,status='pending',created_at=CURRENT_TIMESTAMP WHERE id=?").run(req.user.id,prev.id);inviteId=prev.id;}else inviteId=Number(db.prepare("INSERT INTO chat_group_invites(group_id,from_id,to_id) VALUES(?,?,?)").run(gid,req.user.id,uid).lastInsertRowid);notify(uid,'درخواست عضویت در گروه',`${req.user.display_name} دعوتت کرده عضو گروه «${g.name}» شوی.`,{action:'group_invite',fromId:req.user.id,requestId:inviteId});res.json({ok:true,message:'درخواست عضویت ارسال شد'});});
app.post('/api/group-invites/:id/respond',requireAuth,(req,res)=>{const inv=db.prepare("SELECT * FROM chat_group_invites WHERE id=? AND to_id=? AND status='pending'").get(Number(req.params.id),req.user.id);if(!inv)return res.status(404).json({error:'درخواست پیدا نشد یا قبلاً پاسخ داده شده'});const accept=req.body.accept===true;db.prepare('UPDATE chat_group_invites SET status=? WHERE id=?').run(accept?'accepted':'rejected',inv.id);if(accept){db.prepare("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')").run(inv.group_id,req.user.id);db.prepare("INSERT OR IGNORE INTO group_message_reads(group_id,user_id,last_read_message_id) SELECT ?,?,COALESCE(MAX(id),0) FROM chat_group_messages WHERE group_id=?").run(inv.group_id,req.user.id,inv.group_id);}const g=db.prepare('SELECT name FROM chat_groups WHERE id=?').get(inv.group_id);notify(inv.from_id,accept?'دعوت گروه پذیرفته شد':'دعوت گروه رد شد',`${req.user.display_name} دعوت عضویت در گروه «${g?.name||''}» را ${accept?'پذیرفت':'رد کرد'}.`);res.json({ok:true,accepted:accept});});
app.post('/api/groups/:id/claim-reward',requireAuth,(req,res)=>{const id=Number(req.params.id);if(!groupMember(id,req.user.id))return res.status(403).json({error:'برای دریافت جایزه باید عضو گروه باشی'});const exists=db.prepare("SELECT id FROM group_reward_claims WHERE group_id=? AND user_id=? AND reward_date=date('now')").get(id,req.user.id);if(exists)return res.status(409).json({error:'جایزه امروز این گروه را قبلاً دریافت کرده‌ای'});const coins=10+Math.floor(Math.random()*41);try{atomic(()=>{db.prepare('INSERT INTO group_reward_claims(group_id,user_id,coins) VALUES(?,?,?)').run(id,req.user.id,coins);addTx(req.user.id,coins,'group_reward',`جایزه روزانه گروه #${id}`);db.prepare('UPDATE chat_groups SET fame=fame+10 WHERE id=?').run(id);});}catch(e){return res.status(400).json({error:'دریافت جایزه انجام نشد'});}res.json({ok:true,coins,user:safeUser(userById(req.user.id)),message:`${coins} سکه جایزه گرفتی`});});

app.post('/api/groups/:id/leave',requireAuth,(req,res)=>{const id=Number(req.params.id),m=groupMember(id,req.user.id);if(!m)return res.json({ok:true});if(m.role==='owner')return res.status(400).json({error:'سازنده باید ابتدا مالکیت را منتقل کند یا گروه را حذف کند'});db.prepare('DELETE FROM chat_group_members WHERE group_id=? AND user_id=?').run(id,req.user.id);res.json({ok:true});});
app.get('/api/groups/:id/preview',requireAuth,(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT g.*,(SELECT COUNT(*) FROM chat_group_members cm WHERE cm.group_id=g.id) member_count,u.username owner_username,u.display_name owner_name FROM chat_groups g JOIN users u ON u.id=g.owner_id WHERE g.id=?').get(id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});const member=groupMember(id,req.user.id)||null;const members=db.prepare("SELECT m.user_id,m.role,u.username,u.display_name,u.avatar FROM chat_group_members m JOIN users u ON u.id=m.user_id WHERE m.group_id=? ORDER BY CASE m.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END,m.joined_at LIMIT 50").all(id);res.json({group:g,member,members,messages:[]});});
app.get('/api/groups/:id',requireAuth,(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT g.*,u.username owner_username,u.display_name owner_name FROM chat_groups g JOIN users u ON u.id=g.owner_id WHERE g.id=?').get(id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});const member=groupMember(id,req.user.id);if(!member)return res.status(403).json({error:'برای دیدن پیام‌ها ابتدا عضو گروه شو'});member.notifications_muted=Number(db.prepare('SELECT muted FROM group_user_settings WHERE group_id=? AND user_id=?').get(id,req.user.id)?.muted||0);const members=db.prepare('SELECT m.user_id,m.role,m.muted,u.username,u.display_name,u.avatar FROM chat_group_members m JOIN users u ON u.id=m.user_id WHERE m.group_id=? ORDER BY CASE m.role WHEN \'owner\' THEN 0 WHEN \'admin\' THEN 1 ELSE 2 END,m.joined_at').all(id);const messages=db.prepare('SELECT m.*,u.username,u.display_name,u.avatar,(SELECT COUNT(*) FROM chat_group_messages r WHERE r.reply_to=m.id AND r.deleted=0) reply_count,ru.display_name reply_to_name FROM chat_group_messages m JOIN users u ON u.id=m.sender_id LEFT JOIN chat_group_messages om ON om.id=m.reply_to LEFT JOIN users ru ON ru.id=om.sender_id WHERE m.group_id=? AND m.deleted=0 ORDER BY m.id DESC LIMIT 200').all(id).reverse();markGroupRead(id,req.user.id);io.to('user:'+req.user.id).emit('unread:updated',unreadSummary(req.user.id));res.json({group:g,member,members,messages,unread:0});});
app.post('/api/groups/:id/read',requireAuth,(req,res)=>{const id=Number(req.params.id);if(!groupMember(id,req.user.id))return res.status(403).json({error:'عضو گروه نیستی'});markGroupRead(id,req.user.id);const counts=unreadSummary(req.user.id);io.to('user:'+req.user.id).emit('unread:updated',counts);res.json({ok:true,unread:0,counts});});
app.post('/api/groups/:id/messages',requireAuth,enforceRestriction('messages'),enforceRestriction('conversation'),(req,res)=>{const id=Number(req.params.id),m=groupMember(id,req.user.id),body=String(req.body.body||'').trim().slice(0,2000),replyTo=Number(req.body.replyTo)||null;if(!m)return res.status(403).json({error:'عضو گروه نیستی'});if(db.prepare('SELECT muted FROM chat_groups WHERE id=?').get(id)?.muted)return res.status(403).json({error:'ارسال پیام در این گروه موقتاً توسط مدیر گروه بسته شده است'});if(m.muted)return res.status(403).json({error:'ارسال پیام شما در این گروه بی‌صدا شده است'});if(!body)return res.status(400).json({error:'متن پیام خالی است'});let original=null;if(replyTo){original=db.prepare('SELECT id,sender_id FROM chat_group_messages WHERE id=? AND group_id=? AND deleted=0').get(replyTo,id);if(!original)return res.status(400).json({error:'پیام موردنظر برای پاسخ پیدا نشد'});}const r=db.prepare('INSERT INTO chat_group_messages(group_id,sender_id,body,reply_to) VALUES(?,?,?,?)').run(id,req.user.id,body,replyTo);db.prepare('UPDATE chat_groups SET fame=fame+10 WHERE id=?').run(id);const message=db.prepare('SELECT m.*,u.username,u.display_name,u.avatar,(SELECT COUNT(*) FROM chat_group_messages r WHERE r.reply_to=m.id AND r.deleted=0) reply_count,ru.display_name reply_to_name FROM chat_group_messages m JOIN users u ON u.id=m.sender_id LEFT JOIN chat_group_messages om ON om.id=m.reply_to LEFT JOIN users ru ON ru.id=om.sender_id WHERE m.id=?').get(r.lastInsertRowid);if(original&&Number(original.sender_id)!==Number(req.user.id))notify(original.sender_id,'پاسخ جدید در گروه',`${req.user.display_name} به پیام شما در یک گروه پاسخ داد.`,{action:'group_reply',requestId:r.lastInsertRowid});io.to('group:'+id).emit('group:message',message);for(const member of db.prepare('SELECT user_id FROM chat_group_members WHERE group_id=? AND user_id<>?').all(id,req.user.id))io.to('user:'+member.user_id).emit('unread:updated',unreadSummary(member.user_id));res.json({ok:true,id:r.lastInsertRowid,fameAdded:10,message});});
app.post('/api/groups/:id/media',requireAuth,upload.single('file'),(req,res)=>{const id=Number(req.params.id),m=groupMember(id,req.user.id);if(!m)return res.status(403).json({error:'عضو گروه نیستی'});if(db.prepare('SELECT muted FROM chat_groups WHERE id=?').get(id)?.muted)return res.status(403).json({error:'ارسال پیام در این گروه موقتاً توسط مدیر گروه بسته شده است'});if(m.muted)return res.status(403).json({error:'ارسال پیام شما در این گروه بی‌صدا شده است'});if(!req.file)return res.status(400).json({error:'فایل صوتی یا تصویر معتبر نیست'});const mime=String(req.file.mimetype||'');const type=mime.startsWith('audio/')?'audio':mime.startsWith('image/')?(mime==='image/gif'?'gif':'image'):'';if(!['audio','image','gif'].includes(type)){try{fs.unlinkSync(req.file.path)}catch{}return res.status(400).json({error:'فقط ویس یا تصویر مجاز است'});}const r=db.prepare('INSERT INTO chat_group_messages(group_id,sender_id,body,type,media_url) VALUES(?,?,"",?,?)').run(id,req.user.id,type,'/uploads/'+req.file.filename);db.prepare('UPDATE chat_groups SET fame=fame+10 WHERE id=?').run(id);const message=db.prepare('SELECT m.*,u.username,u.display_name,u.avatar,0 AS reply_count,NULL AS reply_to_name FROM chat_group_messages m JOIN users u ON u.id=m.sender_id WHERE m.id=?').get(r.lastInsertRowid);io.to('group:'+id).emit('group:message',message);for(const member of db.prepare('SELECT user_id FROM chat_group_members WHERE group_id=? AND user_id<>?').all(id,req.user.id))io.to('user:'+member.user_id).emit('unread:updated',unreadSummary(member.user_id));res.json({ok:true,message});});
app.post('/api/groups/:id/mute-all',requireAuth,(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id),m=groupMember(id,req.user.id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});if(!m||!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط سازنده یا مدیر گروه می‌تواند سکوت گروه را تغییر دهد'});const muted=req.body.muted?1:0;db.prepare('UPDATE chat_groups SET muted=? WHERE id=?').run(muted,id);io.to('group:'+id).emit('group:muteAll',{groupId:id,muted:!!muted});logActivity(req.user.id,null,'group_mute_all',`${muted?'فعال‌سازی':'برداشتن'} سکوت گروه #${id}`);res.json({ok:true,muted:!!muted});});
app.patch('/api/groups/:id',requireAuth,(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id),m=groupMember(id,req.user.id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});if(!m||!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط مدیران گروه می‌توانند گروه را ویرایش کنند'});const name=req.body.name===undefined?g.name:String(req.body.name).trim().slice(0,60),description=req.body.description===undefined?g.description:String(req.body.description).trim().slice(0,500),avatar=req.body.avatar===undefined?g.avatar:String(req.body.avatar||''),accessMode=req.body.access_mode===undefined?(g.access_mode||'open'):(String(req.body.access_mode)==='request'?'request':'open');if(name.length<2)return res.status(400).json({error:'نام گروه حداقل ۲ حرف باشد'});if(avatar&&(!/^data:image\/(png|jpeg|jpg|webp|gif);base64,/i.test(avatar)||avatar.length>1450000)&&avatar!==g.avatar)return res.status(400).json({error:'عکس گروه نامعتبر است یا بیشتر از ۱ مگابایت است'});db.prepare('UPDATE chat_groups SET name=?,description=?,avatar=?,access_mode=? WHERE id=?').run(name,description,avatar,accessMode,id);logActivity(req.user.id,null,'group_edit',`ویرایش گروه #${id}`);res.json({ok:true,group:db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id)});});
app.delete('/api/groups/:id/members/:userId',requireAuth,(req,res)=>{const id=Number(req.params.id),uid=Number(req.params.userId),g=db.prepare('SELECT owner_id FROM chat_groups WHERE id=?').get(id),m=groupMember(id,req.user.id),target=groupMember(id,uid);if(!g||!target)return res.status(404).json({error:'گروه یا عضو پیدا نشد'});if(!m||!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط مدیر گروه می‌تواند عضو اخراج کند'});if(target.role==='owner')return res.status(400).json({error:'سازنده گروه قابل اخراج نیست'});if(target.role==='admin'&&m.role!=='owner')return res.status(403).json({error:'فقط سازنده می‌تواند مدیر دیگری را اخراج کند'});db.prepare('DELETE FROM chat_group_members WHERE group_id=? AND user_id=?').run(id,uid);io.to('group:'+id).emit('group:memberRemoved',{groupId:id,userId:uid});notify(uid,'اخراج از گروه',`از گروه «${db.prepare('SELECT name FROM chat_groups WHERE id=?').get(id)?.name||''}» اخراج شدی.`);logActivity(req.user.id,uid,'group_kick',`اخراج کاربر ${uid} از گروه #${id}`);res.json({ok:true});});
app.delete('/api/groups/:id/admins/:userId',requireAuth,(req,res)=>{const id=Number(req.params.id),uid=Number(req.params.userId),g=db.prepare('SELECT owner_id FROM chat_groups WHERE id=?').get(id);if(!g||g.owner_id!==req.user.id)return res.status(403).json({error:'فقط سازنده گروه می‌تواند مدیر را برکنار کند'});const target=groupMember(id,uid);if(!target||target.role!=='admin')return res.status(404).json({error:'مدیر پیدا نشد'});db.prepare("UPDATE chat_group_members SET role='member' WHERE group_id=? AND user_id=?").run(id,uid);io.to('group:'+id).emit('group:roleChanged',{groupId:id,userId:uid,role:'member'});logActivity(req.user.id,uid,'group_remove_admin',`برداشتن مدیریت کاربر ${uid} از گروه #${id}`);res.json({ok:true});});
app.post('/api/groups/:id/messages/:messageId/pin',requireAuth,(req,res)=>{const id=Number(req.params.id),mid=Number(req.params.messageId),m=groupMember(id,req.user.id);if(!m||!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط مدیر گروه می‌تواند پین پیام را مدیریت کند'});const msg=db.prepare('SELECT * FROM chat_group_messages WHERE id=? AND group_id=? AND deleted=0').get(mid,id);if(!msg)return res.status(404).json({error:'پیام پیدا نشد'});const unpinned=Number(msg.pinned)===1;if(unpinned){db.prepare('UPDATE chat_group_messages SET pinned=0 WHERE id=? AND group_id=?').run(mid,id)}else{db.prepare('UPDATE chat_group_messages SET pinned=0 WHERE group_id=?').run(id);db.prepare('UPDATE chat_group_messages SET pinned=1 WHERE id=? AND group_id=?').run(mid,id)}io.to('group:'+id).emit('group:messagePinned',{groupId:id,messageId:mid,pinned:!unpinned});logActivity(req.user.id,msg.sender_id,unpinned?'group_unpin_message':'group_pin_message',`${unpinned?'حذف سنجاق':'پین'} پیام #${mid} در گروه #${id}`);res.json({ok:true,messageId:mid,pinned:!unpinned});});
app.delete('/api/groups/:id/messages/:messageId',requireAuth,(req,res)=>{const id=Number(req.params.id),mid=Number(req.params.messageId),m=groupMember(id,req.user.id),msg=db.prepare('SELECT * FROM chat_group_messages WHERE id=? AND group_id=?').get(mid,id);if(!m||!msg)return res.status(404).json({error:'پیام پیدا نشد'});if(msg.sender_id!==req.user.id&&!['owner','admin'].includes(m.role))return res.status(403).json({error:'فقط فرستنده یا مدیر گروه می‌تواند پیام را حذف کند'});if(msg.media_url){const file=path.resolve(uploadsDir,String(msg.media_url).replace(/^\/uploads\//,''));if(file.startsWith(path.resolve(uploadsDir)+path.sep))try{fs.unlinkSync(file)}catch{}}db.prepare("UPDATE chat_group_messages SET deleted=1,body='',media_url='',pinned=0 WHERE id=?").run(mid);io.to('group:'+id).emit('group:messageDeleted',{groupId:id,messageId:mid});logActivity(req.user.id,msg.sender_id,'group_delete_message',`حذف پیام #${mid} در گروه #${id}`);res.json({ok:true});});
app.post('/api/groups/:id/admins',requireAuth,(req,res)=>{const id=Number(req.params.id),uid=Number(req.body.userId);const g=db.prepare('SELECT owner_id FROM chat_groups WHERE id=?').get(id);if(!g||g.owner_id!==req.user.id)return res.status(403).json({error:'فقط سازنده گروه می‌تواند مدیر تعیین کند'});if(!groupMember(id,uid))return res.status(400).json({error:'این کاربر عضو گروه نیست'});db.prepare("UPDATE chat_group_members SET role='admin' WHERE group_id=? AND user_id=? AND role<>'owner'").run(id,uid);res.json({ok:true});});
app.post('/api/groups/:id/contribute',requireAuth,(req,res)=>{const id=Number(req.params.id),amount=Number(req.body.amount),kind=String(req.body.kind||'coins');if(!groupMember(id,req.user.id))return res.status(403).json({error:'عضو گروه نیستی'});if(!Number.isInteger(amount)||amount<1||amount>50||!['coins','fame'].includes(kind))return res.status(400).json({error:'مقدار باید بین ۱ تا ۵۰ و نوع پاداش معتبر باشد'});if(req.user.coins<amount)return res.status(400).json({error:'سکه کافی نداری'});try{atomic(()=>{const fresh=userById(req.user.id),group=db.prepare('SELECT id FROM chat_groups WHERE id=?').get(id);if(!group)throw new Error('گروه پیدا نشد');if(!fresh||Number(fresh.coins)<amount)throw new Error('سکه کافی نداری');addTx(req.user.id,-amount,'group_contribution',`کمک ${amount} سکه به گروه #${id}`);if(kind==='coins')db.prepare('UPDATE chat_groups SET coins=coins+? WHERE id=?').run(amount,id);else db.prepare('UPDATE chat_groups SET fame=fame+? WHERE id=?').run(amount,id);logActivity(req.user.id,null,'group_contribution',`کمک ${amount} به گروه #${id} از نوع ${kind}`);});}catch(e){return res.status(400).json({error:e.message||'تراکنش ناموفق بود؛ تغییری ذخیره نشد'});}res.json({ok:true,user:safeUser(userById(req.user.id))});});
app.post('/api/groups/:id/report',requireAuth,(req,res)=>{const id=Number(req.params.id),reason=String(req.body.reason||'').trim().slice(0,300);if(!db.prepare('SELECT id FROM chat_groups WHERE id=?').get(id))return res.status(404).json({error:'گروه پیدا نشد'});if(!reason)return res.status(400).json({error:'علت گزارش را وارد کن'});db.prepare('INSERT INTO chat_group_reports(group_id,reporter_id,reason) VALUES(?,?,?)').run(id,req.user.id,reason);db.prepare("INSERT INTO reports(reporter_id,category,target_type,target_id,reason,details) VALUES(?,'chat','group',?,?,?)").run(req.user.id,String(id),reason,'گزارش گروه');res.json({ok:true});});
app.post('/api/groups/:id/members/:userId/mute',requireAuth,(req,res)=>{const id=Number(req.params.id);const uid=Number(req.params.userId);if(uid!==req.user.id&&!groupCanManage(id,req.user.id))return res.status(403).json({error:'فقط سازنده یا مدیر می‌تواند اعضا را بی‌صدا کند'});if(!groupMember(id,uid))return res.status(404).json({error:'عضو پیدا نشد'});db.prepare('UPDATE chat_group_members SET muted=? WHERE group_id=? AND user_id=?').run(req.body.muted?1:0,id,uid);res.json({ok:true});});
app.delete('/api/groups/:id',requireAuth,(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});if(g.owner_id!==req.user.id&&!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی حذف گروه ندارید'});db.prepare('DELETE FROM chat_group_messages WHERE group_id=?').run(id);db.prepare('DELETE FROM chat_group_members WHERE group_id=?').run(id);db.prepare('DELETE FROM chat_group_reports WHERE group_id=?').run(id);db.prepare('DELETE FROM chat_groups WHERE id=?').run(id);res.json({ok:true});});
app.get('/api/admin/groups',requireAuth,requirePermission('users.view'),(req,res)=>{const q=String(req.query.q||'').trim();const groups=db.prepare(`SELECT g.*,u.username owner_username,(SELECT COUNT(*) FROM chat_group_members m WHERE m.group_id=g.id) member_count,(SELECT COUNT(*) FROM chat_group_messages x WHERE x.group_id=g.id) message_count FROM chat_groups g LEFT JOIN users u ON u.id=g.owner_id WHERE (?='' OR g.name LIKE ? OR g.group_code LIKE ? OR CAST(g.id AS TEXT)=?) ORDER BY g.id DESC LIMIT 200`).all(q,'%'+q+'%','%'+q+'%',q);res.json({groups});});
app.get('/api/admin/groups/:id',requireAuth,requirePermission('users.view'),(req,res)=>{const id=Number(req.params.id),group=db.prepare('SELECT * FROM chat_groups WHERE id=?').get(id);if(!group)return res.status(404).json({error:'گروه پیدا نشد'});res.json({group,members:db.prepare('SELECT m.*,u.username,u.display_name FROM chat_group_members m JOIN users u ON u.id=m.user_id WHERE m.group_id=?').all(id),messages:db.prepare('SELECT m.*,u.username FROM chat_group_messages m JOIN users u ON u.id=m.sender_id WHERE m.group_id=? ORDER BY m.id DESC LIMIT 300').all(id),reports:db.prepare('SELECT * FROM chat_group_reports WHERE group_id=? ORDER BY id DESC').all(id)});});
app.patch('/api/admin/groups/:id',requireAuth,requirePermission('users.edit'),(req,res)=>{const id=Number(req.params.id),g=db.prepare('SELECT id FROM chat_groups WHERE id=?').get(id);if(!g)return res.status(404).json({error:'گروه پیدا نشد'});if(req.body.name!==undefined)db.prepare('UPDATE chat_groups SET name=? WHERE id=?').run(String(req.body.name).trim().slice(0,60),id);if(req.body.coins!==undefined){const n=Number(req.body.coins);if(!Number.isInteger(n)||n<0)return res.status(400).json({error:'موجودی نامعتبر است'});db.prepare('UPDATE chat_groups SET coins=? WHERE id=?').run(n,id)}if(req.body.fame!==undefined){const n=Number(req.body.fame);if(!Number.isInteger(n)||n<0)return res.status(400).json({error:'شهرت نامعتبر است'});db.prepare('UPDATE chat_groups SET fame=? WHERE id=?').run(n,id)}res.json({ok:true});});
app.get('/api/chats',requireAuth,(req,res)=>{const uid=req.user.id;const cs=db.prepare('SELECT * FROM conversations WHERE closed_at IS NULL AND ((user_a=? AND hidden_a=0) OR (user_b=? AND hidden_b=0))').all(uid,uid).map(c=>{const other=userById(otherId(c,uid));const last=db.prepare('SELECT * FROM messages WHERE conversation_id=? AND deleted=0 ORDER BY id DESC LIMIT 1').get(c.id);const unread=Number(db.prepare('SELECT COUNT(*) n FROM messages WHERE conversation_id=? AND sender_id<>? AND deleted=0 AND read_at IS NULL').get(c.id,uid)?.n||0);return {id:c.id,user:safeUser(other),pinned:uid===c.user_a?!!c.pinned_a:!!c.pinned_b,last,unread,closed:false};}).sort((a,b)=>Number(b.pinned)-Number(a.pinned)||String(b.last?.created_at||'').localeCompare(String(a.last?.created_at||''))||Number(b.last?.id||0)-Number(a.last?.id||0));res.json({chats:cs,unread:unreadSummary(uid)});});
app.get('/api/unread-counts',requireAuth,(req,res)=>res.json(unreadSummary(req.user.id)));
app.post('/api/chats/:id/pin',requireAuth,(req,res)=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(req.params.id));if(!convMember(c,req.user.id))return res.status(404).json({error:'گفتگو پیدا نشد'});const col=req.user.id===c.user_a?'pinned_a':'pinned_b';db.prepare(`UPDATE conversations SET ${col}=1-${col} WHERE id=?`).run(c.id);res.json({ok:true});});
app.delete('/api/chats/:id',requireAuth,(req,res)=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(req.params.id));if(!convMember(c,req.user.id))return res.status(404).json({error:'گفتگو پیدا نشد'});if(conversationClosed(c))return res.json({ok:true,closed:true});db.prepare('UPDATE conversations SET hidden_a=1,hidden_b=1,closed_at=CURRENT_TIMESTAMP,closed_by=? WHERE id=?').run(req.user.id,c.id);for(const uid of [c.user_a,c.user_b])io.to('user:'+uid).emit('conversation:closed',{conversationId:c.id,closedBy:req.user.id});res.json({ok:true,closed:true});});
app.get('/api/chats/:id/messages',requireAuth,(req,res)=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(req.params.id));if(!convMember(c,req.user.id))return res.status(404).json({error:'گفتگو پیدا نشد'});if(conversationClosed(c))return closedConversationResponse(res);if(rejectBlocked(req,res,otherId(c,req.user.id)))return;db.prepare('UPDATE messages SET delivered_at=COALESCE(delivered_at,CURRENT_TIMESTAMP),read_at=CURRENT_TIMESTAMP WHERE conversation_id=? AND sender_id<>? AND read_at IS NULL AND deleted=0').run(c.id,req.user.id);const rows=db.prepare('SELECT * FROM messages WHERE conversation_id=? ORDER BY id ASC LIMIT 300').all(c.id);io.to('user:'+otherId(c,req.user.id)).emit('messageRead',{conversationId:c.id,readerId:req.user.id});io.to('user:'+req.user.id).emit('unread:updated',unreadSummary(req.user.id));io.to('user:'+otherId(c,req.user.id)).emit('unread:updated',unreadSummary(otherId(c,req.user.id)));res.json({messages:rows});});
app.post('/api/chats/:id/messages',requireAuth,enforceRestriction('messages'),enforceRestriction('conversation'),(req,res)=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(req.params.id));if(!convMember(c,req.user.id))return res.status(404).json({error:'گفتگو پیدا نشد'});if(conversationClosed(c))return closedConversationResponse(res);const otherUser=otherId(c,req.user.id);if(db.prepare('SELECT 1 FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(req.user.id,otherUser,otherUser,req.user.id))return res.status(403).json({error:'این گفتگو به‌دلیل مسدودسازی در دسترس نیست'});const body=String(req.body.body||'').slice(0,4000);if(!body.trim())return res.status(400).json({error:'متن پیام خالی است'});const other=otherId(c,req.user.id);const isPro=!!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now());const messagePass=!!(req.user.message_pass_until&&Date.parse(req.user.message_pass_until)>Date.now());const friends=db.prepare("SELECT 1 FROM friend_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,other,other,req.user.id);const acceptedReq=db.prepare("SELECT 1 FROM message_requests WHERE status='accepted' AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))").get(req.user.id,other,other,req.user.id);if(!friends&&!acceptedReq&&!isPro)return res.status(403).json({error:'برای پیام دادن ابتدا درخواست دنبال کردن یا درخواست پیام را بفرستید'});let paidByCoins=false;if(messagePass){/* پیام از اشتراک پیام مصرف می‌شود؛ سهمیه عادی/پرو دست‌نخورده می‌ماند */}else if(isPro){const qe=consumeProQuota(req.user.id,false);if(qe)paidByCoins=true;}else if(!consumeFreeMessageQuota(req.user.id)){paidByCoins=true;}if(paidByCoins){const cost=Math.max(0,Number(getSetting('direct_cost'))||0);const fresh=userById(req.user.id);if(Number(fresh?.coins||0)<cost)return res.status(400).json({error:`سهمیه پیام تمام شده است؛ برای ارسال این پیام ${cost} سکه لازم است`});addTx(req.user.id,-cost,'direct_message','هزینه ارسال پیام پس از اتمام سهمیه');}const replyTo=Number(req.body.replyTo)||null;if(replyTo&&!db.prepare('SELECT id FROM messages WHERE id=? AND conversation_id=? AND deleted=0').get(replyTo,c.id))return res.status(400).json({error:'پیام پاسخ معتبر نیست'});syncUserFameAndLevel(req.user.id,{fame:Math.max(0,Number(userById(req.user.id)?.fame||0),Number(userById(req.user.id)?.xp||0))+20});res.json({message:sendMessage(c,req.user.id,'text',body,'',replyTo)});});
app.post('/api/chats/:id/media',requireAuth,upload.single('file'),(req,res)=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(req.params.id));if(!convMember(c,req.user.id))return res.status(404).json({error:'گفتگو پیدا نشد'});if(conversationClosed(c))return closedConversationResponse(res);if(rejectBlocked(req,res,otherId(c,req.user.id)))return;if(!req.file)return res.status(400).json({error:'فایل نامعتبر است؛ عکس، GIF یا ویس تا ۸ مگابایت'});if(!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now()))return res.status(403).json({error:'ارسال عکس، GIF، استیکر و ویس در دایرکت مخصوص اشتراک پرو است'});const qe=consumeProQuota(req.user.id,false);if(qe)return res.status(429).json({error:qe});const type=req.file.mimetype==='image/gif'?'gif':req.file.mimetype.startsWith('image/')?'image':'audio';res.json({message:sendMessage(c,req.user.id,type,'', '/uploads/'+req.file.filename)});});
app.post('/api/chats/:id/sticker',requireAuth,(req,res)=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(req.params.id));if(!convMember(c,req.user.id))return res.status(404).json({error:'گفتگو پیدا نشد'});if(conversationClosed(c))return closedConversationResponse(res);if(rejectBlocked(req,res,otherId(c,req.user.id)))return;if(!(req.user.pro_until&&Date.parse(req.user.pro_until)>Date.now()))return res.status(403).json({error:'ارسال استیکر مخصوص اشتراک پرو است'});const sticker=String(req.body.sticker||'').slice(0,8);if(!sticker.trim())return res.status(400).json({error:'استیکر معتبر نیست'});const qe=consumeProQuota(req.user.id,false);if(qe)return res.status(429).json({error:qe});res.json({message:sendMessage(c,req.user.id,'sticker',sticker)});});
app.patch('/api/messages/:id',requireAuth,(req,res)=>{const m=db.prepare('SELECT m.*,c.user_a,c.user_b,c.closed_at FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.id=?').get(Number(req.params.id));if(!m||m.sender_id!==req.user.id||m.deleted)return res.status(403).json({error:'فقط پیام حذف‌نشده خودتان را می‌توانید ویرایش کنید'});if(m.closed_at)return closedConversationResponse(res);if(m.type!=='text')return res.status(400).json({error:'فقط پیام متنی قابل ویرایش است'});const body=String(req.body.body||'').trim().slice(0,4000);if(!body)return res.status(400).json({error:'متن پیام خالی است'});db.prepare('UPDATE messages SET body=? WHERE id=?').run(body,m.id);io.to('user:'+m.user_a).emit('messageEdited',{id:m.id,body});io.to('user:'+m.user_b).emit('messageEdited',{id:m.id,body});res.json({ok:true});});
app.delete('/api/messages/:id',requireAuth,(req,res)=>{const m=db.prepare('SELECT m.*,c.user_a,c.user_b,c.closed_at FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.id=?').get(Number(req.params.id));if(!m||m.sender_id!==req.user.id)return res.status(403).json({error:'فقط پیام خودتان را می‌توانید حذف کنید'});if(m.closed_at)return closedConversationResponse(res);db.prepare('UPDATE messages SET deleted=1,body="",media_url="" WHERE id=?').run(m.id);io.to('user:'+m.user_a).emit('messageDeleted',{id:m.id});io.to('user:'+m.user_b).emit('messageDeleted',{id:m.id});io.to('user:'+m.user_a).emit('unread:updated',unreadSummary(m.user_a));io.to('user:'+m.user_b).emit('unread:updated',unreadSummary(m.user_b));res.json({ok:true});});
app.get('/api/announcement',requireAuth,(req,res)=>{const title=db.prepare("SELECT value FROM settings WHERE key='home_announcement_title'").get()?.value||'خبرهای مهم ACE CLUB';const body=db.prepare("SELECT value FROM settings WHERE key='home_announcement_body'").get()?.value||'از همراهی‌ات خوشحالیم؛ از امکانات کلاب لذت ببر.';const active=db.prepare("SELECT value FROM settings WHERE key='home_announcement_active'").get()?.value!=='0';res.json({title,body,active});});
app.get('/api/notification-settings',requireAuth,(req,res)=>{const row=db.prepare('SELECT preferences_json FROM user_notification_settings WHERE user_id=?').get(req.user.id);let preferences={};try{preferences=JSON.parse(row?.preferences_json||'{}')}catch{};const keys=['messages','coins','profile_views','likes','friends','games','support','important','system'];for(const k of keys)if(preferences[k]===undefined)preferences[k]=true;res.json({preferences});});
app.put('/api/notification-settings',requireAuth,(req,res)=>{const allowed=['messages','coins','profile_views','likes','friends','games','support','important','system'];const old=db.prepare('SELECT preferences_json FROM user_notification_settings WHERE user_id=?').get(req.user.id);let prefs={};try{prefs=JSON.parse(old?.preferences_json||'{}')}catch{};for(const k of allowed)if(typeof req.body?.preferences?.[k]==='boolean')prefs[k]=req.body.preferences[k];db.prepare('INSERT INTO user_notification_settings(user_id,preferences_json,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(user_id) DO UPDATE SET preferences_json=excluded.preferences_json,updated_at=CURRENT_TIMESTAMP').run(req.user.id,JSON.stringify(prefs));res.json({ok:true,preferences:prefs});});
app.get('/api/notifications',requireAuth,(req,res)=>{const rows=db.prepare('SELECT * FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 100').all(req.user.id);db.prepare('UPDATE notifications SET read_at=CURRENT_TIMESTAMP WHERE user_id=? AND read_at IS NULL').run(req.user.id);res.json({notifications:rows});});
app.delete('/api/notifications/all',requireAuth,(req,res)=>{const r=db.prepare('DELETE FROM notifications WHERE user_id=?').run(req.user.id);res.json({ok:true,deleted:r.changes||0});});
app.delete('/api/notifications/:id',requireAuth,(req,res)=>{const r=db.prepare('DELETE FROM notifications WHERE id=? AND user_id=?').run(Number(req.params.id),req.user.id);if(!r.changes)return res.status(404).json({error:'اعلان پیدا نشد'});res.json({ok:true});});
app.get('/api/transactions',requireAuth,(req,res)=>res.json({transactions:db.prepare('SELECT * FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT 100').all(req.user.id)}));
app.post('/api/wheel',requireAuth,(req,res)=>{const uid=req.user.id;const today=db.prepare("SELECT id FROM wheel_claims WHERE user_id=? AND date(created_at)=date('now')").get(uid);if(today)return res.status(429).json({error:'گردونه را امروز چرخانده‌اید'});const prizes=[{label:'۲۰ سکه',type:'coins',amount:20},{label:'۵۰ سکه',type:'coins',amount:50},{label:'۱۰۰ سکه',type:'coins',amount:100},{label:'۵ الماس',type:'diamonds',amount:5},{label:'۵۰ شهرت',type:'fame',amount:50},{label:'اشتراک پرو ۱ روزه',type:'pro',amount:1}];const p=prizes[crypto.randomInt(prizes.length)];if(p.type==='coins')addTx(uid,p.amount,'wheel','گردونه شانس');if(p.type==='xp'){const before=Number(userById(uid).level||1);syncUserFameAndLevel(uid,{fame:Math.max(0,Number(userById(uid)?.fame||0),Number(userById(uid)?.xp||0))+p.amount});recalcGameLevel(uid);const after=Number(userById(uid).level||1),reward=Math.max(0,Number(getSetting('level_diamond_reward'))||0)*(after-before);if(reward>0){db.prepare('UPDATE users SET diamonds=diamonds+? WHERE id=?').run(reward,uid);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(uid,reward,'level_reward',`جایزه ارتقای ${after-before} سطح`);}}if(p.type==='fame')syncUserFameAndLevel(uid,{fame:Math.max(0,Number(userById(uid)?.fame||0),Number(userById(uid)?.xp||0))+p.amount});if(p.type==='diamonds'){db.prepare('UPDATE users SET diamonds=diamonds+? WHERE id=?').run(p.amount,uid);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(uid,p.amount,'wheel_diamonds','جایزه گردونه');}db.prepare('INSERT INTO wheel_claims(user_id,prize) VALUES(?,?)').run(uid,p.label);notify(uid,'جایزه گردونه',`جایزه شما: ${p.label}`);res.json({prize:p.label,type:p.type,user:safeUser(userById(uid))});});
app.get('/api/pro/status',requireAuth,(req,res)=>{
 const u=userById(req.user.id), now=Date.now(), active=x=>x&&Date.parse(x)>now;
 const windows=[['8h',8*3600000,Number(getSetting('pro_quota_8h')||40)],['24h',24*3600000,Number(getSetting('pro_quota_24h')||100)],['7d',7*86400000,Number(getSetting('pro_quota_7d')||400)]];
 const quota=windows.map(([key,ms,total])=>{const row=db.prepare('SELECT * FROM pro_usage WHERE user_id=? AND window_key=?').get(u.id,key);const fresh=!row||now-Date.parse(row.window_start)>=ms;const used=fresh?0:row.used;return {key,total,remaining:Math.max(0,total-used),used,resetAt:fresh?null:new Date(Date.parse(row.window_start)+ms).toISOString()};});
 const chat=db.prepare('SELECT * FROM pro_usage WHERE user_id=? AND window_key=?').get(u.id,'chat24');const chatFresh=!chat||now-Date.parse(chat.window_start)>=86400000;
 res.json({active:active(u.pro_until),proUntil:u.pro_until,boostActive:active(u.boost_until),boostUntil:u.boost_until,messagePassActive:active(u.message_pass_until),messagePassUntil:u.message_pass_until,faceVerified:!!u.face_verified,quota,chatRequestsRemaining:Math.max(0,Number(getSetting('pro_chat_requests')||2)-(chatFresh?0:chat.used)),chatResetAt:chatFresh?null:new Date(Date.parse(chat.window_start)+86400000).toISOString(),features:['مشاهده لایک‌کنندگان','نمایش استان و شهر در اکسپلور','Undo در اکسپلور','دو فیلتر اضافه','آنبلاک رایگان','مشاهده لایک‌های ارسالی','پاک‌کردن دیسلایک‌ها','دو برابر سهمیه اکسپلور','اولویت در اکسپلور و مچ‌ها','ارسال GIF، استیکر، عکس و ویس در دایرکت','دایرکت با سهمیه مصرف منصفانه','افزایش شانس مچ شدن و ویدیوی پروفایل (به‌زودی)','ساخت بازی عمومی و حذف تبلیغات (به‌زودی)']});
});
app.post('/api/economy/buy-coins',requireAuth,(req,res)=>{const id=Number(req.body.packId);if(!Number.isInteger(id)||id<1||id>4)return res.status(400).json({error:'بسته نامعتبر است'});const coins=Number(getSetting('coin_pack_'+id+'_coins')),cost=Number(getSetting('coin_pack_'+id+'_diamonds'));if(!Number.isSafeInteger(coins)||coins<1||!Number.isSafeInteger(cost)||cost<1)return res.status(400).json({error:'تنظیمات بسته معتبر نیست'});const u=userById(req.user.id);if(Number(u.diamonds||0)<cost)return res.status(400).json({error:'الماس کافی ندارید'});db.prepare('UPDATE users SET diamonds=diamonds-?,coins=coins+? WHERE id=?').run(cost,coins,u.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(u.id,-cost,'coin_pack',`خرید ${coins} سکه با ${cost} الماس`);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(u.id,coins,'coin_pack_received',`دریافت ${coins} سکه از بسته ${id}`);res.json({ok:true,user:safeUser(userById(u.id))});});
app.post('/api/economy/exchange',requireAuth,(req,res)=>{const diamonds=Math.floor(Number(req.body.diamonds));const rate=Math.max(1,Math.floor(Number(getSetting('diamond_coin_rate'))||10));if(!Number.isSafeInteger(diamonds)||diamonds<1)return res.status(400).json({error:'تعداد الماس معتبر نیست'});if(Number(req.user.diamonds||0)<diamonds)return res.status(400).json({error:'الماس کافی ندارید'});db.prepare('UPDATE users SET diamonds=diamonds-?, coins=coins+? WHERE id=?').run(diamonds,diamonds*rate,req.user.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(req.user.id,diamonds*rate,'diamond_exchange',`${diamonds} الماس به سکه تبدیل شد`);res.json({ok:true,user:safeUser(userById(req.user.id)),coinsReceived:diamonds*rate});});
app.post('/api/economy/transfer',requireAuth,enforceRestriction('diamond_transfer'),(req,res)=>{const toId=Number(req.body.userId),amount=Math.floor(Number(req.body.diamonds));if(!Number.isSafeInteger(amount)||amount<Math.max(20,Number(getSetting('diamond_transfer_min'))||20))return res.status(400).json({error:`حداقل انتقال ${Math.max(20,Number(getSetting('diamond_transfer_min'))||20)} الماس است`});const target=userById(toId);if(!target||toId===req.user.id)return res.status(400).json({error:'شناسه کاربر معتبر نیست'});if(Number(req.user.diamonds||0)<amount)return res.status(400).json({error:'الماس کافی ندارید'});db.prepare('UPDATE users SET diamonds=diamonds-? WHERE id=?').run(amount,req.user.id);db.prepare('UPDATE users SET diamonds=diamonds+? WHERE id=?').run(amount,toId);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(req.user.id,-amount,'diamond_transfer',`انتقال ${amount} الماس به شناسه ${toId}`);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(toId,amount,'diamond_received',`دریافت ${amount} الماس از شناسه ${req.user.id}`);notify(toId,'دریافت الماس',`${amount} الماس از کاربر ${req.user.id} دریافت کردید`);res.json({ok:true,user:safeUser(userById(req.user.id))});});
const ACE_COSMETICS=(()=>{
 const out=[
  {id:'frame_neon',name:'قاب نئون',kind:'frame',price:200,css:'neon',description:'قاب نئونی بنفش برای آواتار'},
  {id:'frame_gold',name:'قاب طلایی',kind:'frame',price:500,css:'gold',description:'قاب طلایی ویژه پروفایل'},
  {id:'frame_ice',name:'قاب یخی',kind:'frame',price:350,css:'ice',description:'قاب آبی یخی'},
 ];
 const palettes=['neon','gold','ice','rose','ruby','emerald','rainbow','cosmic','plasma','aurora','sakura','sunset','ocean','royal','chrome','fire','mint','violet','opal','galaxy'];
 const names=['نئون بنفش','طلایی سلطنتی','یخی کریستالی','رز درخشان','یاقوت آتشین','زمردی','رنگین‌کمانی','کیهانی','پلاسما','شفق قطبی','ساکورا','غروب','اقیانوسی','سلطنتی','کرومی','شعله‌ای','نعنایی','بنفش کهکشانی','اوپال','کهکشان'];
 for(let i=0;i<300;i++){
  const n=i%20, tier=Math.floor(i/20), kind=i<200?'frame':i<260?'namecolor':'badge';
  const id=kind+'_'+String(i+1).padStart(3,'0');
  out.push({id,name:kind==='frame'?`قاب ${names[n]} ${tier+1}`:kind==='namecolor'?`نام کاربری ${names[n]}`:`نشان ACE ${names[n]}`,kind,price:kind==='frame'?200+tier*45+n*15:kind==='namecolor'?10+tier*5+n*2:500+tier*75+n*20,css:palettes[n],description:kind==='frame'?'قاب تزئینی متحرک دور آواتار':kind==='namecolor'?'رنگ ویژه برای نام کاربری':'نشان اختصاصی ایس کلاب'});
 }
 return out;
})();
 db.exec('CREATE TABLE IF NOT EXISTS user_cosmetic_loadout(user_id INTEGER NOT NULL,kind TEXT NOT NULL,item_id TEXT NOT NULL,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,kind))');
 db.exec('CREATE TABLE IF NOT EXISTS store_admin_grants(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,kind TEXT NOT NULL,item_id TEXT NOT NULL,starts_at TEXT DEFAULT CURRENT_TIMESTAMP,expires_at TEXT,source TEXT DEFAULT \'admin\',note TEXT DEFAULT \'\')');
 db.exec('CREATE TABLE IF NOT EXISTS user_store_offers(user_id INTEGER NOT NULL,item_id TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,item_id))');
app.get('/api/cosmetics',requireAuth,(req,res)=>{const owned=db.prepare('SELECT item_id FROM user_cosmetics WHERE user_id=?').all(req.user.id).map(x=>x.item_id);const offered=db.prepare('SELECT item_id FROM user_store_offers WHERE user_id=?').all(req.user.id).map(x=>x.item_id);const loadout=Object.fromEntries(db.prepare('SELECT kind,item_id FROM user_cosmetic_loadout WHERE user_id=?').all(req.user.id).map(x=>[x.kind,x.item_id]));const equipped=db.prepare('SELECT frame_id FROM user_cosmetic_equips WHERE user_id=?').get(req.user.id)?.frame_id||null;res.json({items:ACE_COSMETICS.map(x=>({...x,currency:x.kind==='frame'?'coins':'diamonds',owned:owned.includes(x.id),offered:offered.includes(x.id),equipped:(loadout[x.kind]===x.id)||(x.kind==='frame'&&equipped===x.id)})),equipped,loadout,coins:Number(userById(req.user.id)?.coins||0),diamonds:Number(userById(req.user.id)?.diamonds||0)});});
app.post('/api/cosmetics/buy',requireAuth,(req,res)=>{
 const item=ACE_COSMETICS.find(x=>x.id===String(req.body.itemId||''));
 if(!item)return res.status(400).json({error:'آیتم فروشگاه معتبر نیست'});
 if(db.prepare('SELECT 1 FROM user_cosmetics WHERE user_id=? AND item_id=?').get(req.user.id,item.id))return res.status(409).json({error:'این آیتم را قبلاً خریده‌ای'});
 const currency=item.kind==='frame'?'coins':'diamonds',u=userById(req.user.id);
 if(Number(u?.[currency]||0)<item.price)return res.status(400).json({error:currency==='coins'?'سکه کافی نداری':'الماس کافی نداری'});
 try{atomic(()=>{
  const col=currency==='coins'?'coins':'diamonds';
  const changed=db.prepare(`UPDATE users SET ${col}=${col}-? WHERE id=? AND ${col}>=?`).run(item.price,req.user.id,item.price);
  if(!changed.changes)throw new Error(currency==='coins'?'سکه کافی نداری':'الماس کافی نداری');
  db.prepare('INSERT INTO user_cosmetics(user_id,item_id) VALUES(?,?)').run(req.user.id,item.id);
  db.prepare('DELETE FROM user_store_offers WHERE user_id=? AND item_id=?').run(req.user.id,item.id);
  db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(req.user.id,-item.price,currency==='coins'?'cosmetic_purchase_coins':'cosmetic_purchase',`خرید آیتم ظاهری با ${currency==='coins'?'سکه':'الماس'}: ${item.name}`);
 });}catch(e){return res.status(409).json({error:e.message||'خرید ثبت نشد؛ دوباره تلاش کن'});}
 notify(req.user.id,'خرید موفق',`آیتم «${item.name}» به مجموعه‌ات اضافه شد`);
 res.json({ok:true,currency,user:safeUser(userById(req.user.id)),item});
});
app.patch('/api/cosmetics/equip',requireAuth,(req,res)=>{const id=String(req.body.itemId||'');if(id==='none'){db.prepare('DELETE FROM user_cosmetic_equips WHERE user_id=?').run(req.user.id);db.prepare('DELETE FROM user_cosmetic_loadout WHERE user_id=?').run(req.user.id);return res.json({ok:true,user:safeUser(userById(req.user.id))});}const item=ACE_COSMETICS.find(x=>x.id===id);if(!item||!db.prepare('SELECT 1 FROM user_cosmetics WHERE user_id=? AND item_id=?').get(req.user.id,id))return res.status(400).json({error:'این آیتم را نخریده‌ای'});db.prepare('INSERT INTO user_cosmetic_loadout(user_id,kind,item_id) VALUES(?,?,?) ON CONFLICT(user_id,kind) DO UPDATE SET item_id=excluded.item_id,updated_at=CURRENT_TIMESTAMP').run(req.user.id,item.kind,id);if(item.kind==='frame')db.prepare('INSERT INTO user_cosmetic_equips(user_id,frame_id) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET frame_id=excluded.frame_id,updated_at=CURRENT_TIMESTAMP').run(req.user.id,id);res.json({ok:true,user:safeUser(userById(req.user.id)),item});});
app.get('/api/admin/store/user',requireAuth,(req,res)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط سازنده به مدیریت فروشگاه دسترسی دارد'});const q=String(req.query.q||'').trim();if(!q)return res.status(400).json({error:'شناسه یا کد کاربر را وارد کن'});const u=/^\d+$/.test(q)?userById(Number(q)):userByName(q.replace(/^@/,''));if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});const grants=db.prepare('SELECT * FROM store_admin_grants WHERE user_id=? ORDER BY id DESC').all(u.id);const owned=db.prepare('SELECT item_id,owned_at FROM user_cosmetics WHERE user_id=?').all(u.id).map(x=>({...x,item:ACE_COSMETICS.find(c=>c.id===x.item_id)||{id:x.item_id,name:x.item_id,kind:'unknown'}}));res.json({user:{id:u.id,username:u.username,display_name:u.display_name,coins:u.coins,diamonds:u.diamonds,pro_until:u.pro_until,boost_until:u.boost_until,message_pass_until:u.message_pass_until},grants,owned,items:ACE_COSMETICS});});
app.patch('/api/admin/store/user/:id',requireAuth,(req,res)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط سازنده به مدیریت فروشگاه دسترسی دارد'});const id=Number(req.params.id);if(!userById(id))return res.status(404).json({error:'کاربر پیدا نشد'});for(const col of ['pro_until','boost_until','message_pass_until']){if(Object.prototype.hasOwnProperty.call(req.body,col)){const v=req.body[col];if(v===null||v===''){db.prepare(`UPDATE users SET ${col}=NULL WHERE id=?`).run(id)}else{const d=new Date(v);if(!Number.isFinite(d.getTime()))return res.status(400).json({error:'تاریخ پایان معتبر نیست'});db.prepare(`UPDATE users SET ${col}=? WHERE id=?`).run(d.toISOString(),id)}}}logActivity(req.user.id,id,'store_subscription_edited','ویرایش پایان اشتراک');res.json({ok:true,user:safeUser(userById(id))});});
app.post('/api/admin/store/grant',requireAuth,(req,res)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط سازنده به مدیریت فروشگاه دسترسی دارد'});const userId=Number(req.body.userId),kind=String(req.body.kind||''),itemId=String(req.body.itemId||''),days=Math.max(0,Math.min(3650,Number(req.body.days)||0));const u=userById(userId);if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});if(['frame','namecolor','badge'].includes(kind)){const item=ACE_COSMETICS.find(x=>x.id===itemId&&x.kind===kind);if(!item)return res.status(400).json({error:'آیتم معتبر نیست'});if(String(req.body.mode||'gift')==='offer'){db.prepare('INSERT OR IGNORE INTO user_store_offers(user_id,item_id) VALUES(?,?)').run(userId,itemId);db.prepare('INSERT INTO store_admin_grants(user_id,kind,item_id,expires_at,note) VALUES(?,?,?,?,?)').run(userId,'offer_'+kind,itemId,null,String(req.body.note||'پیشنهاد اختصاصی فروشگاه'));}else{db.prepare('INSERT OR IGNORE INTO user_cosmetics(user_id,item_id) VALUES(?,?)').run(userId,itemId);db.prepare('INSERT INTO store_admin_grants(user_id,kind,item_id,expires_at,note) VALUES(?,?,?,?,?)').run(userId,kind,itemId,null,String(req.body.note||'هدیه مدیریت'));}}else if(['pro','boost','message_pass'].includes(kind)){const col={pro:'pro_until',boost:'boost_until',message_pass:'message_pass_until'}[kind],duration=days||30;const until=new Date(Date.now()+duration*86400000).toISOString();db.prepare(`UPDATE users SET ${col}=? WHERE id=?`).run(until,userId);db.prepare('INSERT INTO store_admin_grants(user_id,kind,item_id,expires_at,note) VALUES(?,?,?,?,?)').run(userId,kind,kind,until,String(req.body.note||'فعال‌سازی مدیریت'));}else return res.status(400).json({error:'نوع محصول نامعتبر است'});notify(userId,'هدیه از مدیریت فروشگاه','یک آیتم یا اشتراک برای حساب شما فعال شد');logActivity(req.user.id,userId,'store_grant',`${kind}:${itemId}`);res.json({ok:true});});
app.post('/api/store/buy',requireAuth,(req,res)=>{
 const raw=String(req.body.kind||'');const match=raw.match(/^(pro|boost|message_pass)(?:_(1|3|7|30))?$/);if(!match)return res.status(400).json({error:'محصول نامعتبر است'});const kind=match[1],days=Number(match[2]||({pro:30,boost:1,message_pass:7}[kind]));const base={pro:Number(getSetting('pro_cost'))||750,boost:Number(getSetting('boost_cost'))||250,message_pass:200}[kind];const price=Math.max(1,Math.ceil(base*days/({pro:30,boost:1,message_pass:7}[kind])));const col={pro:'pro_until',boost:'boost_until',message_pass:'message_pass_until'}[kind];const u=userById(req.user.id);const active=u[col]&&Date.parse(u[col])>Date.now();if(active)return res.status(409).json({error:`این اشتراک را هنوز داری و تا ${u[col]} فعال است؛ خرید دوباره انجام نشد.`});if(kind==='boost'&&getSetting('boost_enabled')==='0')return res.status(400).json({error:'شتاب‌دهی فعلاً غیرفعال است'});if(Number(u.coins)<price)return res.status(400).json({error:'سکه کافی ندارید'});try{atomic(()=>{const changed=db.prepare('UPDATE users SET coins=coins-? WHERE id=? AND coins>=?').run(price,u.id,price);if(!changed.changes)throw new Error('سکه کافی ندارید');db.prepare('UPDATE users SET '+col+'=? WHERE id=?').run(new Date(Date.now()+days*86400000).toISOString(),u.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(u.id,-price,'purchase',`${kind} · ${days} روز`);});}catch(e){return res.status(409).json({error:e.message||'خرید ثبت نشد'});}notify(u.id,'خرید موفق',`${kind==='pro'?'اشتراک پرو':kind==='boost'?'شتاب‌دهی':'اشتراک پیام'} به مدت ${days} روز فعال شد`);res.json({ok:true,user:safeUser(userById(u.id)),expiresAt:userById(u.id)[col]});});
// Verification is deliberately staff-controlled; users cannot self-verify by editing a request.
app.patch('/api/admin/users/:id/face-verification',requireAuth,requirePermission('users.edit'),(req,res)=>{const id=Number(req.params.id);if(!userById(id))return res.status(404).json({error:'کاربر پیدا نشد'});if(!canUserField(req.user,'edit','ban'))return res.json({ok:true,unchanged:true});const verified=!!req.body.verified;db.prepare('UPDATE users SET face_verified=? WHERE id=?').run(verified?1:0,id);notify(id,'وضعیت احراز چهره',verified?'احراز چهره حساب شما تأیید شد':'وضعیت احراز چهره حساب شما تغییر کرد');res.json({ok:true});});
app.post('/api/tickets',requireAuth,(req,res)=>{const subject=String(req.body.subject||'').slice(0,120),body=String(req.body.body||'').slice(0,3000);if(!subject||!body)return res.status(400).json({error:'عنوان و متن تیکت لازم است'});const r=db.prepare('INSERT INTO tickets(user_id,subject) VALUES(?,?)').run(req.user.id,subject);db.prepare('INSERT INTO ticket_messages(ticket_id,sender_id,body) VALUES(?,?,?)').run(r.lastInsertRowid,req.user.id,body);notify(req.user.id,'تیکت ثبت شد','درخواست شما ثبت شد');for(const staff of db.prepare("SELECT id FROM users WHERE role IN ('owner','creator','admin','support','moderator')").all())notify(staff.id,'تیکت جدید',`تیکت #${r.lastInsertRowid}: ${subject}`,{action:'ticket',requestId:r.lastInsertRowid,fromId:req.user.id});res.json({ok:true,ticketId:r.lastInsertRowid});});
app.get('/api/tickets',requireAuth,(req,res)=>{const own=['owner','creator'].includes(req.user.role);const staffAllowed=own||db.prepare("SELECT 1 FROM user_permissions WHERE user_id=? AND permission IN ('tickets.view','tickets.reply') AND enabled=1 LIMIT 1").get(req.user.id);const tickets=isStaff(req.user)&&staffAllowed?db.prepare('SELECT t.*,u.username FROM tickets t JOIN users u ON u.id=t.user_id ORDER BY t.id DESC').all():db.prepare('SELECT * FROM tickets WHERE user_id=? ORDER BY id DESC').all(req.user.id);res.json({tickets});});
app.patch('/api/tickets/:id/status',requireAuth,requirePermission('tickets.view'),(req,res)=>{const id=Number(req.params.id),status=String(req.body.status||'');if(!['open','pending','closed'].includes(status))return res.status(400).json({error:'وضعیت نامعتبر است'});const t=db.prepare('SELECT * FROM tickets WHERE id=?').get(id);if(!t)return res.status(404).json({error:'تیکت پیدا نشد'});db.prepare('UPDATE tickets SET status=? WHERE id=?').run(status,id);notify(t.user_id,'تغییر وضعیت تیکت',`وضعیت تیکت #${id}: ${{open:'باز',pending:'در انتظار',closed:'بسته'}[status]}`);res.json({ok:true,status});});
app.get('/api/tickets/:id/messages',requireAuth,(req,res)=>{const t=db.prepare('SELECT * FROM tickets WHERE id=?').get(Number(req.params.id));if(!t||(!isStaff(req.user)&&t.user_id!==req.user.id))return res.status(404).json({error:'تیکت پیدا نشد'});if(isStaff(req.user)&&!['owner','creator'].includes(req.user.role)&&!db.prepare("SELECT 1 FROM user_permissions WHERE user_id=? AND permission IN ('tickets.view','tickets.reply') AND enabled=1 LIMIT 1").get(req.user.id))return res.status(403).json({error:'مجوز مشاهده تیکت ندارید'});res.json({messages:db.prepare('SELECT * FROM ticket_messages WHERE ticket_id=? ORDER BY id').all(t.id)});});
app.post('/api/tickets/:id/messages',requireAuth,(req,res)=>{const t=db.prepare('SELECT * FROM tickets WHERE id=?').get(Number(req.params.id));if(!t||(!isStaff(req.user)&&t.user_id!==req.user.id))return res.status(404).json({error:'تیکت پیدا نشد'});if(isStaff(req.user)&&!['owner','creator'].includes(req.user.role)&&!db.prepare("SELECT 1 FROM user_permissions WHERE user_id=? AND permission='tickets.reply' AND enabled=1").get(req.user.id))return res.status(403).json({error:'مجوز پاسخ به تیکت ندارید'});const body=String(req.body.body||'').slice(0,3000);if(!body.trim())return res.status(400).json({error:'پیام خالی است'});db.prepare('INSERT INTO ticket_messages(ticket_id,sender_id,body) VALUES(?,?,?)').run(t.id,req.user.id,body);if(isStaff(req.user))notify(t.user_id,'پاسخ پشتیبانی','پشتیبانی به تیکت شما پاسخ داد');else notify(Number(db.prepare("SELECT id FROM users WHERE role IN ('owner','creator','admin','support') ORDER BY CASE role WHEN 'owner' THEN 0 ELSE 1 END LIMIT 1").get()?.id||req.user.id),'تیکت جدید','یک تیکت نیازمند پاسخ است');res.json({ok:true});});
app.delete('/api/tickets/:id',requireAuth,requirePermission('tickets.delete'),(req,res)=>{const id=Number(req.params.id);db.prepare('DELETE FROM ticket_messages WHERE ticket_id=?').run(id);db.prepare('DELETE FROM tickets WHERE id=?').run(id);res.json({ok:true});});
app.post('/api/tickets/:id/close',requireAuth,requirePermission('tickets.view'),(req,res)=>{db.prepare("UPDATE tickets SET status='closed' WHERE id=?").run(Number(req.params.id));res.json({ok:true});});
// Public ACE CLUB logo configuration; mutation is restricted to owner/creator.
const logoConfig=()=>{try{return JSON.parse(getSetting('site_logo_config')||'{}')}catch{return {}}};
app.get('/api/logo',(req,res)=>{const c=logoConfig();res.set('Cache-Control','no-store').json({logoUrl:c.logoUrl||'/icon-512.png',colors:Array.isArray(c.colors)&&c.colors.length===4?c.colors:['#ff3b8d','#ff304f','#9b5cff','#ff9f43'],intensity:Math.max(0,Math.min(100,Number(c.intensity??70)))});});
app.post('/api/admin/logo/upload',requireAuth,(req,res,next)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط مالک سایت می‌تواند لوگو را تغییر دهد'});next()},logoUpload.single('file'),(req,res)=>{if(!req.file)return res.status(400).json({error:'یک تصویر PNG، JPG، WEBP یا GIF انتخاب کن (حداکثر ۸ مگابایت)'});const c=logoConfig();c.logoUrl='/uploads/'+req.file.filename;setSetting('site_logo_config',JSON.stringify(c));res.json({ok:true,config:logoConfig()});});
app.patch('/api/admin/logo/config',requireAuth,(req,res)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط مالک سایت می‌تواند ظاهر لوگو را تغییر دهد'});const old=logoConfig();const colors=Array.isArray(req.body.colors)?req.body.colors.slice(0,4):old.colors;const validColor=x=>typeof x==='string'&&/^#[0-9a-fA-F]{6}$/.test(x);if(!Array.isArray(colors)||colors.length!==4||!colors.every(validColor))return res.status(400).json({error:'چهار رنگ معتبر انتخاب کن'});const intensity=Number(req.body.intensity);if(!Number.isFinite(intensity)||intensity<0||intensity>100)return res.status(400).json({error:'شدت نور باید بین صفر تا صد باشد'});const config={...old,colors,intensity};setSetting('site_logo_config',JSON.stringify(config));res.json({ok:true,config});});
app.delete('/api/admin/logo',requireAuth,(req,res)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط مالک سایت می‌تواند لوگو را حذف کند'});const c=logoConfig();c.logoUrl='';setSetting('site_logo_config',JSON.stringify(c));res.json({ok:true,config:c});});

// Global music radio: upload/control is restricted to the original owner/creator.
const musicState=()=>{try{return JSON.parse(getSetting('global_music_state')||'{}')}catch{return {}}};
const isCreator=u=>u&&['owner','creator'].includes(u.role);
app.get('/api/music/state',requireAuth,(req,res)=>{const st=musicState();res.json({music:st,muted:st.mutedByUser?.includes(req.user.id)||false});});
app.post('/api/admin/music/upload',requireAuth,(req,res,next)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط مالک برنامه می‌تواند آهنگ آپلود کند'});next()},musicUpload.single('file'),(req,res)=>{
 if(!req.file)return res.status(400).json({error:'یک فایل صوتی معتبر انتخاب کن (حداکثر ۵۰ مگابایت)'});
 const old=musicState();const st={url:'/uploads/'+req.file.filename,title:String(req.file.originalname||'موسیقی سراسری').replace(/[<>]/g,'').slice(0,120),playing:false,startedAt:null,updatedAt:Date.now()};setSetting('global_music_state',JSON.stringify(st));io.emit('music:state',st);res.json({ok:true,music:st});
});
app.post('/api/admin/music/control',requireAuth,(req,res)=>{if(!isCreator(req.user))return res.status(403).json({error:'فقط مالک برنامه می‌تواند پخش را کنترل کند'});const old=musicState();if(!old.url)return res.status(400).json({error:'ابتدا یک آهنگ آپلود کن'});const action=String(req.body.action||'');if(!['play','pause','stop'].includes(action))return res.status(400).json({error:'دستور پخش نامعتبر است'});const pausedPosition=old.playing&&old.startedAt?Math.max(0,Number(old.position||0)+(Date.now()-Number(old.startedAt))/1000):Math.max(0,Number(old.position||0));let st={...old,playing:action==='play',position:action==='stop'?0:(action==='pause'?pausedPosition:pausedPosition),startedAt:action==='play'?Date.now()-pausedPosition*1000:null,updatedAt:Date.now()};if(action==='stop')st={...st,position:0};setSetting('global_music_state',JSON.stringify(st));io.emit('music:state',st);res.json({ok:true,music:st});});
app.post('/api/music/mute',requireAuth,(req,res)=>{const muted=req.body.muted===true;const st=musicState();res.json({ok:true,muted});});
app.get('/api/admin/users/search',requireAuth,requirePermission('users.view'),(req,res)=>{
 const q=String(req.query.q||'').trim().slice(0,80), userId=String(req.query.userId||'').trim().slice(0,40), code=String(req.query.code||'').trim().slice(0,80), display=String(req.query.display||'').trim().slice(0,100), device=String(req.query.deviceId||'').trim().slice(0,100), minLevel=Number(req.query.minLevel||0), maxLevel=Number(req.query.maxLevel||0), banned=String(req.query.banned||'any'), gameBanned=String(req.query.gameBanned||'any'), limit=Math.min(100,Math.max(1,Number(req.query.limit)||25));
 const where=[],params=[];
 const add=(sql,val)=>{where.push(sql);params.push(val)};
 if(userId)add('CAST(id AS TEXT) LIKE ?', '%'+userId+'%');
 if(code)add('username LIKE ?', '%'+code+'%');
 if(display)add('display_name LIKE ?', '%'+display+'%');
 if(device)add('device_id LIKE ?', '%'+device+'%');
 if(minLevel>0){where.push('level >= ?');params.push(minLevel)}
 if(maxLevel>0){where.push('level <= ?');params.push(maxLevel)}
 if(banned==='yes')where.push('is_banned=1');else if(banned==='no')where.push('is_banned=0');
 if(gameBanned==='yes')where.push('COALESCE(is_game_banned,0)=1');else if(gameBanned==='no')where.push('COALESCE(is_game_banned,0)=0');
 if(q){const id=Number(q);where.push('(username LIKE ? OR display_name LIKE ? OR device_id LIKE ? OR (? > 0 AND id=?))');params.push('%'+q+'%','%'+q+'%','%'+q+'%',id,id)}
 const sqlWhere=where.length?'WHERE '+where.join(' AND '):'';
 const total=Number(db.prepare(`SELECT COUNT(*) AS total FROM users ${sqlWhere}`).get(...params).total||0);
 const totalPages=Math.max(1,Math.ceil(total/limit));
 const page=Math.min(totalPages,Math.max(1,Math.floor(Number(req.query.page)||1)));
 const offset=(page-1)*limit;
 const users=db.prepare(`SELECT id,username,display_name,phone,age,province,coins,diamonds,xp,fame,level,role,is_banned,is_game_banned,game_ban_until,game_exit_streak,ban_reason,ban_until,created_at,last_seen,COALESCE(device_id,'') AS device_id,(SELECT COUNT(*) FROM users u2 WHERE COALESCE(u2.device_id,'')<>'' AND u2.device_id=users.device_id) AS device_account_count FROM users ${sqlWhere} ORDER BY id DESC LIMIT ? OFFSET ?`).all(...params,limit,offset);
 res.json({total,totalPages,page,pageSize:limit,users:users.map(u=>{const x={...u};if(!canUserField(req.user,'view','coins'))delete x.coins;if(!canUserField(req.user,'view','diamonds'))delete x.diamonds;if(!canUserField(req.user,'view','xp')){delete x.xp;delete x.fame}if(!canUserField(req.user,'view','level'))delete x.level;if(!canUserField(req.user,'view','role'))delete x.role;if(!canUserField(req.user,'view','phone'))delete x.phone;if(!canUserField(req.user,'view','age'))delete x.age;if(!canUserField(req.user,'view','province'))delete x.province;if(!canUserField(req.user,'view','display_name'))delete x.display_name;if(!canUserField(req.user,'view','ban')){delete x.is_banned;delete x.is_game_banned;delete x.ban_reason;delete x.ban_until;}if(!canUserField(req.user,'view','device_id')){delete x.device_id;delete x.device_account_count}return x;})});
});
app.get('/api/admin/users/:id/detail',requireAuth,requirePermission('users.view'),(req,res)=>{const u=userById(Number(req.params.id));if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});const raw={id:u.id,username:u.username,display_name:u.display_name,phone:u.phone,age:u.age,province:u.province,coins:u.coins,diamonds:u.diamonds,xp:u.xp,fame:u.fame,level:u.level,role:u.role,is_banned:u.is_banned,is_game_banned:Number(u.is_game_banned||0),game_ban_until:u.game_ban_until||null,game_exit_streak:Number(u.game_exit_streak||0),device_id:u.device_id||'',device_account_count:u.device_id?db.prepare('SELECT COUNT(*) AS n FROM users WHERE device_id=?').get(u.device_id).n:0,ban_reason:u.ban_reason,ban_until:u.ban_until,created_at:u.created_at,last_seen:u.last_seen};const fields=['display_name','password','device_id','role','coins','diamonds','xp','level','age','province','phone','ban'];const viewFields=fields.filter(f=>canUserField(req.user,'view',f));const editFields=fields.filter(f=>canUserField(req.user,'edit',f));const user={...raw};for(const f of fields)if(!viewFields.includes(f)){if(f==='ban'){delete user.is_banned;delete user.is_game_banned;delete user.ban_reason;delete user.ban_until}else if(f==='password'){}else if(f==='device_id'){delete user.device_id;delete user.device_account_count}else {delete user[f];if(f==='xp')delete user.fame;}}res.json({user,viewFields,editFields,owner:isPrivilegedManager(req.user),permissions:db.prepare('SELECT permission,enabled FROM user_permissions WHERE user_id=? AND enabled=1').all(u.id).map(x=>x.permission)});});
app.post('/api/admin/users/:id/password',requireAuth,requirePermission('users.edit'),(req,res)=>{const id=Number(req.params.id),u=userById(id);if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});if(!canUserField(req.user,'edit','password'))return res.json({ok:true,unchanged:true});if(u.role==='owner'&&req.user.role!=='owner')return res.status(403).json({error:'فقط سازنده می‌تواند رمز حساب سازنده را تغییر دهد'});const password=String(req.body?.password||'');if(password.length<8)return res.status(400).json({error:'رمز جدید باید حداقل ۸ کاراکتر باشد'});if(!/[A-Za-z]/.test(password)||!/[0-9]/.test(password))return res.status(400).json({error:'رمز جدید باید شامل حداقل یک حرف انگلیسی و یک عدد باشد'});db.prepare('UPDATE users SET password_hash=? WHERE id=?').run(bcrypt.hashSync(password,12),id);logActivity(req.user.id,id,'admin_password_reset','تغییر رمز عبور کاربر توسط مدیریت');res.json({ok:true,message:'رمز عبور کاربر تغییر کرد'});});
app.patch('/api/admin/users/:id/permissions',requireAuth,requirePermission('roles.manage'),(req,res)=>{const id=Number(req.params.id);if(!userById(id))return res.status(404).json({error:'کاربر پیدا نشد'});const allowed=new Set(['users.view','users.edit','economy.manage','messages.view','messages.delete','tickets.view','tickets.reply','tickets.delete','reports.view','reports.export','security.manage','roles.manage','lobby.view','activity.view','shop.view','shop.edit','notifications.manage']);const list=Array.isArray(req.body.permissions)?[...new Set(req.body.permissions.filter(p=>allowed.has(p)))]:[];for(const p of allowed)db.prepare('DELETE FROM user_permissions WHERE user_id=? AND permission=?').run(id,p);for(const p of list)db.prepare('INSERT INTO user_permissions(user_id,permission,enabled) VALUES(?,?,1)').run(id,p);res.json({ok:true});});
app.put('/api/admin/announcement',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'فقط سازنده می‌تواند اعلان خانه را تغییر دهد'});const title=String(req.body?.title||'').trim().slice(0,100),body=String(req.body?.body||'').trim().slice(0,1200),active=req.body?.active===false?'0':'1';if(!title||!body)return res.status(400).json({error:'عنوان و متن اعلان الزامی است'});for(const [key,value] of [['home_announcement_title',title],['home_announcement_body',body],['home_announcement_active',active]])db.prepare('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run(key,value);res.json({ok:true,message:'اعلان صفحه خانه ذخیره شد'});});
app.post('/api/admin/broadcast',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'فقط سازنده اجازه ارسال اعلان همگانی دارد'});const title=String(req.body.title||'').trim().slice(0,100),body=String(req.body.body||'').trim().slice(0,1500);if(!title||!body)return res.status(400).json({error:'عنوان و متن اعلان الزامی است'});const users=db.prepare('SELECT id FROM users ORDER BY id').all();for(const u of users)notify(u.id,title,body,{action:'creator_broadcast',fromId:req.user.id});res.json({ok:true,sent:users.length});});
app.get('/api/admin/my-permissions',requireAuth,(req,res)=>{const owner=['owner','creator'].includes(req.user.role);const permissions=owner?['users.view','users.edit','economy.manage','messages.view','messages.delete','tickets.view','tickets.reply','tickets.delete','reports.view','reports.export','security.manage','roles.manage','lobby.view','activity.view','shop.view','shop.edit','notifications.manage']:db.prepare('SELECT permission FROM user_permissions WHERE user_id=? AND enabled=1').all(req.user.id).map(x=>x.permission);res.json({owner,permissions});});
app.get('/api/admin/management/search',requireAuth,requirePermission('roles.manage'),(req,res)=>{const q=String(req.query.q||'').trim().slice(0,100),by=String(req.query.by||'code');if(!q)return res.json({users:[]});let users;if(by==='id'){const id=Number(q);users=Number.isSafeInteger(id)&&id>0?db.prepare('SELECT id,username,display_name,role,(SELECT title FROM admin_titles WHERE user_id=users.id) AS admin_title FROM users WHERE id=? LIMIT 1').all(id):[]}else{users=db.prepare('SELECT id,username,display_name,role,(SELECT title FROM admin_titles WHERE user_id=users.id) AS admin_title FROM users WHERE username LIKE ? ORDER BY id DESC LIMIT 30').all('%'+q+'%')}res.json({users});});
app.get('/api/admin/management/:id(\\d+)',requireAuth,requirePermission('roles.manage'),(req,res)=>{const id=Number(req.params.id),u=userById(id);if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});res.json({user:{id:u.id,username:u.username,display_name:u.display_name,role:u.role},title:db.prepare('SELECT title FROM admin_titles WHERE user_id=?').get(id)?.title||'',permissions:db.prepare('SELECT permission FROM user_permissions WHERE user_id=? AND enabled=1').all(id).map(x=>x.permission)});});
app.post('/api/admin/management/:id',requireAuth,requirePermission('roles.manage'),(req,res)=>{const id=Number(req.params.id),u=userById(id);if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});if(['owner','creator'].includes(u.role)&&!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'تغییر دسترسی سازنده فقط برای مالک مجاز است'});const title=String(req.body.title||'پشتیبانی').trim().slice(0,60);if(!['کاربر عادی','پشتیبانی','ناظر','ادمین','سازنده'].includes(title))return res.status(400).json({error:'عنوان مدیریتی معتبر نیست'});if(title==='کاربر عادی'){if(['owner','creator'].includes(u.role))return res.status(403).json({error:'تبدیل مالک یا سازنده به کاربر عادی از این بخش مجاز نیست'});db.prepare('DELETE FROM user_permissions WHERE user_id=?').run(id);db.prepare('DELETE FROM admin_titles WHERE user_id=?').run(id);db.prepare("UPDATE users SET role='user' WHERE id=?").run(id);logActivity(req.user.id,id,'admin_permissions_updated','تبدیل به کاربر عادی و حذف تمام دسترسی‌های مدیریتی');return res.json({ok:true,role:'user',permissions:[],title:'کاربر عادی'});}const allowed=new Set(['users.view','users.edit','economy.manage','messages.view','messages.delete','tickets.view','tickets.reply','tickets.delete','reports.view','reports.export','security.manage','roles.manage','lobby.view','activity.view','shop.view','shop.edit','notifications.manage','users.field.view.coins','users.field.edit.coins','users.field.view.diamonds','users.field.edit.diamonds','users.field.view.xp','users.field.edit.xp','users.field.view.level','users.field.edit.level','users.field.view.age','users.field.edit.age','users.field.view.province','users.field.edit.province','users.field.view.phone','users.field.edit.phone','users.field.view.device_id','users.field.edit.device_id','users.field.view.role','users.field.edit.role','users.field.view.ban','users.field.edit.ban','users.field.view.password','users.field.edit.password','users.field.view.display_name','users.field.edit.display_name']);const list=Array.isArray(req.body.permissions)?[...new Set(req.body.permissions.filter(p=>allowed.has(p)))]:[];db.prepare('DELETE FROM user_permissions WHERE user_id=?').run(id);for(const p of list)db.prepare('INSERT INTO user_permissions(user_id,permission,enabled) VALUES(?,?,1)').run(id,p);db.prepare('INSERT INTO admin_titles(user_id,title,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(user_id) DO UPDATE SET title=excluded.title,updated_at=CURRENT_TIMESTAMP').run(id,title);if(!['owner','creator'].includes(u.role)){const baseRole=title==='ناظر'?'moderator':title==='پشتیبانی'?'support':'admin';db.prepare('UPDATE users SET role=? WHERE id=?').run(baseRole,id);}logActivity(req.user.id,id,'admin_permissions_updated','عنوان: '+title+'؛ تعداد مجوز: '+list.length);res.json({ok:true});});
app.post('/api/admin/economy/grant-all',requireAuth,requirePermission('economy.manage'),(req,res)=>{if(!['owner','creator'].includes(req.user.role)&&!db.prepare("SELECT 1 FROM user_permissions WHERE user_id=? AND permission='economy.manage' AND enabled=1").get(req.user.id))return res.status(403).json({error:'دسترسی لازم نیست'});const type=String(req.body.type||''),amount=Math.floor(Number(req.body.amount)),reason=String(req.body.reason||'').trim().slice(0,200);if(!['coins','diamonds','both'].includes(type)||!Number.isInteger(amount)||amount<1||amount>1000000||!reason)return res.status(400).json({error:'نوع، تعداد معتبر و علت شارژ را وارد کن'});const users=db.prepare('SELECT id FROM users').all();for(const u of users){if(type==='coins'||type==='both')addTx(u.id,amount,'admin_mass_grant','شارژ همگانی: '+reason);if(type==='diamonds'||type==='both'){db.prepare('UPDATE users SET diamonds=COALESCE(diamonds,0)+? WHERE id=?').run(amount,u.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(u.id,amount,'admin_mass_diamonds','شارژ همگانی الماس: '+reason);}notify(u.id,'شارژ همگانی',`${type==='coins'?'':type==='diamonds'?'':'سکه و '}${type==='coins'?'سکه':type==='diamonds'?'الماس':'الماس و سکه'} به تعداد ${amount} به حساب شما اضافه شد. علت: ${reason}`);}logActivity(req.user.id,null,'admin_mass_economy_grant',`${type} × ${amount}؛ ${reason}؛ تعداد کاربران ${users.length}`);res.json({ok:true,users:users.length});});
app.get('/api/admin/access-list',requireAuth,requirePermission('roles.manage'),(req,res)=>{
 const rows=db.prepare("SELECT u.id,u.username,u.display_name,u.role,u.is_banned,(SELECT title FROM admin_titles WHERE user_id=u.id) AS admin_title, GROUP_CONCAT(p.permission, ',') AS permissions FROM users u LEFT JOIN user_permissions p ON p.user_id=u.id AND p.enabled=1 WHERE u.role IN ('owner','creator','admin','moderator','support') OR p.permission IS NOT NULL GROUP BY u.id ORDER BY CASE u.role WHEN 'owner' THEN 0 WHEN 'creator' THEN 1 WHEN 'admin' THEN 2 WHEN 'moderator' THEN 3 WHEN 'support' THEN 4 ELSE 5 END,u.id DESC LIMIT 500").all();
 res.json({users:rows.map(u=>({...u,permissions:String(u.permissions||'').split(',').filter(Boolean)}))});
});
app.get('/api/admin/users/:id/messages',requireAuth,requirePermission('messages.view'),(req,res)=>{const uid=Number(req.params.id);if(!userById(uid))return res.status(404).json({error:'کاربر پیدا نشد'});const conversations=db.prepare('SELECT c.*,CASE WHEN c.user_a=? THEN c.user_b ELSE c.user_a END other_id FROM conversations c WHERE c.user_a=? OR c.user_b=? ORDER BY c.id DESC').all(uid,uid,uid).map(c=>({id:c.id,other_username:userById(c.other_id)?.username||'نامشخص'}));res.json({conversations});});
app.get('/api/admin/conversations/:id/messages',requireAuth,requirePermission('messages.view'),(req,res)=>{const id=Number(req.params.id),c=db.prepare('SELECT * FROM conversations WHERE id=?').get(id);if(!c)return res.status(404).json({error:'گفتگو پیدا نشد'});const messages=db.prepare('SELECT m.*,u.username FROM messages m JOIN users u ON u.id=m.sender_id WHERE m.conversation_id=? ORDER BY m.id').all(id);res.json({messages});});
app.post('/api/reports',requireAuth,(req,res)=>{const category=String(req.body.category||'').slice(0,40);const targetType=String(req.body.targetType||'').slice(0,30);const targetId=String(req.body.targetId||'').slice(0,80);const reason=String(req.body.reason||'').trim().slice(0,200);const details=String(req.body.details||'').trim().slice(0,1000);const allowed=new Set(['profile','message','chat','game','user','post','comment','other']);if(!allowed.has(category)||!targetType||!targetId||!reason)return res.status(400).json({error:'نوع گزارش، مورد و علت گزارش الزامی است'});let reported=Number(req.body.reportedUserId)||null;let captured=details;if(targetType==='message'){const msg=db.prepare('SELECT m.sender_id,m.body,m.type,m.conversation_id FROM messages m WHERE m.id=?').get(Number(targetId));if(!msg)return res.status(404).json({error:'پیام برای گزارش پیدا نشد'});if(!reported)reported=msg.sender_id;if(!captured)captured='محتوای پیام: '+String(msg.body||'['+msg.type+']').slice(0,500)+' · گفتگوی #'+msg.conversation_id;}else if(targetType==='profile'&&reported){const ru=userById(reported);if(!ru)return res.status(404).json({error:'پروفایل پیدا نشد'});if(!captured)captured='پروفایل کاربر @'+ru.username+' · نام نمایشی: '+ru.display_name;}else if(targetType==='game_room'){const gr=db.prepare('SELECT game_type,room_code,status FROM game_rooms WHERE id=?').get(Number(targetId));if(gr&&!captured)captured='بازی: '+gr.game_type+' · وضعیت: '+gr.status+' · '+(gr.room_code?'اتاق خصوصی':'بازی عمومی');}if(reported===req.user.id)return res.status(400).json({error:'نمی‌توانی خودت را گزارش کنی'});const dupe=db.prepare("SELECT id FROM reports WHERE reporter_id=? AND category=? AND target_type=? AND target_id=? AND status='new'").get(req.user.id,category,targetType,targetId);if(dupe)return res.status(409).json({error:'این مورد را قبلاً گزارش کرده‌ای'});const out=db.prepare('INSERT INTO reports(reporter_id,reported_user_id,category,target_type,target_id,reason,details) VALUES(?,?,?,?,?,?,?)').run(req.user.id,reported,category,targetType,targetId,reason,captured);res.json({ok:true,id:out.lastInsertRowid});});
app.get('/api/games/active',requireAuth,(req,res)=>{const room=db.prepare("SELECT r.id FROM game_rooms r JOIN game_players p ON p.room_id=r.id WHERE p.user_id=? AND r.status IN ('waiting','playing') ORDER BY r.id DESC LIMIT 1").get(req.user.id);if(!room)return res.status(404).json({error:'بازی فعالی پیدا نشد'});res.json({roomId:room.id});});
app.get('/api/games/history',requireAuth,(req,res)=>{const rows=db.prepare(`SELECT gr.id,gr.room_id,gr.game_type,gr.result,gr.fame_delta,gr.created_at,r.room_code FROM game_results gr JOIN game_rooms r ON r.id=gr.room_id WHERE gr.user_id=? ORDER BY gr.id DESC LIMIT 100`).all(req.user.id);res.json({results:rows});});
app.post('/api/games/room/:id/leave',requireAuth,requireGameAccess,(req,res)=>{
 const id=Number(req.params.id),room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);
 if(!room)return res.status(404).json({error:'اتاق پیدا نشد'});
 const member=db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(id,req.user.id);
 if(!member)return res.status(404).json({error:'عضو این بازی نیستی'});
 if(room.game_type==='classic'&&room.status==='playing'&&!room.room_code){
   const u=userById(req.user.id);let penalty=''; const exitStreak=Number(u?.game_exit_streak||0)+1; const gameLock=exitStreak>=2; if(gameLock)db.prepare("UPDATE users SET game_exit_streak=0,game_ban_until=datetime('now','+6 hours') WHERE id=?").run(req.user.id);else db.prepare('UPDATE users SET game_exit_streak=? WHERE id=?').run(exitStreak,req.user.id);
   try{atomic(()=>{
     if(Number(u?.diamonds||0)>=1){db.prepare('UPDATE users SET diamonds=diamonds-1 WHERE id=?').run(req.user.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(req.user.id,-1,'classic_game_exit','خروج از بازی کلاسیک');penalty='۱ الماس';}
     else if(Number(u?.coins||0)>=100){addTx(req.user.id,-100,'classic_game_exit','خروج از بازی کلاسیک؛ بدون الماس');penalty='۱۰۰ سکه';}
     else{const fame=Math.max(0,Number(u?.fame||0),Number(u?.xp||0));syncUserFameAndLevel(req.user.id,{fame:Math.max(0,fame-2000)});penalty='۲۰۰۰ شهرت';}
     db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,0)').run(id,room.game_type,req.user.id,'left');
   });}catch(e){return res.status(400).json({error:'ثبت هزینه خروج ناموفق بود'});}
   db.prepare('DELETE FROM game_players WHERE room_id=? AND user_id=?').run(id,req.user.id);
   const left=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at').all(id);
   if(!left.length)db.prepare("UPDATE game_rooms SET status='finished',finished_at=CURRENT_TIMESTAMP WHERE id=?").run(id);else finishGame(id,left[0].user_id);
   notify(req.user.id,'خروج از بازی',`از بازی کلاسیک خارج شدی؛ ${penalty} از حسابت کم شد.`);
   return res.json({ok:true,penalty:true,gameLock,message:gameLock?'به علت خروج از بازی امتیازی، حساب شما به مدت ۶ ساعت از بازی مسدود می‌باشد.':`خروج ثبت شد؛ ${penalty} کم شد.`});
 }
 let gameLock=false; if(!room.room_code&&room.status==='playing'){const streak=Number(userById(req.user.id)?.game_exit_streak||0)+1;if(streak>=2){db.prepare("UPDATE users SET game_exit_streak=0,game_ban_until=datetime('now','+6 hours') WHERE id=?").run(req.user.id);gameLock=true;}else db.prepare('UPDATE users SET game_exit_streak=? WHERE id=?').run(streak,req.user.id);syncUserFameAndLevel(req.user.id,{fame:Math.max(0,Number(userById(req.user.id)?.fame||0),Number(userById(req.user.id)?.xp||0))-10});db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,?)').run(id,room.game_type,req.user.id,'left',-10);notify(req.user.id,'خروج از بازی','به دلیل خروج از بازی عمومی، ۱۰ شهرت از تو کم شد.');}
 else if(room.room_code&&room.status==='playing'){db.prepare('UPDATE users SET game_exit_streak=0 WHERE id=?').run(req.user.id);db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,0)').run(id,room.game_type,req.user.id,'left');}
 db.prepare('DELETE FROM game_players WHERE room_id=? AND user_id=?').run(id,req.user.id);const left=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at').all(id);if(!left.length)db.prepare("UPDATE game_rooms SET status='finished',finished_at=CURRENT_TIMESTAMP WHERE id=?").run(id);else if(room.status==='playing'&&left.length===1)finishGame(id,left[0].user_id);res.json({ok:true,penalty:!room.room_code&&room.status==='playing'?10:0,gameLock,message:gameLock?'به علت خروج از بازی امتیازی، حساب شما به مدت ۶ ساعت از بازی مسدود می‌باشد.':undefined});
});
app.get('/api/admin/games/rooms',requireAuth,requirePermission('lobby.view'),(req,res)=>{
  const rooms=db.prepare(`
    SELECT r.*,
      u.username creator_username,
      u.display_name creator_name,
      (SELECT COUNT(*) FROM game_players p WHERE p.room_id=r.id) player_count,
      (SELECT GROUP_CONCAT(COALESCE(NULLIF(u2.display_name,''),u2.username),'، ')
       FROM game_players gp
       LEFT JOIN users u2 ON u2.id=gp.user_id
       WHERE gp.room_id=r.id) player_names
    FROM game_rooms r
    LEFT JOIN users u ON u.id=r.created_by
    ORDER BY CASE r.status WHEN 'playing' THEN 0 WHEN 'waiting' THEN 1 ELSE 2 END,r.id DESC
    LIMIT 200
  `).all();
  res.json({rooms});
});

app.post('/api/admin/games/rooms/:id/close',requireAuth,requirePermission('lobby.view'),(req,res)=>{
  const id=Number(req.params.id);
  const reason=String(req.body?.reason||'').trim().slice(0,500);
  if(!reason)return res.status(400).json({error:'برای بستن اتاق باید دلیل بنویسی'});
  const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);
  if(!room)return res.status(404).json({error:'اتاق بازی پیدا نشد'});
  if(room.status==='finished')return res.status(400).json({error:'این اتاق قبلاً پایان یافته است'});
  db.prepare("UPDATE game_rooms SET status='finished',finished_at=CURRENT_TIMESTAMP,admin_close_reason=? WHERE id=?").run(reason,id);
  db.prepare('INSERT INTO game_admin_actions(room_id,admin_id,action,reason) VALUES(?,?,?,?)').run(id,req.user.id,'admin_close',reason);
  const players=db.prepare('SELECT user_id FROM game_players WHERE room_id=?').all(id);
  for(const player of players){
    notify(player.user_id,'بازی توسط مدیریت بسته شد','دلیل: '+reason,{action:'game_admin_close',requestId:id});
  }
  res.json({ok:true,message:'اتاق بسته شد و دلیل برای بازیکنان ارسال شد'});
});
app.get('/api/admin/game-history',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const userId=String(req.query.userId||'').trim(),gameId=String(req.query.gameId||'').trim(),gameType=String(req.query.gameType||'').trim(),mode=String(req.query.mode||'').trim(),finished=String(req.query.finished||'').trim(),from=String(req.query.from||'').trim(),to=String(req.query.to||'').trim();
 const limit=Math.min(100,Math.max(1,Number(req.query.limit)||25));const page=Math.max(1,Number.parseInt(req.query.page,10)||1);const offset=(page-1)*limit;const where=[],args=[];
 if(userId){where.push('(EXISTS (SELECT 1 FROM game_players fp WHERE fp.room_id=r.id AND fp.user_id=?) OR EXISTS (SELECT 1 FROM game_results fr WHERE fr.room_id=r.id AND fr.user_id=?))');args.push(Number(userId)||-1,Number(userId)||-1)}
 if(gameId){const numeric=Number(gameId);if(Number.isFinite(numeric)&&numeric>0)where.push('r.id=?'),args.push(numeric);else where.push('r.game_code=?'),args.push(gameId)}
 if(gameType){if(gameType==='friendly')where.push('r.room_code IS NOT NULL');else if(gameType==='classic')where.push("r.game_type='classic' AND r.room_code IS NULL");else if(gameType==='advanced')where.push("r.game_type='advanced'")}
 if(mode==='classic')where.push("r.game_type='classic'");if(mode==='advanced')where.push("r.game_type='advanced'");
 if(finished==='yes')where.push("r.status='finished'");if(finished==='no')where.push("r.status!='finished'");
 if(from&&!/^\d{4}-\d{2}-\d{2}$/.test(from))return res.status(400).json({error:'تاریخ شروع معتبر نیست'});if(to&&!/^\d{4}-\d{2}-\d{2}$/.test(to))return res.status(400).json({error:'تاریخ پایان معتبر نیست'});if(from&&to&&from>to)return res.status(400).json({error:'تاریخ شروع نباید بعد از تاریخ پایان باشد'});if(from){where.push('date(COALESCE(r.finished_at,r.created_at))>=date(?)');args.push(from)}if(to){where.push('date(COALESCE(r.finished_at,r.created_at))<=date(?)');args.push(to)}
 const filter=where.length?'WHERE '+where.join(' AND '):'';
 const total=db.prepare(`SELECT COUNT(*) n FROM game_rooms r ${filter}`).get(...args).n;
 const rows=db.prepare(`SELECT r.id,r.game_code,r.game_type,r.room_code,r.status,r.created_at,r.started_at,r.finished_at,r.winner_id,r.admin_close_reason, (SELECT COUNT(*) FROM game_players p WHERE p.room_id=r.id) player_count FROM game_rooms r ${filter} ORDER BY r.id DESC LIMIT ${limit} OFFSET ${offset}`).all(...args);
 res.json({total,limit,page,pages:Math.max(1,Math.ceil(total/limit)),rows});
});
app.get('/api/admin/game-history/:id',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const id=Number(req.params.id);processGameTurnTimeout(id);let beforeLive=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(beforeLive?.game_type==='advanced'&&beforeLive.status==='playing')runAdvancedBots(id);const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(!room)return res.status(404).json({error:'بازی پیدا نشد'});
 const players=db.prepare(`SELECT p.user_id,MIN(p.joined_at) joined_at,u.username,u.display_name,u.avatar,COALESCE(gr.result,'pending') result,COALESCE(gr.fame_delta,0) fame_delta,CASE WHEN gr.result='win' AND r.room_code IS NULL AND r.game_type='classic' THEN 5 ELSE 0 END diamonds_delta
 FROM (SELECT user_id,joined_at,room_id FROM game_players WHERE room_id=? UNION SELECT user_id,created_at AS joined_at,room_id FROM game_results WHERE room_id=?) p
 JOIN users u ON u.id=p.user_id JOIN game_rooms r ON r.id=p.room_id LEFT JOIN game_results gr ON gr.room_id=p.room_id AND gr.user_id=p.user_id
 GROUP BY p.user_id ORDER BY joined_at,p.user_id`).all(id,id);
 const messages=db.prepare('SELECT gm.id,gm.user_id,u.display_name,u.username,gm.body,gm.created_at FROM game_messages gm JOIN users u ON u.id=gm.user_id WHERE gm.room_id=? ORDER BY gm.id ASC LIMIT 200').all(id);
 res.json({room:{id:room.id,gameCode:room.game_code||('GAME-'+String(id).padStart(8,'0')),gameType:room.game_type,roomCode:room.room_code,status:room.status,createdAt:room.created_at,startedAt:room.started_at,finishedAt:room.finished_at,winnerId:room.winner_id,adminCloseReason:room.admin_close_reason||'',state:JSON.parse(room.state_json||'{}')},players,messages});
});
app.post('/api/admin/game-history/:id/winner',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const id=Number(req.params.id),winnerId=Number(req.body.winnerId),room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(!room)return res.status(404).json({error:'بازی پیدا نشد'});if(room.status!=='playing')return res.status(409).json({error:'فقط بازی در حال اجرا را می‌توان برنده اعلام کرد'});if(!winnerId||!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(id,winnerId))return res.status(400).json({error:'برنده انتخاب‌شده بازیکن این بازی نیست'});
 finishGame(id,winnerId);db.prepare('INSERT INTO game_admin_actions(room_id,admin_id,action,reason) VALUES(?,?,?,?)').run(id,req.user.id,'force_winner',String(req.body.reason||'تعیین برنده توسط مدیریت').slice(0,500));res.json({ok:true});
});
app.post('/api/admin/game-history/:id/close',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const id=Number(req.params.id),reason=String(req.body.reason||'').trim().slice(0,500),room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(!room)return res.status(404).json({error:'بازی پیدا نشد'});if(!reason)return res.status(400).json({error:'علت بستن بازی را بنویس'});if(room.status==='finished')return res.status(409).json({error:'این بازی قبلاً بسته شده است'});
 db.prepare("UPDATE game_rooms SET status='finished',finished_at=CURRENT_TIMESTAMP,admin_close_reason=? WHERE id=?").run(reason,id);db.prepare('INSERT INTO game_admin_actions(room_id,admin_id,action,reason) VALUES(?,?,?,?)').run(id,req.user.id,'admin_close',reason);const ps=db.prepare('SELECT user_id FROM game_players WHERE room_id=?').all(id);for(const p of ps)notify(p.user_id,'بازی توسط مدیریت بسته شد',reason,{action:'game_admin_close',requestId:id});res.json({ok:true});
});
app.get('/api/admin/game-history/:id/live',requireAuth,requirePermission('lobby.view'),(req,res)=>{const id=Number(req.params.id);processGameTurnTimeout(id);const beforeLive=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(beforeLive?.game_type==='advanced'&&beforeLive.status==='playing')runAdvancedBots(id);const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(!room)return res.status(404).json({error:'بازی پیدا نشد'});res.json({room:gameRoomState(id,null)});});

app.get('/api/admin/posts',requireAuth,requirePermission('reports.view'),(req,res)=>{const q=String(req.query.q||'').trim().slice(0,100),code=String(req.query.code||'').trim().slice(0,100),name=String(req.query.name||'').trim().slice(0,100),url=String(req.query.url||'').trim().slice(0,150),from=String(req.query.from||'').trim(),to=String(req.query.to||'').trim();const where=["p.status<>'deleted'"],args=[];if(q){if(/^\d+$/.test(q)){where.push('(p.user_id=? OR u.id=? OR p.id=?)');args.push(Number(q),Number(q),Number(q));}else{where.push('(u.username LIKE ? OR u.display_name LIKE ? OR p.post_code LIKE ?)');args.push('%'+q+'%','%'+q+'%','%'+q+'%');}}if(code){where.push('(u.username LIKE ? OR CAST(u.id AS TEXT)=?)');args.push('%'+code+'%',code);}if(name){where.push('u.display_name LIKE ?');args.push('%'+name+'%');}if(url){const m=url.match(/(?:\/p\/|post=)([A-Za-z0-9_-]+)/);where.push('(p.post_code=? OR CAST(p.id AS TEXT)=?)');args.push(m?m[1]:url,m?m[1]:url);}if(from){where.push('date(p.created_at)>=date(?)');args.push(from);}if(to){where.push('date(p.created_at)<=date(?)');args.push(to);}const rows=db.prepare(postSelect+(where.length?' WHERE '+where.join(' AND '):'')+' ORDER BY p.id DESC LIMIT 500').all(...args).map(p=>{let media_size_bytes=0;if(p.media_url){try{const mediaPath=path.join(uploadsDir,path.basename(String(p.media_url)));const st=fs.statSync(mediaPath);if(st.isFile())media_size_bytes=st.size;}catch{}}return {...p,media_size_bytes};});res.json({posts:rows});});
app.delete('/api/admin/posts/:id',requireAuth,requirePermission('reports.view'),(req,res)=>{const id=Number(req.params.id),p=db.prepare('SELECT * FROM posts WHERE id=?').get(id);if(!p)return res.status(404).json({error:'پست پیدا نشد'});atomic(()=>{const tables=db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(x=>x.name);for(const table of tables){if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)||table==='posts')continue;let cols=[];try{cols=db.prepare(`PRAGMA table_info("${table}")`).all().map(c=>c.name)}catch{}if(cols.includes('post_id'))db.prepare(`DELETE FROM "${table}" WHERE post_id=?`).run(id);}db.prepare("UPDATE reports SET status='resolved',reviewed_by=?,reviewed_at=CURRENT_TIMESTAMP WHERE target_type='post' AND target_id=? AND status IN ('new','reviewing')").run(req.user.id,String(id));db.prepare('DELETE FROM posts WHERE id=?').run(id);});if(p.media_url){try{fs.unlinkSync(path.join(uploadsDir,path.basename(p.media_url)))}catch{}}logActivity(req.user.id,p.user_id,'admin_post_deleted',`حذف کامل پست #${p.post_code}`);notify(p.user_id,'پست شما حذف شد','مدیریت پس از بررسی گزارش، پست را حذف کرد. هزینه حمایت بازپرداخت نمی‌شود.',{action:'post_admin_deleted',requestId:id});res.json({ok:true});});
app.get('/api/admin/reports',requireAuth,requirePermission('reports.view'),(req,res)=>{const category=String(req.query.category||'all');const status=String(req.query.status||'all');const where=[];const args=[];if(category!=='all'){where.push('r.category=?');args.push(category)}if(status!=='all'){where.push('r.status=?');args.push(status)}const rows=db.prepare(`SELECT r.*,a.username reporter_username,a.display_name reporter_name,b.username reported_username,b.display_name reported_name,rv.username reviewer_username FROM reports r LEFT JOIN users a ON a.id=r.reporter_id LEFT JOIN users b ON b.id=r.reported_user_id LEFT JOIN users rv ON rv.id=r.reviewed_by ${where.length?'WHERE '+where.join(' AND '):''} ORDER BY r.id DESC LIMIT 500`).all(...args);res.json({reports:rows,counts:{all:db.prepare('SELECT COUNT(*) n FROM reports').get().n,new:db.prepare("SELECT COUNT(*) n FROM reports WHERE status='new'").get().n,profile:db.prepare("SELECT COUNT(*) n FROM reports WHERE category='profile'").get().n,message:db.prepare("SELECT COUNT(*) n FROM reports WHERE category='message'").get().n,chat:db.prepare("SELECT COUNT(*) n FROM reports WHERE category='chat'").get().n,game:db.prepare("SELECT COUNT(*) n FROM reports WHERE category='game'").get().n,post:db.prepare("SELECT COUNT(*) n FROM reports WHERE category='post'").get().n,comment:db.prepare("SELECT COUNT(*) n FROM reports WHERE category='comment'").get().n}});});
app.patch('/api/admin/reports/:id',requireAuth,requirePermission('reports.view'),(req,res)=>{const id=Number(req.params.id),status=String(req.body.status||'');if(!['new','reviewing','resolved','dismissed'].includes(status))return res.status(400).json({error:'وضعیت گزارش نامعتبر است'});const row=db.prepare('SELECT * FROM reports WHERE id=?').get(id);if(!row)return res.status(404).json({error:'گزارش پیدا نشد'});db.prepare('UPDATE reports SET status=?,reviewed_by=?,reviewed_at=CURRENT_TIMESTAMP WHERE id=?').run(status,req.user.id,id);if(row.target_type==='post'&&status==='dismissed'){const related=db.prepare("SELECT COUNT(*) n FROM reports WHERE target_type='post' AND target_id=? AND status IN ('new','reviewing')").get(row.target_id).n;if(!related){db.prepare("UPDATE posts SET status='active' WHERE id=? AND status='hidden'").run(Number(row.target_id));const post=db.prepare('SELECT user_id,post_code FROM posts WHERE id=?').get(Number(row.target_id));if(post)notify(post.user_id,'پست شما پس از بررسی بازگردانده شد','گزارش بررسی شد و پست شما دوباره در دسترس قرار گرفت.',{action:'post_restored',requestId:Number(row.target_id)});}}res.json({ok:true});});
app.get('/api/maintenance/status',(req,res)=>res.json({maintenance:getSetting('maintenance')==='1'}));
app.post('/api/admin/maintenance',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'فقط مالک برنامه می‌تواند حالت بروزرسانی را تغییر دهد'});const enabled=req.body?.enabled===true;setSetting('maintenance',enabled?'1':'0');res.json({ok:true,maintenance:enabled});});
app.get('/api/ranking/weekly',requireAuth,(req,res)=>{const now=iranClock();const dow=(now.getUTCDay()+6)%7;const startDate=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate()-dow));const start=iranMidnightUtc(startDate).toISOString().slice(0,19).replace('T',' ');const all=db.prepare("SELECT u.id,u.username,u.display_name,u.avatar,COALESCE(SUM(g.amount),0) score FROM users u LEFT JOIN fame_events g ON g.user_id=u.id AND g.created_at>=? WHERE u.is_banned=0 GROUP BY u.id ORDER BY score DESC,u.id ASC").all(start);const rank=Math.max(1,all.findIndex(x=>x.id===req.user.id)+1);res.json({rows:all.slice(0,50),rank,me:all.find(x=>x.id===req.user.id)||null,serverTime:new Date().toISOString(),nextReset:iranMidnightUtc(new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate()+((7-dow)%7||7)))).toISOString()});});
app.get('/api/ranking/daily',requireAuth,(req,res)=>{const now=iranClock(),start=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' ');const all=db.prepare("SELECT u.id,u.username,u.display_name,u.avatar,COALESCE(SUM(g.amount),0) score FROM users u LEFT JOIN fame_events g ON g.user_id=u.id AND g.created_at>=? WHERE u.is_banned=0 GROUP BY u.id ORDER BY score DESC,u.id ASC").all(start);const rank=Math.max(1,all.findIndex(x=>x.id===req.user.id)+1);res.json({rank,me:all.find(x=>x.id===req.user.id)||null,rows:all.slice(0,30),serverTime:new Date().toISOString(),nextReset:new Date(iranMidnightUtc(new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate()+1)))).toISOString()});});
app.get('/api/ranking',requireAuth,(req,res)=>{const now=iranClock(),start=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' ');const dailyAll=db.prepare("SELECT u.id,u.username,u.display_name,u.avatar,COALESCE(SUM(g.amount),0) score FROM users u LEFT JOIN fame_events g ON g.user_id=u.id AND g.created_at>=? WHERE u.is_banned=0 GROUP BY u.id ORDER BY score DESC,u.id ASC").all(start);const allFull=db.prepare('SELECT id,username,display_name,avatar,xp score FROM users WHERE is_banned=0 ORDER BY xp DESC,id ASC').all();const rankOf=rows=>Math.max(1,rows.findIndex(x=>x.id===req.user.id)+1);const tomorrow=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate()+1));res.json({daily:dailyAll.slice(0,30),all:allFull.slice(0,100),dailyRank:rankOf(dailyAll),allRank:rankOf(allFull),dailyMe:dailyAll.find(x=>x.id===req.user.id)||null,allMe:allFull.find(x=>x.id===req.user.id)||null,serverTime:new Date().toISOString(),nextReset:iranMidnightUtc(tomorrow).toISOString()});});
app.get('/api/admin/ranking',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی مدیریت لازم است'});const period=String(req.query.period||'daily');let start=null,limit=30;if(period==='daily'){const now=iranClock();start=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' ');limit=30}else if(period==='weekly'){const now=iranClock(),dow=(now.getUTCDay()+6)%7;start=iranMidnightUtc(new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth(),now.getUTCDate()-dow))).toISOString().slice(0,19).replace('T',' ');limit=50}else if(period==='all')limit=100;else return res.status(400).json({error:'بازه رتبه‌بندی نامعتبر است'});const rows=period==='all'?db.prepare('SELECT id,username,display_name,avatar,fame score FROM users WHERE is_banned=0 ORDER BY fame DESC,id ASC LIMIT ?').all(limit):db.prepare('SELECT u.id,u.username,u.display_name,u.avatar,COALESCE(SUM(f.amount),0) score FROM users u LEFT JOIN fame_events f ON f.user_id=u.id AND f.created_at>=? WHERE u.is_banned=0 GROUP BY u.id ORDER BY score DESC,u.id ASC LIMIT ?').all(start,limit);res.json({period,rows});});
app.post('/api/admin/ranking/daily-adjust',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی مدیریت لازم است'});const uid=Number(req.body.userId),amount=Number(req.body.amount),reason=String(req.body.reason||'').trim().slice(0,300),u=userById(uid);if(!u||!Number.isInteger(amount)||amount===0||Math.abs(amount)>100000000||!reason)return res.status(400).json({error:'کاربر، مقدار و علت معتبر وارد کن'});const now=iranClock(),start=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' ');if(amount<0){const todayScore=Number(db.prepare('SELECT COALESCE(SUM(amount),0) n FROM fame_events WHERE user_id=? AND created_at>=?').get(uid,start).n||0);if(Math.abs(amount)>Math.max(0,todayScore))return res.status(400).json({error:'مقدار کسر شهرت از شهرت روزانه موجود بیشتر است'});}db.prepare('INSERT INTO fame_events(user_id,amount) VALUES(?,?)').run(uid,amount);syncUserFameAndLevel(uid,{fame:Math.max(0,Number(u.fame||0)+amount)});logActivity(req.user.id,uid,'ranking_daily_fame_adjust',`${amount>0?'افزایش':'کسر'} ${Math.abs(amount)} شهرت روزانه؛ علت: ${reason}`);res.json({ok:true,user:safeUser(userById(uid))});});
app.get('/api/admin/restrictions/users',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی مدیر سازنده لازم است'});const q=String(req.query.q||'').trim(),id=Number(req.query.id)||0,device=String(req.query.device||'').trim(),levelFrom=Math.max(0,Number(req.query.levelFrom)||0),levelTo=Math.max(0,Number(req.query.levelTo)||9999),blocked=String(req.query.blocked||'all'),limit=Math.max(1,Math.min(100,Number(req.query.limit)||25));let where=['u.level BETWEEN ? AND ?'],args=[levelFrom,levelTo];if(id){where.push('u.id=?');args.push(id)}if(q){where.push('(u.username LIKE ? OR u.display_name LIKE ? OR CAST(u.id AS TEXT)=?)');args.push('%'+q+'%','%'+q+'%',q)}if(device){where.push('u.device_id=?');args.push(device)}if(blocked==='blocked')where.push('u.is_banned=1');if(blocked==='unblocked')where.push('u.is_banned=0');const users=db.prepare(`SELECT u.id,u.username,u.display_name,u.avatar,u.level,u.is_banned,u.device_id FROM users u WHERE ${where.join(' AND ')} ORDER BY u.id DESC LIMIT ?`).all(...args,limit);const rows=users.map(u=>({...u,restrictions:db.prepare("SELECT id,restriction_type,starts_at,ends_at,reason,active FROM user_restrictions WHERE user_id=? AND active=1 AND ends_at>CURRENT_TIMESTAMP ORDER BY ends_at").all(u.id)}));res.json({users:rows});});
app.post('/api/admin/restrictions',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی مدیر سازنده لازم است'});const uid=Number(req.body.userId),type=String(req.body.type||''),duration=Number(req.body.duration),unit=String(req.body.unit||'hours'),reason=String(req.body.reason||'').trim().slice(0,300);const types=['profile','messages','games','app','posts','comments','diamond_transfer','conversation'];if(!userById(uid)||!types.includes(type)||!Number.isFinite(duration)||duration<1||duration>3650||!reason)return res.status(400).json({error:'کاربر، نوع محدودیت، مدت و علت را درست وارد کن'});const factor={hours:3600000,days:86400000,months:30*86400000,years:365*86400000}[unit];if(!factor)return res.status(400).json({error:'واحد مدت نامعتبر است'});const start=new Date(),end=new Date(start.getTime()+duration*factor),sqlTime=d=>d.toISOString().slice(0,19).replace('T',' ');const r=db.prepare('INSERT INTO user_restrictions(user_id,restriction_type,starts_at,ends_at,reason,created_by) VALUES(?,?,?,?,?,?)').run(uid,type,sqlTime(start),sqlTime(end),reason,req.user.id);logActivity(req.user.id,uid,'user_restriction',`محدودیت ${type} تا ${end.toISOString()}؛ علت: ${reason}`);res.json({ok:true,id:r.lastInsertRowid,endsAt:end.toISOString()});});
app.delete('/api/admin/restrictions/:id',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی مدیر سازنده لازم است'});const r=db.prepare('UPDATE user_restrictions SET active=0 WHERE id=?').run(Number(req.params.id));res.json({ok:true,changed:r.changes||0});});
app.get('/api/admin/overview',requireAuth,(req,res)=>{const own=['owner','creator'].includes(req.user.role);const perms=db.prepare('SELECT permission FROM user_permissions WHERE user_id=? AND enabled=1').all(req.user.id).map(x=>x.permission);if(!own&&!perms.some(p=>['users.view','users.edit','economy.manage','messages.view','tickets.view','tickets.reply','reports.view','roles.manage','activity.view','shop.view','shop.edit','notifications.manage'].includes(p)))return res.status(403).json({error:'دسترسی پنل مدیریت برای شما فعال نیست'});res.json({counts:{users:db.prepare('SELECT COUNT(*) n FROM users').get().n,messages:db.prepare('SELECT COUNT(*) n FROM messages').get().n,tickets:db.prepare("SELECT COUNT(*) n FROM tickets WHERE status='open'").get().n,online:db.prepare("SELECT COUNT(*) n FROM users WHERE last_seen>=datetime('now','-2 minutes') AND is_banned=0").get().n,activeRooms:db.prepare("SELECT COUNT(*) n FROM game_rooms WHERE status IN ('waiting','playing')").get().n,newReports:db.prepare("SELECT COUNT(*) n FROM reports WHERE status='new'").get().n},settings:db.prepare("SELECT key,value FROM settings WHERE key IN ('maintenance','app_name')").all()});});
app.patch('/api/admin/settings',requireAuth,requirePermission('economy.manage'),(req,res)=>{for(const [k,v] of Object.entries(req.body||{})){if(/^[a-z][a-z0-9_]{1,39}$/.test(k))setSetting(k,String(v).slice(0,500));}res.json({ok:true});});
app.delete('/api/admin/users/:id',requireAuth,requirePermission('users.edit'),(req,res)=>{const id=Number(req.params.id),target=userById(id);if(!target)return res.status(404).json({error:'کاربر پیدا نشد'});if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'فقط مالک یا سازنده سایت می‌تواند حساب را حذف کند'});if(id===req.user.id)return res.status(400).json({error:'حسابی که با آن وارد شده‌ای قابل حذف نیست'});if(target.role==='owner')return res.status(403).json({error:'حساب مالک اصلی قابل حذف نیست'});try{atomic(()=>{const tables=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all().map(x=>x.name);const refs=['user_id','from_id','to_id','follower_id','followed_id','blocked_id','reporter_id','reported_user_id','sender_id','receiver_id','owner_id','created_by','reviewed_by','actor_id','target_user_id','viewer_id','viewed_id','friend_id','inviter_id'];for(const table of tables){if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)||table==='users')continue;let cols=[];try{cols=db.prepare(`PRAGMA table_info("${table}")`).all().map(c=>c.name)}catch{}for(const col of refs){if(cols.includes(col)){try{db.prepare(`DELETE FROM "${table}" WHERE "${col}"=?`).run(id)}catch{}}}}db.prepare('DELETE FROM users WHERE id=?').run(id);});logActivity(req.user.id,id,'admin_user_delete',`حذف حساب کاربری #${id} (@${target.username})`);res.json({ok:true,message:'حساب کاربری حذف شد'});}catch(e){res.status(500).json({error:'حذف حساب انجام نشد؛ هیچ تغییری را دستی تکرار نکن و گزارش سرور را بررسی کن'});}});
app.patch('/api/admin/users/:id',requireAuth,requirePermission('users.edit'),(req,res)=>{const id=Number(req.params.id),u=userById(id);if(!u)return res.status(404).json({error:'کاربر پیدا نشد'});if(!isPrivilegedManager(req.user)){const map={display_name:'display_name',role:'role',coins:'coins',diamonds:'diamonds',xp:'xp',fame:'xp',level:'level',age:'age',province:'province',phone:'phone',device_id:'device_id',is_banned:'ban',is_game_banned:'ban',ban_reason:'ban',ban_days:'ban'};for(const [k,f] of Object.entries(map))if(req.body&&req.body[k]!==undefined&&!canUserField(req.user,'edit',f))delete req.body[k];if(!Object.keys(req.body||{}).length)return res.json({ok:true,unchanged:true});}if(u.role==='owner'&&req.user.role!=='owner')return res.status(403).json({error:'فقط سازنده می‌تواند حساب سازنده را ویرایش کند'});const {role,is_banned,is_game_banned,display_name,coins,diamonds,xp,fame,level,age,province,phone,device_id,username}=req.body||{};let nextUsername;if(username!==undefined){if(!isPrivilegedManager(req.user))return res.status(403).json({error:'تغییر کد کاربری فقط برای مالک یا سازنده مجاز است'});nextUsername=String(username||'').trim();if(!/^[A-Za-z0-9_]{3,24}$/.test(nextUsername))return res.status(400).json({error:'کد کاربری باید ۳ تا ۲۴ حرف انگلیسی، عدد یا زیرخط باشد'});const duplicate=db.prepare('SELECT id FROM users WHERE username=? COLLATE NOCASE AND id<>?').get(nextUsername,id);if(duplicate)return res.status(409).json({error:'این کد کاربری قبلاً برای حساب دیگری ثبت شده است'});}if(req.body?.clear_game_ban===true){if(!canUserField(req.user,'edit','ban'))return res.status(403).json({error:'اجازه رفع محرومیت بازی را ندارید'});db.prepare('UPDATE users SET game_ban_until=NULL,game_exit_streak=0,is_game_banned=0 WHERE id=?').run(id);
db.prepare("UPDATE user_restrictions SET active=0 WHERE user_id=? AND restriction_type IN ('games','app') AND active=1").run(id);logActivity(req.user.id,id,'admin_game_unban','رفع محرومیت بازی و پاک‌کردن شمارنده خروج توسط مدیریت');notify(id,'رفع محرومیت بازی','محرومیت بازی حساب شما توسط مدیریت برداشته شد.');}if(role&&['user','moderator','admin','creator','support'].includes(role)&&(req.user.role==='owner'||role!=='creator')){db.prepare('UPDATE users SET role=? WHERE id=?').run(role,id);if(role==='user'){db.prepare('DELETE FROM user_permissions WHERE user_id=?').run(id);db.prepare('DELETE FROM admin_titles WHERE user_id=?').run(id);}}if(is_banned!==undefined){if(is_banned){const reason=String(req.body.ban_reason||'تخلف از قوانین').trim().slice(0,300)||'تخلف از قوانین';const days=Math.max(1,Math.min(3650,Number(req.body.ban_days)||1));const until=new Date(Date.now()+days*86400000).toISOString();db.prepare('UPDATE users SET is_banned=1,ban_reason=?,ban_until=? WHERE id=?').run(reason,until,id);notify(id,'مسدودیت حساب',`حساب شما به مدت ${days} روز مسدود شد. علت: ${reason}`);}else{db.prepare('UPDATE users SET is_banned=0,ban_reason=NULL,ban_until=NULL WHERE id=?').run(id);notify(id,'رفع مسدودیت','مسدودیت حساب شما توسط مدیریت برداشته شد.');}}if(is_game_banned!==undefined)db.prepare('UPDATE users SET is_game_banned=? WHERE id=?').run(is_game_banned?1:0,id);if(nextUsername!==undefined&&nextUsername!==u.username){db.prepare('UPDATE users SET username=? WHERE id=?').run(nextUsername,id);logActivity(req.user.id,id,'admin_username_change',`تغییر کد کاربری از @${u.username} به @${nextUsername}`);}if(display_name!==undefined)db.prepare('UPDATE users SET display_name=? WHERE id=?').run(String(display_name).slice(0,40),id);for(const [k,v] of Object.entries({coins,diamonds,age})){if(v!==undefined&&Number.isFinite(Number(v)))db.prepare(`UPDATE users SET ${k}=? WHERE id=?`).run(Number(v),id);}
const fameInput=fame!==undefined?fame:xp;
if(fameInput!==undefined&&Number.isFinite(Number(fameInput))){
 syncUserFameAndLevel(id,{fame:Number(fameInput)});
}
if(level!==undefined&&Number.isFinite(Number(level))){
 syncUserFameAndLevel(id,{level:Number(level)});
}if(province!==undefined)db.prepare('UPDATE users SET province=? WHERE id=?').run(String(province).slice(0,80),id);if(phone!==undefined)db.prepare('UPDATE users SET phone=? WHERE id=?').run(String(phone).slice(0,30)||null,id);if(device_id!==undefined){const normalized=String(device_id||'').trim().slice(0,100);if(normalized&&!/^[A-Za-z0-9._:-]+$/.test(normalized))return res.status(400).json({error:'شناسه دستگاه فقط می‌تواند شامل حروف انگلیسی، عدد و . _ : - باشد'});if(normalized&&db.prepare('SELECT id FROM users WHERE device_id=? AND id<>?').get(normalized,id))return res.status(409).json({error:'این شناسه دستگاه برای حساب دیگری ثبت شده است'});if(normalized||canUserField(req.user,'view','device_id'))db.prepare('UPDATE users SET device_id=? WHERE id=?').run(normalized||null,id);}notify(id,'به‌روزرسانی حساب','اطلاعات حساب شما توسط مدیریت تغییر کرد');res.json({ok:true});});
app.post('/api/admin/users/:id/coins',requireAuth,requirePermission('economy.manage'),(req,res)=>{const id=Number(req.params.id),amount=Number(req.body.amount);if(!userById(id)||!Number.isFinite(amount)||Math.abs(amount)>1e7)return res.status(400).json({error:'مقدار نامعتبر است'});addTx(id,amount,'admin_adjust',String(req.body.note||'تغییر توسط مدیریت').slice(0,200));notify(id,'تغییر موجودی',`${amount>0?'+':''}${amount} سکه توسط مدیریت`);res.json({ok:true});});
app.delete('/api/admin/messages/:id',requireAuth,requirePermission('messages.delete'),(req,res)=>{db.prepare('UPDATE messages SET deleted=1,body="",media_url="" WHERE id=?').run(Number(req.params.id));res.json({ok:true});});
app.delete('/api/admin/tickets/:id/messages/:messageId',requireAuth,requirePermission('tickets.delete'),(req,res)=>{db.prepare('DELETE FROM ticket_messages WHERE id=? AND ticket_id=?').run(Number(req.params.messageId),Number(req.params.id));res.json({ok:true});});
app.get('/api/admin/export',requireAuth,(req,res,next)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'خروجی کامل اطلاعات فقط برای سازنده فعال است'});next();},(req,res)=>{res.setHeader('Content-Disposition','attachment; filename=dordor-backup.json');res.json({version:2,users:db.prepare('SELECT * FROM users').all(),settings:db.prepare('SELECT * FROM settings').all(),user_permissions:db.prepare('SELECT * FROM user_permissions').all(),chat_groups:db.prepare('SELECT * FROM chat_groups').all(),chat_group_members:db.prepare('SELECT * FROM chat_group_members').all(),chat_group_messages:db.prepare('SELECT * FROM chat_group_messages').all(),chat_group_reports:db.prepare('SELECT * FROM chat_group_reports').all(),chat_group_invites:db.prepare('SELECT * FROM chat_group_invites').all(),group_reward_claims:db.prepare('SELECT * FROM group_reward_claims').all(),conversations:db.prepare('SELECT * FROM conversations').all(),messages:db.prepare('SELECT * FROM messages').all(),notifications:db.prepare('SELECT * FROM notifications').all(),transactions:db.prepare('SELECT * FROM transactions').all(),tickets:db.prepare('SELECT * FROM tickets').all(),ticket_messages:db.prepare('SELECT * FROM ticket_messages').all(),follows:db.prepare('SELECT * FROM follows').all(),likes:db.prepare('SELECT * FROM likes').all(),friend_requests:db.prepare('SELECT * FROM friend_requests').all(),message_requests:db.prepare('SELECT * FROM message_requests').all(),blocks:db.prepare('SELECT * FROM blocks').all(),profile_views:db.prepare('SELECT * FROM profile_views').all(),reports:db.prepare('SELECT * FROM reports').all(),random_reports:db.prepare('SELECT * FROM random_reports').all(),game_rooms:db.prepare('SELECT * FROM game_rooms').all(),game_players:db.prepare('SELECT * FROM game_players').all(),game_results:db.prepare('SELECT * FROM game_results').all(),game_messages:db.prepare('SELECT * FROM game_messages').all(),group_promotion_views:db.prepare('SELECT * FROM group_promotion_views').all(),game_reward_claims:db.prepare('SELECT * FROM game_reward_claims').all(),level_rewards:db.prepare('SELECT * FROM level_rewards').all(),wheel_claims:db.prepare('SELECT * FROM wheel_claims').all(),pro_usage:db.prepare('SELECT * FROM pro_usage').all(),daily_presence_rewards:db.prepare('SELECT * FROM daily_presence_rewards').all(),challenge_claims:db.prepare('SELECT * FROM challenge_claims').all(),user_login_days:db.prepare('SELECT * FROM user_login_days').all(),referrals:db.prepare('SELECT * FROM referrals').all(),custom_challenges:db.prepare('SELECT * FROM custom_challenges').all(),daily_ranking_distributions:db.prepare('SELECT * FROM daily_ranking_distributions').all()});});
app.get('/api/admin/activity',requireAuth,requirePermission('activity.view'),(req,res)=>{
 const limit=Math.max(1,Math.min(500,Number(req.query.limit)||25));
 const type=['all','admin','users'].includes(String(req.query.type||'all'))?String(req.query.type||'all'):'all';
 const actorId=Number(req.query.actorUserId||0),targetId=Number(req.query.targetUserId||0),actorUsername=String(req.query.actorUsername||'').trim(),targetUsername=String(req.query.targetUsername||'').trim();
 const from=String(req.query.from||'').trim(),to=String(req.query.to||'').trim(),action=String(req.query.action||'').trim(),details=String(req.query.details||'').trim(),deviceId=String(req.query.deviceId||'').trim();
 const where=[],params=[];
 if(type==='admin')where.push("COALESCE(NULLIF(a.actor_role,''),actor.role) IN ('owner','creator','admin','moderator','support')");
 if(type==='users')where.push("(COALESCE(NULLIF(a.actor_role,''),actor.role) IS NULL OR COALESCE(NULLIF(a.actor_role,''),actor.role) NOT IN ('owner','creator','admin','moderator','support'))");
 if(actorId>0){where.push('a.actor_id=?');params.push(actorId)}
 if(targetId>0){where.push('a.target_user_id=?');params.push(targetId)}
 if(actorUsername){where.push('actor.username LIKE ?');params.push('%'+actorUsername+'%')}
 if(targetUsername){where.push('target.username LIKE ?');params.push('%'+targetUsername+'%')}
 if(action){where.push('a.action LIKE ?');params.push('%'+action+'%')}
 if(details){where.push('a.details LIKE ?');params.push('%'+details+'%')}
 if(deviceId){where.push('actor.device_id LIKE ?');params.push('%'+deviceId+'%')}
 if(from){where.push("datetime(a.created_at)>=datetime(?)");params.push(from+' 00:00:00')}
 if(to){where.push("datetime(a.created_at)<datetime(?, '+1 day')");params.push(to+' 00:00:00')}
 const sql=`SELECT a.*,actor.username actor_username,actor.display_name actor_display_name,COALESCE(NULLIF(a.actor_role,''),actor.role) actor_role,target.username target_username,target.display_name target_display_name FROM activity_log a LEFT JOIN users actor ON actor.id=a.actor_id LEFT JOIN users target ON target.id=a.target_user_id ${where.length?'WHERE '+where.join(' AND '):''} ORDER BY a.id DESC LIMIT ?`;
 params.push(limit);
 res.json({activities:db.prepare(sql).all(...params)});
});
app.get('/api/activity/me',requireAuth,(req,res)=>{
 const limit=Math.max(1,Math.min(200,Number(req.query.limit)||50));
 const rows=db.prepare("SELECT a.id,a.actor_id,a.target_user_id,a.action,a.details,a.created_at,target.username target_username FROM activity_log a LEFT JOIN users target ON target.id=a.target_user_id WHERE a.actor_id=? ORDER BY a.id DESC LIMIT ?").all(req.user.id,limit);
 res.json({activities:rows});
});
app.get('/api/admin/health',requireAuth,requirePermission('lobby.view'),(req,res)=>{const tables=db.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE type='table'").get().n;res.json({ok:true,uptimeSeconds:Math.floor(process.uptime()),memoryBytes:process.memoryUsage().rss,tableCount:tables,onlineUsers:db.prepare("SELECT COUNT(*) n FROM users WHERE last_seen>=datetime('now','-2 minutes') AND is_banned=0").get().n,activeRooms:db.prepare("SELECT COUNT(*) n FROM game_rooms WHERE status IN ('waiting','playing')").get().n,openReports:db.prepare("SELECT COUNT(*) n FROM reports WHERE status='new'").get().n});});
function zipCrc32(buf){let c=0xffffffff;for(const byte of buf){c^=byte;for(let k=0;k<8;k++)c=(c&1)?(0xedb88320^(c>>>1)):(c>>>1)}return (c^0xffffffff)>>>0;}
function makeZip(entries){const locals=[],centrals=[];let offset=0;for(const ent of entries){const name=Buffer.from(ent.name.replace(/\\/g,'/'),'utf8'),data=Buffer.isBuffer(ent.data)?ent.data:Buffer.from(ent.data),compressed=zlib.deflateRawSync(data),crc=zipCrc32(data);const lh=Buffer.alloc(30);lh.writeUInt32LE(0x04034b50,0);lh.writeUInt16LE(20,4);lh.writeUInt16LE(0x800,6);lh.writeUInt16LE(8,8);lh.writeUInt32LE(crc,14);lh.writeUInt32LE(compressed.length,18);lh.writeUInt32LE(data.length,22);lh.writeUInt16LE(name.length,26);locals.push(lh,name,compressed);const ch=Buffer.alloc(46);ch.writeUInt32LE(0x02014b50,0);ch.writeUInt16LE(20,4);ch.writeUInt16LE(20,6);ch.writeUInt16LE(0x800,8);ch.writeUInt16LE(8,10);ch.writeUInt32LE(crc,16);ch.writeUInt32LE(compressed.length,20);ch.writeUInt32LE(data.length,24);ch.writeUInt16LE(name.length,28);ch.writeUInt32LE(offset,42);centrals.push(ch,name);offset+=lh.length+name.length+compressed.length;}const centralSize=centrals.reduce((n,b)=>n+b.length,0),end=Buffer.alloc(22);end.writeUInt32LE(0x06054b50,0);end.writeUInt16LE(entries.length,8);end.writeUInt16LE(entries.length,10);end.writeUInt32LE(centralSize,12);end.writeUInt32LE(offset,16);return Buffer.concat([...locals,...centrals,end]);}
app.get('/api/user/account-backup.json',requireAuth,(req,res)=>{const u=db.prepare('SELECT id,username,display_name,bio,gender,age,province,avatar,coins,diamonds,xp,level,fame,created_at FROM users WHERE id=?').get(req.user.id);const payload={format:'ACECLUB_ACCOUNT_BACKUP_V2',account:u,exported_at:new Date().toISOString()};const signature=crypto.createHmac('sha256',creatorRecoveryKey).update(JSON.stringify(payload)).digest('hex');res.setHeader('Content-Disposition',`attachment; filename=aceclub-account-${req.user.id}-${new Date().toISOString().slice(0,10)}.json`);res.setHeader('Cache-Control','no-store');res.json({...payload,signature,notice:'فایل امضاشده است. اطلاعات پروفایل و موجودی امضاشده را می‌توان برای بازیابی حساب استفاده کرد؛ نقش مدیریتی و رمز عبور منتقل نمی‌شود.'});});
app.post('/api/user/account-restore',(req,res)=>{const b=req.body||{},payload={format:b.format,account:b.account,exported_at:b.exported_at};if(b.format!=='ACECLUB_ACCOUNT_BACKUP_V2'||!b.account||typeof b.signature!=='string')return res.status(400).json({error:'فایل پشتیبان معتبر نیست یا از نسخه قدیمی است'});const expected=crypto.createHmac('sha256',creatorRecoveryKey).update(JSON.stringify(payload)).digest('hex');if(!/^[a-f0-9]{64}$/i.test(b.signature)||!crypto.timingSafeEqual(Buffer.from(expected,'hex'),Buffer.from(b.signature,'hex')))return res.status(400).json({error:'امضای پشتیبان معتبر نیست؛ فایل دست‌کاری شده یا کلید بازیابی سرور تغییر کرده است'});const a=b.account,username=String(a.username||'').trim();if(!/^[a-zA-Z0-9_]{3,24}$/.test(username))return res.status(400).json({error:'نام کاربری داخل پشتیبان معتبر نیست'});const existing=userByName(username);if(existing){if(!req.session?.uid||Number(req.session.uid)!==Number(existing.id))return res.status(403).json({error:'این حساب هنوز وجود دارد؛ برای بازیابی باید ابتدا با همان حساب وارد شوید'});const name=String(a.display_name||'').trim().slice(0,40);if(!name)return res.status(400).json({error:'نام نمایشی فایل معتبر نیست'});const bio=String(a.bio||'').trim().slice(0,300),province=String(a.province||'').trim().slice(0,60),gender=['زن','مرد','دیگر','نامشخص'].includes(a.gender)?a.gender:'نامشخص',age=Number.isInteger(Number(a.age))&&Number(a.age)>=13&&Number(a.age)<=120?Number(a.age):18,avatar=typeof a.avatar==='string'&&a.avatar.startsWith('/uploads/')&&!a.avatar.includes('..')?a.avatar:'';db.prepare('UPDATE users SET display_name=?,bio=?,province=?,gender=?,age=?,avatar=?,coins=?,diamonds=?,xp=?,level=?,fame=? WHERE id=?').run(name,bio,province,gender,age,avatar,Math.max(0,Number(a.coins)||0),Math.max(0,Math.floor(Number(a.diamonds)||0)),Math.max(0,Number(a.xp)||0),Math.max(1,Math.floor(Number(a.level)||1)),Math.max(0,Number(a.fame)||0),existing.id);syncUserFameAndLevel(existing.id);return res.json({ok:true,restoredExisting:true,message:'پروفایل و موجودی ثبت‌شده در فایل امضاشده بازیابی شد. رمز عبور و نقش حساب تغییر نکرد.'});}const password=String(b.newPassword||'');if(password.length<8||!/[A-Za-z]/.test(password)||!/[0-9]/.test(password))return res.status(400).json({error:'برای بازیابی حساب حذف‌شده، رمز جدید حداقل ۸ نویسه و شامل حرف انگلیسی و عدد وارد کن'});const name=String(a.display_name||username).trim().slice(0,40);if(name.length<2)return res.status(400).json({error:'نام نمایشی فایل معتبر نیست'});const bio=String(a.bio||'').trim().slice(0,300),province=String(a.province||'').trim().slice(0,60),gender=['زن','مرد','دیگر','نامشخص'].includes(a.gender)?a.gender:'نامشخص',age=Number.isInteger(Number(a.age))&&Number(a.age)>=13&&Number(a.age)<=120?Number(a.age):18,avatar=typeof a.avatar==='string'&&a.avatar.startsWith('/uploads/')&&!a.avatar.includes('..')?a.avatar:'';try{const r=db.prepare('INSERT INTO users(username,display_name,password_hash,bio,gender,age,province,avatar,coins,diamonds,xp,level,fame,role) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(username,name,bcrypt.hashSync(password,12),bio,gender,age,province,avatar,Math.max(0,Number(a.coins)||0),Math.max(0,Math.floor(Number(a.diamonds)||0)),Math.max(0,Number(a.xp)||0),Math.max(1,Math.floor(Number(a.level)||1)),Math.max(0,Number(a.fame)||0),'user');const uid=Number(r.lastInsertRowid);logActivity(uid,uid,'account_backup_restore','بازیابی حساب از پشتیبان امضاشده');req.session.uid=uid;recordLoginDay(uid);syncUserFameAndLevel(uid);res.json({ok:true,restoredExisting:false,user:safeUser(userById(uid)),message:'حساب از پشتیبان معتبر بازیابی شد. برای امنیت، رمز جدید تنظیم شد و نقش مدیریتی منتقل نشد.'});}catch(e){res.status(409).json({error:'ساخت حساب از پشتیبان انجام نشد؛ ممکن است نام کاربری هم‌زمان ثبت شده باشد'});}});
app.get('/api/user/backup.zip',requireAuth,(req,res)=>{try{const uid=req.user.id;const user=db.prepare('SELECT id,username,display_name,bio,gender,age,province,avatar,coins,diamonds,xp,level,created_at FROM users WHERE id=?').get(uid);const conversations=db.prepare('SELECT * FROM conversations WHERE user_a=? OR user_b=?').all(uid,uid);const cids=conversations.map(c=>c.id);const messages=cids.length?db.prepare(`SELECT m.* FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE (c.user_a=? OR c.user_b=?) ORDER BY m.id`).all(uid,uid):[];const data={exported_at:new Date().toISOString(),account:user,conversations,messages,notifications:db.prepare('SELECT title,body,action,created_at FROM notifications WHERE user_id=? ORDER BY id').all(uid),transactions:db.prepare('SELECT amount,kind,note,created_at FROM transactions WHERE user_id=? ORDER BY id').all(uid),tickets:db.prepare('SELECT id,subject,status,created_at FROM tickets WHERE user_id=? ORDER BY id').all(uid),game_results:db.prepare('SELECT game_type,result,fame_delta,created_at FROM game_results WHERE user_id=? ORDER BY id').all(uid)};const entries=[{name:'خلاصه/اطلاعات-حساب.json',data:Buffer.from(JSON.stringify(data,null,2),'utf8')},{name:'عکس‌ها/',data:Buffer.alloc(0)},{name:'فیلم‌ها/',data:Buffer.alloc(0)},{name:'ویس‌ها/',data:Buffer.alloc(0)},{name:'فایل‌ها/',data:Buffer.alloc(0)}];const mediaSeen=new Set();const addMedia=(url,preferred)=>{if(!url||typeof url!=='string')return;const clean=url.split('?')[0];if(!clean.startsWith('/uploads/'))return;const rel=clean.slice('/uploads/'.length);if(!rel||rel.includes('..')||mediaSeen.has(rel))return;const full=path.resolve(uploadsDir,rel);if(!full.startsWith(path.resolve(uploadsDir)+path.sep)||!fs.existsSync(full)||!fs.statSync(full).isFile())return;mediaSeen.add(rel);const ext=path.extname(rel).toLowerCase();const dest=['عکس‌ها','فیلم‌ها','ویس‌ها','فایل‌ها'].includes(preferred)?preferred:/\.(mp4|mov|m4v|avi|mkv)$/.test(ext)?'فیلم‌ها':/\.(mp3|wav|ogg|m4a|aac|opus|webm)$/.test(ext)&&/voice|audio/i.test(rel)?'ویس‌ها':/\.(mp3|wav|ogg|m4a|aac|opus)$/.test(ext)?'ویس‌ها':/\.(jpg|jpeg|png|webp|gif|avif)$/.test(ext)?'عکس‌ها':'فایل‌ها';entries.push({name:`${dest}/${path.basename(rel)}`,data:fs.readFileSync(full)});};addMedia(user.avatar,'عکس‌ها');for(const m of messages){const t=String(m.type||'').toLowerCase();addMedia(m.media_url,/voice|audio/.test(t)?'ویس‌ها':/video|movie/.test(t)?'فیلم‌ها':/image|photo|picture/.test(t)?'عکس‌ها':'');}const archive=makeZip(entries);res.setHeader('Content-Type','application/zip');res.setHeader('Content-Disposition',`attachment; filename=aceclub-user-backup-${uid}-${new Date().toISOString().slice(0,10)}.zip`);res.setHeader('Cache-Control','no-store');res.send(archive);}catch(e){console.error('User backup failed',e);res.status(500).json({error:'ساخت پشتیبان شخصی ناموفق بود'});}});
app.get('/api/admin/backup.full',requireAuth,(req,res,next)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'پشتیبان کامل فقط برای سازنده فعال است'});next();},(req,res)=>{try{const dbBytes=Buffer.from(rawDb.export());const digest=crypto.createHash('sha256').update(dbBytes).digest('hex');const creatorName=String(req.user.username).toLowerCase();const signature=crypto.createHmac('sha256',creatorRecoveryKey).update('ACECLUB_CREATOR_BACKUP_V1|'+digest+'|'+creatorName).digest('hex');const manifest=Buffer.from(JSON.stringify({format:'ACECLUB_CREATOR_BACKUP_V1',db_sha256:digest,creator_username:creatorName,signature,created_at:new Date().toISOString()}));const entries=[{name:'database/dordor.sqlite',data:dbBytes},{name:'recovery/manifest.json',data:manifest},...listFiles(uploadsDir,'uploads')];const archive=zlib.gzipSync(makeTar(entries),{level:6});res.setHeader('Content-Type','application/gzip');res.setHeader('Content-Disposition',`attachment; filename=aceclub-full-backup-${new Date().toISOString().slice(0,10)}.tar.gz`);res.setHeader('Cache-Control','no-store');res.send(archive);}catch(e){res.status(500).json({error:'ساخت پشتیبان کامل ناموفق بود'});}});
function upgradeRestoredDatabase(candidate){
 const add=(table,col,def)=>{try{candidate.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${def}`)}catch(e){if(!/duplicate column|no such table/i.test(String(e.message)))throw e}};
 const create=(sql)=>candidate.exec(sql);
 create("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, display_name TEXT NOT NULL, phone TEXT UNIQUE, password_hash TEXT NOT NULL, bio TEXT DEFAULT '', gender TEXT DEFAULT 'نامشخص', age INTEGER DEFAULT 18, province TEXT DEFAULT '', avatar TEXT DEFAULT '', coins REAL DEFAULT 100, diamonds INTEGER DEFAULT 0, xp INTEGER DEFAULT 0, level INTEGER DEFAULT 1, fame INTEGER DEFAULT 0, role TEXT DEFAULT 'user', is_banned INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, last_seen TEXT DEFAULT CURRENT_TIMESTAMP, terms_accepted_at TEXT)");
 create("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)");
 create("CREATE TABLE IF NOT EXISTS activity_log(id INTEGER PRIMARY KEY AUTOINCREMENT,actor_id INTEGER,target_user_id INTEGER,action TEXT NOT NULL,details TEXT DEFAULT '',actor_role TEXT DEFAULT '',created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS user_permissions(user_id INTEGER NOT NULL,permission TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(user_id,permission))");
 create("CREATE TABLE IF NOT EXISTS pro_usage(user_id INTEGER NOT NULL,window_key TEXT NOT NULL,window_start TEXT NOT NULL,used INTEGER DEFAULT 0,PRIMARY KEY(user_id,window_key))");
 create("CREATE TABLE IF NOT EXISTS follows(id INTEGER PRIMARY KEY AUTOINCREMENT,follower_id INTEGER NOT NULL,followed_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(follower_id,followed_id))");
 create("CREATE TABLE IF NOT EXISTS likes(id INTEGER PRIMARY KEY AUTOINCREMENT,from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(from_id,to_id))");
 create("CREATE TABLE IF NOT EXISTS conversations(id INTEGER PRIMARY KEY AUTOINCREMENT,user_a INTEGER NOT NULL,user_b INTEGER NOT NULL,pinned_a INTEGER DEFAULT 0,pinned_b INTEGER DEFAULT 0,hidden_a INTEGER DEFAULT 0,hidden_b INTEGER DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,closed_at TEXT,closed_by INTEGER,UNIQUE(user_a,user_b))");
 create("CREATE TABLE IF NOT EXISTS group_message_reads(group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,last_read_message_id INTEGER NOT NULL DEFAULT 0,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(group_id,user_id))");
 create("CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT,conversation_id INTEGER NOT NULL,sender_id INTEGER NOT NULL,type TEXT DEFAULT 'text',body TEXT DEFAULT '',media_url TEXT DEFAULT '',deleted INTEGER DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,delivered_at TEXT,read_at TEXT,reply_to INTEGER)");
 create("CREATE TABLE IF NOT EXISTS notifications(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,title TEXT NOT NULL,body TEXT DEFAULT '',read_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,action TEXT,from_id INTEGER,request_id INTEGER)");
 create("CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,amount REAL NOT NULL,kind TEXT NOT NULL,note TEXT DEFAULT '',created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS tickets(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,subject TEXT NOT NULL,status TEXT DEFAULT 'open',created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS ticket_messages(id INTEGER PRIMARY KEY AUTOINCREMENT,ticket_id INTEGER NOT NULL,sender_id INTEGER NOT NULL,body TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS random_queue(user_id INTEGER PRIMARY KEY,mode TEXT DEFAULT 'random',gender_pref TEXT DEFAULT 'any',age_min INTEGER DEFAULT 18,age_max INTEGER DEFAULT 99,province TEXT DEFAULT '',created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS random_blocks(user_id INTEGER NOT NULL,blocked_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(user_id,blocked_id))");
 create("CREATE TABLE IF NOT EXISTS random_reports(id INTEGER PRIMARY KEY AUTOINCREMENT,reporter_id INTEGER NOT NULL,reported_id INTEGER NOT NULL,room_id TEXT DEFAULT '',reason TEXT DEFAULT 'گزارش از چت ناشناس',created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS reports(id INTEGER PRIMARY KEY AUTOINCREMENT,reporter_id INTEGER NOT NULL,reported_user_id INTEGER,category TEXT NOT NULL,target_type TEXT NOT NULL,target_id TEXT NOT NULL,reason TEXT NOT NULL,details TEXT DEFAULT '',status TEXT NOT NULL DEFAULT 'new',reviewed_by INTEGER,reviewed_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS chat_groups(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,group_code TEXT UNIQUE NOT NULL,description TEXT DEFAULT '',avatar TEXT DEFAULT '',owner_id INTEGER NOT NULL,coins INTEGER NOT NULL DEFAULT 0,fame INTEGER NOT NULL DEFAULT 0,muted INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,promoted_remaining INTEGER NOT NULL DEFAULT 0,promoted_until TEXT)");
 create("CREATE TABLE IF NOT EXISTS chat_group_members(group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,role TEXT NOT NULL DEFAULT 'member',muted INTEGER NOT NULL DEFAULT 0,joined_at TEXT DEFAULT CURRENT_TIMESTAMP,notifications_muted INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(group_id,user_id))");
 create("CREATE TABLE IF NOT EXISTS chat_group_messages(id INTEGER PRIMARY KEY AUTOINCREMENT,group_id INTEGER NOT NULL,sender_id INTEGER NOT NULL,body TEXT DEFAULT '',type TEXT NOT NULL DEFAULT 'text',media_url TEXT DEFAULT '',deleted INTEGER NOT NULL DEFAULT 0,pinned INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,reply_to INTEGER)");
 create("CREATE TABLE IF NOT EXISTS chat_group_reports(id INTEGER PRIMARY KEY AUTOINCREMENT,group_id INTEGER NOT NULL,reporter_id INTEGER NOT NULL,reason TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'new',created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS chat_group_invites(id INTEGER PRIMARY KEY AUTOINCREMENT,group_id INTEGER NOT NULL,from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'pending',created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(group_id,to_id))");
 create("CREATE TABLE IF NOT EXISTS group_reward_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,group_id INTEGER NOT NULL,user_id INTEGER NOT NULL,reward_date TEXT NOT NULL DEFAULT (date('now')),coins INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(group_id,user_id,reward_date))");
 create("CREATE TABLE IF NOT EXISTS wheel_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,prize TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS friend_requests(id INTEGER PRIMARY KEY AUTOINCREMENT,from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,status TEXT DEFAULT 'pending',created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(from_id,to_id))");
 create("CREATE TABLE IF NOT EXISTS message_requests(id INTEGER PRIMARY KEY AUTOINCREMENT,from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,status TEXT DEFAULT 'pending',created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(from_id,to_id))");
 create("CREATE TABLE IF NOT EXISTS blocks(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,blocked_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(user_id,blocked_id))");
 create("CREATE TABLE IF NOT EXISTS profile_views(id INTEGER PRIMARY KEY AUTOINCREMENT,viewer_id INTEGER NOT NULL,viewed_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS game_rooms(id INTEGER PRIMARY KEY AUTOINCREMENT,game_type TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'waiting',created_by INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,started_at TEXT,finished_at TEXT,winner_id INTEGER,deadline INTEGER NOT NULL,state_json TEXT NOT NULL DEFAULT '{}',room_code TEXT)");
 create("CREATE TABLE IF NOT EXISTS game_players(room_id INTEGER NOT NULL,user_id INTEGER NOT NULL,position INTEGER NOT NULL DEFAULT 0,choice TEXT,roll INTEGER,joined_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(room_id,user_id))");
 create("CREATE TABLE IF NOT EXISTS game_results(id INTEGER PRIMARY KEY AUTOINCREMENT,room_id INTEGER NOT NULL,game_type TEXT NOT NULL,user_id INTEGER NOT NULL,result TEXT NOT NULL,fame_delta INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS game_reward_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,period_key TEXT NOT NULL,place INTEGER NOT NULL,reward_type TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(user_id,period_key))");
 create("CREATE TABLE IF NOT EXISTS level_rewards(user_id INTEGER NOT NULL,level INTEGER NOT NULL,diamonds INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,level))");
 create("CREATE TABLE IF NOT EXISTS daily_presence_rewards(user_id INTEGER PRIMARY KEY,claimed_at INTEGER NOT NULL)");
 create("CREATE TABLE IF NOT EXISTS challenge_claims(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,challenge_id TEXT NOT NULL,period_key TEXT NOT NULL,reward_coins INTEGER NOT NULL DEFAULT 0,reward_fame INTEGER NOT NULL DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(user_id,challenge_id,period_key))");
 create("CREATE TABLE IF NOT EXISTS user_login_days(user_id INTEGER NOT NULL,day_key TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,day_key))");
 create("CREATE TABLE IF NOT EXISTS referrals(inviter_id INTEGER NOT NULL,invitee_id INTEGER NOT NULL UNIQUE,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(inviter_id,invitee_id))");
 create("CREATE TABLE IF NOT EXISTS daily_ranking_distributions(period_key TEXT PRIMARY KEY,distributed_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS custom_challenges(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,category TEXT NOT NULL DEFAULT 'چالش اختصاصی مدیر',metric TEXT NOT NULL,target INTEGER NOT NULL,scope TEXT NOT NULL DEFAULT 'permanent',reward_coins INTEGER NOT NULL DEFAULT 0,reward_fame INTEGER NOT NULL DEFAULT 0,starts_at TEXT,ends_at TEXT,active INTEGER NOT NULL DEFAULT 1,created_by INTEGER,created_at TEXT DEFAULT CURRENT_TIMESTAMP)");
 create("CREATE TABLE IF NOT EXISTS fame_like_awards(from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(from_id,to_id))");
 create("CREATE TABLE IF NOT EXISTS post_fame_like_awards(post_id INTEGER NOT NULL,from_id INTEGER NOT NULL,to_id INTEGER NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(post_id,from_id))");
 for(const [t,c,d] of [
  ['activity_log','actor_role',"TEXT DEFAULT ''"],['messages','delivered_at','TEXT'],['messages','read_at','TEXT'],['messages','reply_to','INTEGER'],['users','terms_accepted_at','TEXT'],['users','ban_reason','TEXT'],['users','ban_until','TEXT'],['users','message_pass_until','TEXT'],['users','diamonds','INTEGER DEFAULT 0'],['users','face_verified','INTEGER DEFAULT 0'],['users','boost_until','TEXT'],['users','pro_until','TEXT'],['users','device_id','TEXT'],['users','is_game_banned','INTEGER DEFAULT 0'],['notifications','action','TEXT'],['notifications','from_id','INTEGER'],['notifications','request_id','INTEGER'],['chat_groups','promoted_remaining','INTEGER NOT NULL DEFAULT 0'],['chat_groups','promoted_until','TEXT'],['chat_group_members','notifications_muted','INTEGER NOT NULL DEFAULT 0'],['chat_group_messages','type',"TEXT NOT NULL DEFAULT 'text'"],['chat_group_messages','media_url',"TEXT DEFAULT ''"],['chat_group_messages','deleted','INTEGER NOT NULL DEFAULT 0'],['chat_group_messages','pinned','INTEGER NOT NULL DEFAULT 0'],['chat_group_messages','reply_to','INTEGER'],['game_rooms','room_code','TEXT']
 ])add(t,c,d);
 // Repair relational invariants after restoring older backups. These are additive/safe repairs;
 // user content is retained while orphaned links are removed and group owners regain membership.
 for (const sql of [
  'DELETE FROM follows WHERE follower_id NOT IN (SELECT id FROM users) OR followed_id NOT IN (SELECT id FROM users) OR follower_id=followed_id',
  'DELETE FROM likes WHERE from_id NOT IN (SELECT id FROM users) OR to_id NOT IN (SELECT id FROM users) OR from_id=to_id',
  'DELETE FROM chat_group_members WHERE group_id NOT IN (SELECT id FROM chat_groups) OR user_id NOT IN (SELECT id FROM users)',
  'DELETE FROM chat_group_messages WHERE group_id NOT IN (SELECT id FROM chat_groups) OR sender_id NOT IN (SELECT id FROM users)',
  'DELETE FROM chat_group_invites WHERE group_id NOT IN (SELECT id FROM chat_groups) OR from_id NOT IN (SELECT id FROM users) OR to_id NOT IN (SELECT id FROM users)',
  'DELETE FROM friend_requests WHERE from_id NOT IN (SELECT id FROM users) OR to_id NOT IN (SELECT id FROM users) OR from_id=to_id',
  'DELETE FROM notifications WHERE user_id NOT IN (SELECT id FROM users)',
  'DELETE FROM user_permissions WHERE user_id NOT IN (SELECT id FROM users)',
  'DELETE FROM pro_usage WHERE user_id NOT IN (SELECT id FROM users)'
 ]) candidate.exec(sql);
 // Repair group membership after restore without removing any valid group or member.
 const groups=candidate.exec('SELECT id,owner_id FROM chat_groups')[0]?.values||[];
 for(const [gid,ownerId] of groups){
  const ownerExists=candidate.exec('SELECT id FROM users WHERE id='+Number(ownerId))[0]?.values?.length>0;
  if(ownerExists) candidate.run("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'owner')",[gid,ownerId]);
 }
 // A restore can contain users but lack the official community group's membership rows.
 // Recreate that group only when absent, then add missing memberships idempotently.
 let official=candidate.exec("SELECT id FROM chat_groups WHERE name='گروه ایس کلاب' ORDER BY id LIMIT 1")[0]?.values?.[0]?.[0];
 const firstOwner=candidate.exec("SELECT id FROM users WHERE role IN ('owner','creator') ORDER BY CASE role WHEN 'owner' THEN 0 ELSE 1 END,id LIMIT 1")[0]?.values?.[0]?.[0];
 if(!official && firstOwner){
  let code='ACECLUB_OFFICIAL';
  const codeTaken=candidate.exec('SELECT id FROM chat_groups WHERE group_code='+JSON.stringify(code))[0]?.values?.length;
  if(codeTaken) code='ACECLUB_OFFICIAL_'+Date.now().toString(36).toUpperCase();
  candidate.run('INSERT INTO chat_groups(name,group_code,description,owner_id) VALUES(?,?,?,?)',['گروه ایس کلاب',code,'گروه همگانی رسمی ایس کلاب',firstOwner]);
  official=candidate.exec("SELECT id FROM chat_groups WHERE name='گروه ایس کلاب' ORDER BY id LIMIT 1")[0]?.values?.[0]?.[0];
 }
 if(official){
  const users=candidate.exec('SELECT id FROM users')[0]?.values||[];
  for(const [uid] of users) candidate.run("INSERT OR IGNORE INTO chat_group_members(group_id,user_id,role) VALUES(?,?,'member')",[official,uid]);
  if(firstOwner) candidate.run("UPDATE chat_group_members SET role='owner' WHERE group_id=? AND user_id=?",[official,firstOwner]);
 }
 const hasAdminHash=candidate.exec("SELECT 1 FROM settings WHERE key='admin_panel_password_hash'")[0]?.values?.length>0;
 if(!hasAdminHash) candidate.run("INSERT INTO settings(key,value) VALUES('admin_panel_password_hash',?)",[bcrypt.hashSync(String(cfg('ADMIN_PANEL_PASSWORD')||'0P9O8I7U'),12)]);
 // Keep large follow/invite lists fast after restoring older databases.
 for(const sql of [
  'CREATE INDEX IF NOT EXISTS idx_follows_follower_id ON follows(follower_id,id)',
  'CREATE INDEX IF NOT EXISTS idx_follows_followed_id ON follows(followed_id,id)',
  'CREATE INDEX IF NOT EXISTS idx_group_members_group_user ON chat_group_members(group_id,user_id)',
  'CREATE INDEX IF NOT EXISTS idx_friend_requests_status_to ON friend_requests(status,to_id)',
  'CREATE INDEX IF NOT EXISTS idx_notifications_user_created ON notifications(user_id,created_at)'
 ]){ try { candidate.run(sql); } catch(e) { console.warn('Restore index skipped:',e.message); } }
}
app.post('/api/admin/restore.full',requireAuth,(req,res,next)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'فقط سازنده می‌تواند پشتیبان کامل را بازیابی کند'});next();},databaseUpload.single('file'),(req,res)=>{let candidate;try{if(!req.file)throw new Error('فایل پشتیبان کامل را انتخاب کن');const isGzip=req.file.buffer[0]===0x1f&&req.file.buffer[1]===0x8b;const isZip=req.file.buffer[0]===0x50&&req.file.buffer[1]===0x4b;if(!isGzip&&!isZip)throw new Error('فرمت فایل پشتیبان شناخته نشد؛ فایل کامل .tar.gz یا .zip شامل دیتابیس، یا فایل دیتابیس .sqlite را انتخاب کن');const files=isGzip?readTarGz(req.file.buffer):readZip(req.file.buffer);const dbEntry=files.find(f=>{const n=f.name.replace(/\\/g,'/').split('/').pop().toLowerCase();return ['dordor.sqlite','dordor.db','backup.sqlite','database.sqlite','app.sqlite'].includes(n)||(n.endsWith('.sqlite')||n.endsWith('.db'))&&!n.endsWith('-shm')&&!n.endsWith('-wal')});if(!dbEntry)throw new Error('داخل این آرشیو فایل دیتابیس پیدا نشد؛ احتمالاً این ZIP سورس برنامه است، نه پشتیبان دیتابیس. فایل پشتیبان کامل یا .sqlite را انتخاب کن');for(const f of files){if(!safeArchivePath(f.name))throw new Error('آرشیو شامل مسیر غیرمجاز است');}candidate=new SQL.Database(new Uint8Array(dbEntry.data));upgradeRestoredDatabase(candidate);const integrity=candidate.exec('PRAGMA quick_check');if(integrity?.[0]?.values?.[0]?.[0]!=='ok')throw new Error('فایل دیتابیس آسیب‌دیده است و برای جلوگیری از خرابی اطلاعات بازیابی نشد');const names=candidate.exec("SELECT name FROM sqlite_master WHERE type='table'")[0]?.values?.map(r=>r[0])||[];if(!names.includes('users')||!names.includes('settings'))throw new Error('این فایل دیتابیس برنامه ACE CLUB نیست یا جدول کاربران در آن وجود ندارد');const userCols=candidate.exec('PRAGMA table_info(users)')[0]?.values?.map(r=>r[1])||[];if(!['id','username','role'].every(n=>userCols.includes(n)))throw new Error('ساختار پایه حساب‌های کاربری معتبر نیست');const userCount=candidate.exec('SELECT COUNT(*) FROM users')[0]?.values?.[0]?.[0]||0;if(!userCount)throw new Error('نسخه پشتیبان هیچ حساب کاربری ندارد؛ دیتابیس فعلی حفظ شد');const uploadFiles=files.filter(x=>/(^|\/)uploads\//.test(x.name)&&!x.name.endsWith('/'));
const tempUploads=uploadsDir+'.restore-'+crypto.randomBytes(6).toString('hex');
const shouldReplaceUploads=uploadFiles.length>0;
if(shouldReplaceUploads){
  fs.mkdirSync(tempUploads,{recursive:true});
  for(const f of uploadFiles){
    const rel=f.name.slice(f.name.lastIndexOf('uploads/')+'uploads/'.length);
    if(!rel)continue;
    const dest=path.resolve(tempUploads,rel);
    if(!dest.startsWith(path.resolve(tempUploads)+path.sep))throw new Error('مسیر آپلود نامعتبر است');
    fs.mkdirSync(path.dirname(dest),{recursive:true});
    fs.writeFileSync(dest,f.data,{mode:0o600,flag:'wx'});
  }
}
const restoredRow=candidate.prepare('SELECT id,role FROM users WHERE username=? COLLATE NOCASE').get([req.user.username]);const restoredAccount=restoredRow?{id:Number(restoredRow[0]),role:String(restoredRow[1]||'')}:null;if(!restoredAccount)throw new Error('حساب سازنده در نسخه پشتیبان پیدا نشد؛ بازیابی لغو شد تا دسترسی مدیریتی از بین نرود');if(!['owner','creator'].includes(restoredAccount.role))candidate.run("UPDATE users SET role='owner' WHERE id=?",[restoredAccount.id]);createSafetyBackup('pre-restore');
const previous=rawDb;
const oldUpload=uploadsDir+'.previous-'+crypto.randomBytes(4).toString('hex');
try{
  if(shouldReplaceUploads){
    if(fs.existsSync(uploadsDir))fs.renameSync(uploadsDir,oldUpload);
    fs.renameSync(tempUploads,uploadsDir);
  }
  rawDb=candidate;
  persistDb();
  req.session.uid=Number(restoredAccount.id);
  try{previous.close()}catch{}
  if(shouldReplaceUploads&&fs.existsSync(oldUpload))fs.rmSync(oldUpload,{recursive:true,force:true});
}catch(e){
  rawDb=previous;
  try{
    if(shouldReplaceUploads){
      if(fs.existsSync(uploadsDir))fs.rmSync(uploadsDir,{recursive:true,force:true});
      if(fs.existsSync(oldUpload))fs.renameSync(oldUpload,uploadsDir);
      if(fs.existsSync(tempUploads))fs.rmSync(tempUploads,{recursive:true,force:true});
    }
  }catch{}
  throw e;
}
res.json({ok:true,message:'پشتیبان بازیابی شد؛ دیتابیس با مسیر پایدار ذخیره شد و اطلاعات رسانه‌ای فقط در صورت وجود در بکاپ جایگزین شدند. صفحه را دوباره بارگذاری کن.'});}catch(e){try{candidate?.close()}catch{}res.status(400).json({error:e.message||'بازیابی پشتیبان کامل ناموفق بود'});}});
app.get('/api/admin/backup.sqlite',requireAuth,(req,res,next)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'پشتیبان کامل فقط برای سازنده فعال است'});next();},(req,res)=>{res.setHeader('Content-Disposition','attachment; filename=dordor-backup.sqlite');res.type('application/vnd.sqlite3').send(Buffer.from(rawDb.export()));});
app.post('/api/admin/restore.sqlite',requireAuth,(req,res,next)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'فقط سازنده می‌تواند نسخه پشتیبان را بازیابی کند'});next();},databaseUpload.single('file'),(req,res)=>{if(!req.file)return res.status(400).json({error:'فایل پشتیبان SQLite را انتخاب کن'});let candidate;try{candidate=new SQL.Database(new Uint8Array(req.file.buffer));upgradeRestoredDatabase(candidate);const integrity=candidate.exec('PRAGMA quick_check');if(integrity?.[0]?.values?.[0]?.[0]!=='ok')throw new Error('فایل دیتابیس آسیب‌دیده است و برای جلوگیری از خرابی اطلاعات بازیابی نشد');const names=candidate.exec("SELECT name FROM sqlite_master WHERE type='table'")[0]?.values?.map(r=>r[0])||[];if(!names.includes('users')||!names.includes('settings'))throw new Error('این فایل دیتابیس برنامه ACE CLUB نیست یا جدول کاربران در آن وجود ندارد');const userCols=candidate.exec('PRAGMA table_info(users)')[0]?.values?.map(r=>r[1])||[];if(!['id','username','role'].every(n=>userCols.includes(n)))throw new Error('ساختار پایه حساب‌های کاربری معتبر نیست');const userCount=candidate.exec('SELECT COUNT(*) FROM users')[0]?.values?.[0]?.[0]||0;if(!userCount)throw new Error('نسخه پشتیبان هیچ حساب کاربری ندارد؛ دیتابیس فعلی حفظ شد');const restoredRow=candidate.prepare('SELECT id,role FROM users WHERE username=? COLLATE NOCASE').get([req.user.username]);const restoredAccount=restoredRow?{id:Number(restoredRow[0]),role:String(restoredRow[1]||'')}:null;if(!restoredAccount)throw new Error('حساب فعلی در نسخه پشتیبان پیدا نشد؛ بازیابی لغو شد تا دسترسی حساب از بین نرود');if(['owner','creator'].includes(req.user.role)&&!['owner','creator'].includes(restoredAccount.role))candidate.run("UPDATE users SET role='owner' WHERE id=?",[restoredAccount.id]);createSafetyBackup('pre-restore');
const previous=rawDb;
rawDb=candidate;
try{
  persistDb();
  req.session.uid=Number(restoredAccount.id);
  try{previous.close()}catch{}
}catch(e){
  rawDb=previous;
  throw e;
}
res.json({ok:true,message:'پشتیبان SQLite بازیابی شد؛ ذخیره‌سازی بعدی تغییرات روی همین مسیر پایدار انجام می‌شود. صفحه را دوباره بارگذاری کن.'});}catch(e){try{candidate?.close()}catch{}res.status(400).json({error:e.message||'بازیابی فایل ناموفق بود'});}});
io.use((socket,next)=>{const req=socket.request;const sid=(req.headers.cookie||'').split(';').map(x=>x.trim()).find(x=>x.startsWith('dor.sid='));if(!sid)return next(new Error('unauthorized'));const cookie=require('cookie');const raw=cookie.parse(req.headers.cookie||'')['dor.sid'];if(!raw)return next(new Error('unauthorized'));const unsigned=raw.startsWith('s:')?raw.slice(2).split('.')[0]:raw;const sessionId=decodeURIComponent(unsigned);sessionStore.get(sessionId,(err,sess)=>{if(err||!sess?.uid)return next(new Error('unauthorized'));socket.userId=sess.uid;next();});});
const anonQueue = new Map();
const anonByUser = new Map();
const anonRooms = new Map();
let anonRoomSeq = 1;
function leaveAnonymous(uid, reason='گفتگوی ناشناس پایان یافت') {
  anonQueue.delete(uid);
  const roomId=anonByUser.get(uid); if(!roomId)return false;
  const room=anonRooms.get(roomId); anonByUser.delete(uid);
  if(room){
    const other=room.a===uid?room.b:room.a;
    anonByUser.delete(other); anonRooms.delete(roomId);
    io.to('user:'+other).emit('anon:ended',{reason:'طرف مقابل گفتگو را بست'});
    io.to('user:'+uid).emit('anon:ended',{reason});
  }
  return true;
}
io.on('connection',socket=>{
 socket.join('user:'+socket.userId);
 for(const g of db.prepare('SELECT group_id FROM chat_group_members WHERE user_id=?').all(socket.userId))socket.join('group:'+g.group_id);
 socket.on('group:join',data=>{const gid=Number(data?.groupId);if(Number.isInteger(gid)&&gid>0&&groupMember(gid,socket.userId))socket.join('group:'+gid)});
 socket.on('group:leave',data=>{const gid=Number(data?.groupId);if(Number.isInteger(gid)&&gid>0)socket.leave('group:'+gid)});
 socket.on('random:join',prefs=>{
  const uid=socket.userId;
  if(anonByUser.has(uid)){socket.emit('anon:locked',{message:'ابتدا گفتگوی ناشناس فعلی را ببند؛ تا آن زمان نمی‌توانی گفتگوی دیگری شروع کنی.'});return;}
  const p={mode:prefs?.mode==='advanced'?'advanced':'random',gender_pref:['زن','مرد','any'].includes(prefs?.gender_pref)?prefs.gender_pref:'any',age_min:Math.max(18,Number(prefs?.age_min)||18),age_max:Math.min(99,Number(prefs?.age_max)||99),province:String(prefs?.province||'').slice(0,50)};
  if(p.age_min>p.age_max){socket.emit('random:error',{message:'حداقل سن نباید از حداکثر سن بیشتر باشد'});return;}
  const mine=userById(uid); const candidates=[...anonQueue.values()].filter(q=>q.userId!==uid).filter(q=>{
    const other=userById(q.userId); if(!other||anonByUser.has(q.userId))return false;if(db.prepare('SELECT 1 FROM random_blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(uid,q.userId,q.userId,uid))return false;
    if(p.mode==='advanced'||q.prefs.mode==='advanced'){
      const mePref=p.gender_pref==='any'||other.gender===p.gender_pref;
      const themPref=q.prefs.gender_pref==='any'||mine.gender===q.prefs.gender_pref;
      return mePref&&themPref&&other.age>=p.age_min&&other.age<=p.age_max&&mine.age>=q.prefs.age_min&&mine.age<=q.prefs.age_max&&(!p.province||!other.province||other.province===p.province)&&(!q.prefs.province||!mine.province||mine.province===q.prefs.province);
    }
    return true;
  });
  if(candidates.length){
    const mate=candidates[0];anonQueue.delete(mate.userId);anonQueue.delete(uid);
    const roomId='anon-'+(anonRoomSeq++);anonRooms.set(roomId,{a:uid,b:mate.userId});anonByUser.set(uid,roomId);anonByUser.set(mate.userId,roomId);
    io.to('user:'+uid).emit('anon:matched',{roomId,label:'ناشناس ۱'});io.to('user:'+mate.userId).emit('anon:matched',{roomId,label:'ناشناس ۱'});
  }else{anonQueue.set(uid,{userId:uid,prefs:p});socket.emit('random:waiting',{message:'در حال پیدا کردن یک نفر…'});}
 });
 socket.on('anon:message',data=>{const uid=socket.userId,roomId=anonByUser.get(uid),room=anonRooms.get(roomId);const body=String(data?.body||'').trim().slice(0,2000);if(!room||!body)return;const other=room.a===uid?room.b:room.a;const msg={body,from:'ناشناس',createdAt:new Date().toISOString()};io.to('user:'+uid).emit('anon:message',msg);io.to('user:'+other).emit('anon:message',msg);});
 socket.on('anon:report',(payload={})=>{const uid=socket.userId,roomId=anonByUser.get(uid),room=anonRooms.get(roomId);if(!room)return;const other=room.a===uid?room.b:room.a;const reason=String(payload.reason||'گزارش از چت تصادفی').trim().slice(0,200);db.prepare('INSERT INTO random_reports(reporter_id,reported_id,room_id,reason) VALUES(?,?,?,?)').run(uid,other,String(roomId||''),reason);db.prepare("INSERT INTO reports(reporter_id,reported_user_id,category,target_type,target_id,reason,details) VALUES(?,?,'chat','random_chat',?,?,?)").run(uid,other,String(roomId||''),reason,'گزارش از چت تصادفی');});
 socket.on('anon:block',()=>{const uid=socket.userId,roomId=anonByUser.get(uid),room=anonRooms.get(roomId);if(!room)return;const other=room.a===uid?room.b:room.a;db.prepare('INSERT OR IGNORE INTO random_blocks(user_id,blocked_id) VALUES(?,?)').run(uid,other);leaveAnonymous(uid,'کاربر مسدود شد');socket.emit('anon:ended',{reason:'این کاربر مسدود شد'});});
 socket.on('anon:leave',()=>leaveAnonymous(socket.userId));
 socket.on('random:leave',()=>{anonQueue.delete(socket.userId);});
 socket.on('typing',data=>{const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(Number(data?.conversationId));if(convMember(c,socket.userId)&&!conversationClosed(c))io.to('user:'+otherId(c,socket.userId)).emit('typing',{conversationId:c.id,username:userById(socket.userId)?.display_name});});
 socket.on('call:signal',data=>{const cid=Number(data?.conversationId),target=Number(data?.targetId),signal=data?.signal;const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(cid);if(!c||conversationClosed(c)||!convMember(c,socket.userId)||![c.user_a,c.user_b].includes(target)||target===socket.userId||!signal||typeof signal!=='object')return;if(db.prepare('SELECT 1 FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)').get(socket.userId,target,target,socket.userId))return;const validDescription=signal.description&&['offer','answer'].includes(signal.description.type)&&typeof signal.description.sdp==='string'&&signal.description.sdp.length<100000;const validCandidate=signal.candidate&&typeof signal.candidate==='object'&&String(signal.candidate.candidate||'').length<4096;if(!validDescription&&!validCandidate)return;io.to('user:'+target).emit('call:signal',{conversationId:cid,fromId:socket.userId,signal});});
 socket.on('call:end',data=>{const cid=Number(data?.conversationId),target=Number(data?.targetId);const c=db.prepare('SELECT * FROM conversations WHERE id=?').get(cid);if(c&&!conversationClosed(c)&&convMember(c,socket.userId)&&[c.user_a,c.user_b].includes(target)&&target!==socket.userId)io.to('user:'+target).emit('call:end',{conversationId:cid,fromId:socket.userId});});
 socket.on('game:signal',data=>{const roomId=Number(data?.roomId),target=Number(data?.targetId);if(!roomId||!target)return;const room=db.prepare('SELECT * FROM game_rooms WHERE id=? AND room_code IS NOT NULL AND status=\'waiting\'').get(roomId);if(!room)return;const fromMember=db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(roomId,socket.userId);const toMember=db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(roomId,target);if(!fromMember||!toMember)return;io.to('user:'+target).emit('game:signal',{roomId,fromId:socket.userId,signal:data.signal});});
 socket.on('game:voice-off',data=>{const roomId=Number(data?.roomId);if(!roomId)return;const room=db.prepare('SELECT id FROM game_rooms WHERE id=? AND room_code IS NOT NULL AND status=\'waiting\'').get(roomId);if(!room||!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(roomId,socket.userId))return;const peers=db.prepare('SELECT user_id FROM game_players WHERE room_id=? AND user_id<>?').all(roomId,socket.userId);for(const p of peers)io.to('user:'+p.user_id).emit('game:voice-off',{roomId,fromId:socket.userId});});
 socket.on('disconnect',()=>{anonQueue.delete(socket.userId);leaveAnonymous(socket.userId,'اتصال شما قطع شد');});
});
// Multiplayer mini-games: server-authoritative rooms, ratings and rewards.
const GAME_RULES={classic:{min:2,max:2,target:8},advanced:{min:4,max:4,target:11},snakes:{min:2,max:5,target:100},rps:{min:2,max:2,target:1},connect4:{min:2,max:2,target:4},drawguess:{min:2,max:8,target:1}};
const gameRoomState = (roomId,userId=null) => {
 const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(Number(roomId));
 if(!room)return null;
 const players=db.prepare('SELECT gp.user_id,gp.position,gp.choice,gp.roll,u.username,u.display_name,u.avatar,u.level FROM game_players gp JOIN users u ON u.id=gp.user_id WHERE gp.room_id=? ORDER BY gp.joined_at,gp.user_id').all(room.id);
 const safeState=JSON.parse(room.state_json||'{}');if(room.game_type==='drawguess'&&Number(safeState.drawer)!==Number(userId))safeState.word=null;const mappedPlayers=players.map(p=>({user_id:p.user_id,position:['classic','advanced'].includes(room.game_type)?(safeState.players?.[String(p.user_id)]?.position||{r:0,c:4}):p.position,walls:['classic','advanced'].includes(room.game_type)?(safeState.players?.[String(p.user_id)]?.walls??(room.game_type==='advanced'?20:10)):undefined,goal:['classic','advanced'].includes(room.game_type)?(safeState.players?.[String(p.user_id)]?.goal||'bottom'):undefined,team:safeState.players?.[String(p.user_id)]?.team,color:safeState.players?.[String(p.user_id)]?.color,roll:p.roll,username:p.username,display_name:p.display_name,avatar:p.avatar,level:p.level}));if(room.game_type==='advanced')for(const bid of (safeState.botIds||[])){const bp=safeState.players?.[String(bid)];if(bp)mappedPlayers.push({user_id:bid,position:bp.position,walls:bp.walls,goal:bp.goal,team:bp.team,color:bp.color,username:'ace_bot',display_name:bp.name||'ربات حرفه‌ای',avatar:null,level:50,isBot:true});}return {id:room.id,gameCode:room.game_code||('GAME-'+String(room.id).padStart(8,'0')),roomCode:room.room_code||null,gameType:room.game_type,status:room.status,createdAt:room.created_at,startedAt:room.started_at,deadline:room.deadline,winnerId:room.winner_id,createdBy:room.created_by,isPrivate:!!room.room_code,state:safeState,players:mappedPlayers};
};
function fameRequiredForLevel(level){
 const target=Math.max(1,Math.min(100,Math.floor(Number(level)||1)));
 if(target===1)return 0;
 if(target<=51){const t=(target-2)/49;return Math.round(100*Math.pow(10000000,t));}
 const t=(target-51)/49;return Math.round(1000000000*Math.pow(100,t));
}
function fameLevelForAmount(fame){
 const value=Math.max(0,Number(fame)||0);
 let level=1;
 while(level<100 && value>=fameRequiredForLevel(level+1)) level++;
 return level;
}
function syncUserFameAndLevel(uid, options={}){
 const u=userById(uid); if(!u)return;
 const currentFame=Math.max(0,Number(u.fame)||0);
 const currentXp=Math.max(0,Number(u.xp)||0);
 const fame=options.fame!==undefined ? Math.max(0,Number(options.fame)||0) : Math.max(currentFame,currentXp);
 let level=fameLevelForAmount(fame);
 if(options.level!==undefined){
  const requested=Math.max(1,Math.min(100,Math.floor(Number(options.level)||1)));
  if(requested>level){
   const required=fameRequiredForLevel(requested);
   if(required>fame){
    db.prepare('UPDATE users SET fame=?,xp=? WHERE id=?').run(required,required,uid);
    level=requested;
   }
  }else{
   level=level;
  }
 }
 if(fame>currentFame){const gained=Math.floor(fame-currentFame);if(gained>0)db.prepare('INSERT INTO fame_events(user_id,amount) VALUES(?,?)').run(uid,gained);}
 db.prepare('UPDATE users SET fame=?,xp=?,level=? WHERE id=?').run(fame,fame,level,uid);
 return {fame,level};
}
function recalcGameLevel(uid){
 const u=userById(uid); if(!u)return;
 const oldLevel=Math.max(1,Number(u.level)||1);
 const result=syncUserFameAndLevel(uid);
 const level=result.level;
 if(level>oldLevel){
  const base=Math.max(0,Number(getSetting('level_diamond_reward'))||10);
  for(let reached=oldLevel+1;reached<=level;reached++){
   const amount=base*reached;
   const exists=db.prepare('SELECT 1 FROM level_rewards WHERE user_id=? AND level=?').get(uid,reached);
   if(!exists){
    db.prepare('INSERT INTO level_rewards(user_id,level,diamonds) VALUES(?,?,?)').run(uid,reached,amount);
    db.prepare('UPDATE users SET diamonds=diamonds+? WHERE id=?').run(amount,uid);
    db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(uid,amount,'level_reward',`جایزه الماس ارتقای سطح ${reached}`);
    notify(uid,'ارتقای سطح',`به سطح ${reached} رسیدی و ${amount} الماس جایزه گرفتی.`);
   }
  }
 }
}
function finishGame(roomId,winnerId){
 const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(roomId); if(!room||room.status==='finished')return;
 const players=db.prepare('SELECT user_id FROM game_players WHERE room_id=?').all(roomId);
 db.prepare("UPDATE game_rooms SET status='finished',finished_at=CURRENT_TIMESTAMP,winner_id=? WHERE id=?").run(winnerId||null,roomId);
 if(room.game_type==='advanced'&&!room.room_code){
  const st=JSON.parse(room.state_json||'{}'),winningTeam=st.players?.[String(winnerId)]?.team;
  for(const p of players){const pp=st.players?.[String(p.user_id)],win=!!winnerId&&Number(pp?.team)===Number(winningTeam);db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,?)').run(roomId,room.game_type,p.user_id,win?'win':'loss',win?10000:0);if(win){const u=userById(p.user_id);syncUserFameAndLevel(p.user_id,{fame:Math.max(0,Number(u?.fame||0),Number(u?.xp||0))+10000});db.prepare('UPDATE users SET diamonds=diamonds+10 WHERE id=?').run(p.user_id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(p.user_id,10,'advanced_game_win_diamonds','جایزه برد بازی پیشرفته تیمی');recalcGameLevel(p.user_id);notify(p.user_id,'پیروزی در بازی پیشرفته','تیمت برنده شد؛ ۱۰ الماس و ۱۰۰۰۰ شهرت گرفتی.');}else notify(p.user_id,'نتیجه بازی پیشرفته','تیمت این بازی را واگذار کرد.');}return;
 }
 if(room.game_type==='classic'&&!room.room_code){
   for(const p of players){const win=Number(winnerId)===Number(p.user_id);db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,?)').run(roomId,room.game_type,p.user_id,win?'win':'loss',win?500:0);if(win){syncUserFameAndLevel(p.user_id,{fame:Math.max(0,Number(userById(p.user_id)?.fame||0),Number(userById(p.user_id)?.xp||0))+500});db.prepare('UPDATE users SET diamonds=diamonds+5 WHERE id=?').run(p.user_id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(p.user_id,5,'classic_game_win_diamonds','جایزه برد بازی کلاسیک');recalcGameLevel(p.user_id);notify(p.user_id,'پیروزی در بازی کلاسیک','۵ الماس و ۵۰۰ شهرت به حسابت اضافه شد.');}else notify(p.user_id,'نتیجه بازی کلاسیک','این بازی را به حریف واگذار کردی.');}return;
 }
 if(room.room_code){for(const p of players){const win=winnerId&&p.user_id===winnerId;db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,0)').run(roomId,room.game_type,p.user_id,win?'win':winnerId?'loss':'draw');}return;} // اتاق خصوصی در سوابق دیده می‌شود اما شهرت عمومی نمی‌دهد
 const winDelta=Math.max(1,Number(getSetting('game_win_fame'))||250), lossDelta=Math.max(1,Number(getSetting('game_loss_fame'))||30);
 for(const p of players){const win=winnerId&&p.user_id===winnerId;const delta=win?winDelta:-lossDelta;db.prepare('INSERT INTO game_results(room_id,game_type,user_id,result,fame_delta) VALUES(?,?,?,?,?)').run(roomId,room.game_type,p.user_id,win?'win':winnerId?'loss':'draw',winnerId?delta:0);if(winnerId){syncUserFameAndLevel(p.user_id,{fame:Math.max(0,Number(userById(p.user_id)?.fame||0),Number(userById(p.user_id)?.xp||0))+delta});recalcGameLevel(p.user_id);notify(p.user_id,win?'پیروزی در بازی':'نتیجه بازی',win?`برنده شدی؛ ${delta} شهرت گرفتی.`:`باختی؛ ${Math.abs(delta)} شهرت کم شد.`);}}
}

function advancedBotDistances(state,p,wallSet=state.walls||[]){
 const N=12,q=[[+p.position.r,+p.position.c]],dist=new Map([[q[0].join(','),0]]);
 const blocked=(r1,c1,r2,c2)=>{const dr=r2-r1,dc=c2-c1,h=(r,c)=>wallSet.some(w=>w.o==='h'&&+w.r===r&&+w.c===c),v=(r,c)=>wallSet.some(w=>w.o==='v'&&+w.r===r&&+w.c===c);if(dr===-1)return h(r2,c2)||h(r2,c2-1);if(dr===1)return h(r1,c1)||h(r1,c1-1);if(dc===-1)return v(r2,c2)||v(r2-1,c2);if(dc===1)return v(r1,c1)||v(r1-1,c1);return true;};
 while(q.length){const [r,c]=q.shift(),d=dist.get(`${r},${c}`);for(const [dr,dc] of [[-1,0],[1,0],[0,-1],[0,1]]){const nr=r+dr,nc=c+dc,k=`${nr},${nc}`;if(nr<0||nr>=N||nc<0||nc>=N||dist.has(k)||blocked(r,c,nr,nc))continue;dist.set(k,d+1);q.push([nr,nc]);}}
 const gr=p.goal==='top'?0:N-1;let best=Infinity;for(let c=0;c<N;c++){const d=dist.get(`${gr},${c}`);if(d!==undefined)best=Math.min(best,d);}return {dist,best};
}
function advancedBotWallLegal(state,o,r,c){
 const ws=state.walls||[],has=(a,b,d)=>ws.some(w=>w.o===a&&+w.r===b&&+w.c===d);
 if(r<0||c<0||r>10||c>10||has(o,r,c)||(o==='h'&&(has('h',r,c-1)||has('h',r,c+1)||has('v',r,c)))||(o==='v'&&(has('v',r-1,c)||has('v',r+1,c)||has('h',r,c))))return false;
 const test=[...ws,{o,r,c}];return Object.values(state.players||{}).every(p=>advancedBotDistances(state,p,test).best<Infinity);
}
function completeAdvancedRound(roomId,state,winnerUid){
 const team=Number(state.players?.[String(winnerUid)]?.team);
 if(!Number.isFinite(team))return false;
 state.roundWins=state.roundWins||{0:0,1:0};
 state.roundWins[team]=(Number(state.roundWins[team])||0)+1;
 const round=Number(state.round||1);
 state.roundHistory=Array.isArray(state.roundHistory)?state.roundHistory:[];
 state.roundHistory.push({round,winnerTeam:team,roundWins:{0:Number(state.roundWins[0]||0),1:Number(state.roundWins[1]||0)}});
 state.lastRoundResult={round,winnerTeam:team,roundWins:{0:Number(state.roundWins[0]||0),1:Number(state.roundWins[1]||0)}};
 if(Number(state.roundWins[team]||0)>=3){
  const winTeam=team;
  const humans=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(roomId).map(x=>Number(x.user_id));
  const winner=humans.find(id=>Number(state.players?.[String(id)]?.team)===winTeam) || humans[0] || Number(winnerUid);
  db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);
  finishGame(roomId,winner);return true;
 }
 state.round=round+1;state.walls=[];
 Object.values(state.players||{}).forEach(x=>{x.position={r:Number(x.team)===0?11:0,c:(x.color==='blue'||x.color==='yellow')?7:4};x.walls=20;});
 const order=(state.turnOrder||[]).map(Number);state.turn=order[Math.floor(Math.random()*order.length)]||order[0]||null;
 state.turnStartedAt=Date.now();state.turnReadyAt=Date.now()+(((state.botIds||[]).map(Number).includes(Number(state.turn)))?700:1000);
 db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);
 return true;
}

function chooseAdvancedBotWall(state,p){
 const all=Object.values(state.players||{}),own=all.filter(x=>+x.team===+p.team),opp=all.filter(x=>+x.team!==+p.team),walls=state.walls||[];
 if(Number(p.walls)<=0||!opp.length)return null;
 const baseOwn=Math.min(...own.map(x=>advancedBotDistances(state,x).best));
 const enemyBase=opp.map(x=>({player:x,distance:advancedBotDistances(state,x).best}));
 let chosen=null,scoreBest=-Infinity;
 // Evaluate legal walls against all routes. A wall is useful when it delays an opponent
 // without making our team's shortest route more than two steps longer.
 for(const o of ['h','v'])for(let r=0;r<11;r++)for(let c=0;c<11;c++){
  if(!advancedBotWallLegal(state,o,r,c))continue;
  const test=[...walls,{o,r,c}];
  const our=Math.min(...own.map(x=>advancedBotDistances(state,x,test).best));
  // Team wins when any teammate reaches the goal, so evaluate each enemy lane
  // individually; using only the opponent team's minimum distance made bots
  // refuse to wall whenever the other enemy still had an open lane.
  const gain=Math.max(...enemyBase.map(e=>advancedBotDistances(state,e.player,test).best-e.distance));
  const cost=our-baseOwn;
  if(gain<1||cost>2)continue;
  const score=gain*4-cost*1.5;
  if(score>scoreBest){scoreBest=score;chosen={o,r,c};}
 }
 return chosen;
}
function runAdvancedBots(roomId){
 for(let guard=0;guard<8;guard++){
  const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(roomId);if(!room||room.status!=='playing'||room.game_type!=='advanced')return;
  const st=JSON.parse(room.state_json||'{}'),bots=st.botIds||[],uid=Number(st.turn);if(!bots.includes(uid))return;
  if(Number(st.turnReadyAt||0)>Date.now())return;
  const p=st.players?.[String(uid)],order=(st.turnOrder||[]).map(Number);if(!p||!order.length)return;
  const all=Object.values(st.players||{}),own=all.filter(x=>+x.team===+p.team),opp=all.filter(x=>+x.team!==+p.team),walls=st.walls||[];
  // Bots now actively search for a legal strategic wall before deciding to move.
  const chosen=chooseAdvancedBotWall(st,p);
  if(chosen){st.walls=[...walls,chosen];const remain=Math.max(0,+p.walls-1);own.forEach(x=>x.walls=remain);}
  else{
   const r=+p.position.r,c=+p.position.c,occupied=new Set(all.filter(x=>x!==p).map(x=>`${x.position.r},${x.position.c}`)),opts=[];
   const blocked=(a,b,x,y)=>{const ws=st.walls||[],h=(rr,cc)=>ws.some(w=>w.o==='h'&&+w.r===rr&&+w.c===cc),v=(rr,cc)=>ws.some(w=>w.o==='v'&&+w.r===rr&&+w.c===cc);if(x-a===-1)return h(x,y)||h(x,y-1);if(x-a===1)return h(a,b)||h(a,b-1);if(y-b===-1)return v(x,y)||v(x-1,y);if(y-b===1)return v(a,b)||v(a-1,b);return true;};
   for(const [dr,dc] of [[-1,0],[1,0],[0,-1],[0,1]]){const nr=r+dr,nc=c+dc,k=`${nr},${nc}`;if(nr<0||nr>=12||nc<0||nc>=12||blocked(r,c,nr,nc))continue;
    if(!occupied.has(k)){const d=advancedBotDistances(st,{...p,position:{r:nr,c:nc}}).best;if(Number.isFinite(d))opts.push({r:nr,c:nc,d});continue;}
    const teammate=all.some(x=>x!==p&&+x.team===+p.team&&+x.position.r===nr&&+x.position.c===nc);if(!teammate)continue;
    const jr=nr+dr,jc=nc+dc,jk=`${jr},${jc}`;if(jr<0||jr>=12||jc<0||jc>=12||occupied.has(jk)||blocked(nr,nc,jr,jc))continue;
    const d=advancedBotDistances(st,{...p,position:{r:jr,c:jc}}).best;if(Number.isFinite(d))opts.push({r:jr,c:jc,d});
   }
   opts.sort((a,b)=>a.d-b.d);if(!opts.length){st.turn=order[(order.indexOf(uid)+1)%order.length];st.turnStartedAt=Date.now();st.turnReadyAt=Date.now()+((bots.map(Number).includes(Number(st.turn)))?700:1000);db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(st),roomId);return;}
   p.position={r:opts[0].r,c:opts[0].c};
   if(p.position.r===(p.goal==='top'?0:11)){if(completeAdvancedRound(roomId,st,uid))return;}
  }
  st.turn=order[(order.indexOf(uid)+1)%order.length];st.turnStartedAt=Date.now();st.turnReadyAt=Date.now()+((bots.map(Number).includes(Number(st.turn)))?700:1000);db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(st),roomId);return;
 }
}

function startGameIfReady(roomId,force=false){
 const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(roomId);if(!room||room.status!=='waiting')return room;
 const rules=GAME_RULES[room.game_type],count=db.prepare('SELECT COUNT(*) n FROM game_players WHERE room_id=?').get(roomId).n;
 const botFill=room.game_type==='advanced'&&!room.room_code&&Date.now()>=Number(room.deadline||0)&&count<rules.max;
 if(!(room.room_code?(force&&count>=rules.min):(count>=rules.min||botFill)))return room;
 const humanPs=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(roomId);
 const botIds=botFill?Array.from({length:Math.max(0,rules.max-count)},(_,i)=>-(Number(room.id)*10+i+1)):[];
 const ps=[...humanPs,...botIds.map(user_id=>({user_id}))];
 let gamePlayers={};
 if(room.game_type==='classic')gamePlayers=Object.fromEntries(ps.map((p,i)=>[String(p.user_id),{position:{r:i===0?8:0,c:4},walls:10,goal:i===0?'top':'bottom'}]));
 if(room.game_type==='advanced'){
  const colors=['red','blue','green','yellow'];
  const shuffled=ps.map(p=>p.user_id).sort(()=>Math.random()-.5);const teams=[0,0,1,1];
  const assigned=shuffled.map((uid,i)=>({uid,team:teams[i],color:colors[i]}));
  gamePlayers=Object.fromEntries(assigned.map((p,i)=>[String(p.uid),{position:{r:p.team===0?11:0,c:p.color==='blue'||p.color==='yellow'?7:4},walls:20,goal:p.team===0?'top':'bottom',team:p.team,color:p.color,...(botIds.includes(Number(p.uid))?{name:['ربات حرفه‌ای','ربات استراتژیست','ربات تاکتیکی'][i%3]}:{})}]));
 }
 const initial={turn:ps[0]?.user_id||null,turnOrder:ps.map(p=>Number(p.user_id)),botIds,round:1,roundWins:{0:0,1:0},turnStartedAt:Date.now(),turnReadyAt:room.game_type==='advanced'?Date.now()+((botIds.map(Number).includes(Number(ps[0]?.user_id)))?700:1000):0,missedTurns:{},board:room.game_type==='classic'||room.game_type==='advanced'?{}:room.game_type==='snakes'?{snakes:{16:6,47:26,49:11,56:53,62:19,64:60,87:24,93:73,95:75,98:78},ladders:{1:38,4:14,9:31,21:42,28:84,36:44,51:67,71:91,80:100}}:room.game_type==='connect4'?Array.from({length:6},()=>Array(7).fill(0)): {},walls:[],players:gamePlayers,word:room.game_type==='drawguess'?['سیب','گربه','دوچرخه','خورشید','خانه','ماهی','درخت','هواپیما'][Math.floor(Math.random()*8)]:null,drawer:room.game_type==='drawguess'?ps[0]?.user_id:null,strokes:[],guesses:[],activityAt:room.game_type==='drawguess'?Object.fromEntries(ps.map(p=>[p.user_id,Date.now()])):{},scores:room.game_type==='drawguess'?Object.fromEntries(ps.map(p=>[p.user_id,0])):{},matchEndsAt:room.game_type==='drawguess'?Date.now()+180000:null,roundEndsAt:room.game_type==='drawguess'?Date.now()+80000:null};
 db.prepare("UPDATE game_rooms SET status='playing',started_at=CURRENT_TIMESTAMP WHERE id=?").run(roomId);db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(initial),roomId);
 if(['classic','advanced'].includes(room.game_type))for(const p of humanPs)notify(p.user_id,'بازی شروع شد','بازیکنان آماده‌اند؛ ورود به بازی',{action:'game_start',requestId:roomId});
 return db.prepare('SELECT * FROM game_rooms WHERE id=?').get(roomId);
}
// CONNECT: queue-based matchmaking (no user-created rooms), authoritative server state.
const connectQueue=[]; const connectGames=new Map(); db.exec('CREATE TABLE IF NOT EXISTS connect_games(id INTEGER PRIMARY KEY, state_json TEXT NOT NULL, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'); db.exec('CREATE TABLE IF NOT EXISTS connect_game_rewards(game_id INTEGER PRIMARY KEY,winner_id INTEGER NOT NULL,awarded_at TEXT DEFAULT CURRENT_TIMESTAMP)'); let connectNextId=1; const connectSave=g=>{db.prepare('INSERT INTO connect_games(id,state_json,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json,updated_at=CURRENT_TIMESTAMP').run(g.id,JSON.stringify(g));}; try{for(const row of db.prepare('SELECT state_json FROM connect_games').all()){const g=JSON.parse(row.state_json);if(g&&g.id){connectGames.set(Number(g.id),g);connectNextId=Math.max(connectNextId,Number(g.id)+1);}}}catch(e){console.error('CONNECT restore failed:',e.message);}
const connectNorm=s=>String(s||'').normalize('NFKC').replace(/[\u064a]/g,'ی').replace(/[\u0643]/g,'ک').replace(/[\u064b-\u065f\u0670\u0640]/g,'').replace(/\s+/g,' ').trim().toLowerCase();
const connectPublicGame=(g,uid)=>{const main=Number(g.mainId)===Number(uid);return {id:g.id,status:g.status,startedAt:g.startedAt,endsAt:g.endsAt,queueStartedAt:g.queueStartedAt,players:g.players.map(x=>({id:x.id,name:x.name,avatar:x.avatar,role:x.id===g.mainId?'main':'team',online:!!(io.sockets.adapter.rooms.get('user:'+x.id)?.size)})),meRole:main?'main':'team',prefix:g.prefix,secretLength:g.secret?Array.from(g.secret).length:0,secret:main?g.secret:null,proposal:g.proposal?{clue:g.proposal.clue,authorName:g.players.find(x=>x.id===g.proposal.authorId)?.name||'هم‌تیمی',isMine:Number(g.proposal.authorId)===Number(uid),createdAt:g.proposal.createdAt,wrongAttempts:Number(g.proposal.wrongAttempts||0),maxWrongAttempts:3,guesses:(g.proposal.guesses||[]).map(x=>({name:x.name,answer:x.answer,at:x.at}))}:null,burnedCount:g.burned.length,usedPrefix:g.prefix,finishedAt:g.finishedAt,result:g.result,winnerId:g.winnerId,winnerName:g.winnerName,reason:g.reason,reward:{fame:50000,diamonds:10},myCanPropose:!main&&g.status==='playing'&&!g.proposal,myCanGuessClue:!!g.proposal&&g.status==='playing'&&Number(g.proposal.authorId)!==Number(uid),lastEvent:g.lastEvent};};
function connectFinish(g,winnerId,reason){if(!g||g.status==='finished')return;g.status='finished';g.finishedAt=Date.now();g.winnerId=winnerId||null;g.reason=reason;const winner=g.players.find(p=>p.id===winnerId);g.winnerName=winner?.name||'';g.result=winnerId===g.mainId?'main':winnerId?'team':'none';if(winner){const award=db.prepare('INSERT OR IGNORE INTO connect_game_rewards(game_id,winner_id) VALUES(?,?)').run(g.id,winner.id);if(award.changes){const u=userById(winner.id);syncUserFameAndLevel(winner.id,{fame:Math.max(0,Number(u?.fame||0),Number(u?.xp||0))+50000});db.prepare('UPDATE users SET diamonds=diamonds+10 WHERE id=?').run(winner.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(winner.id,10,'connect_game_win_diamonds','جایزه برد بازی CONNECT');recalcGameLevel(winner.id);notify(winner.id,'پیروزی در CONNECT','برنده شدی؛ ۵۰٬۰۰۰ شهرت و ۱۰ الماس به حسابت اضافه شد.',{action:'connect_game_result',requestId:g.id});}}for(const p of g.players){if(reason&&reason!=='timeout'&&reason!=='direct_secret'&&reason!=='team_guess'){notify(p.id,'بازی CONNECT توسط مدیریت پایان یافت',`دلیل: ${reason}`,{action:'connect_game_result',requestId:g.id});}else if(p.id!==winnerId){notify(p.id,'پایان بازی CONNECT',winner?`برنده: ${winner.name}`:'زمان بازی تمام شد.',{action:'connect_game_result',requestId:g.id});}}g.lastEvent={type:'finished',at:Date.now()};connectSave(g);}
function connectStartGroup(entries){const players=entries.map(e=>{const u=userById(e.userId);return {id:Number(e.userId),name:u?.display_name||u?.username||'بازیکن',avatar:u?.avatar||''}});if(players.length<3)return;const main=players[crypto.randomInt(players.length)];const now=Date.now(),id=connectNextId++;const g={id,status:'word',players,mainId:main.id,secret:null,prefix:'',proposal:null,burned:[],queueStartedAt:Math.min(...entries.map(e=>e.joinedAt)),startedAt:now,endsAt:null,finishedAt:null,result:null,winnerId:null,winnerName:'',reason:'',lastEvent:{type:'started',at:now}};connectGames.set(id,g);connectSave(g);for(const p of players)notify(p.id,'بازی CONNECT شروع شد','مسابقه پیدا شد؛ برای ورود مستقیم به بازی بزن.',{action:'connect_game_start',requestId:id});}
function connectMatchQueue(){const now=Date.now();for(let i=connectQueue.length-1;i>=0;i--){const e=connectQueue[i];if(!userById(e.userId)||now-e.joinedAt>240000)connectQueue.splice(i,1);}while(connectQueue.length>=14){connectStartGroup(connectQueue.splice(0,14));}const oldest=connectQueue[0];if(oldest&&now-oldest.joinedAt>=180000){const group=connectQueue.splice(0,Math.min(14,connectQueue.length));if(group.length>=3)connectStartGroup(group);else connectQueue.unshift(...group);}}
setInterval(connectMatchQueue,1000).unref();
app.post('/api/connect/queue',requireAuth,requireGameAccess,(req,res)=>{const uid=Number(req.user.id);for(const g of connectGames.values())if(g.status!=='finished'&&g.players.some(p=>p.id===uid))return res.json({game:connectPublicGame(g,uid)});const otherActive=db.prepare("SELECT r.id,r.game_type FROM game_rooms r JOIN game_players p ON p.room_id=r.id WHERE p.user_id=? AND r.status IN ('waiting','playing') LIMIT 1").get(uid);if(otherActive)return res.status(409).json({error:'ابتدا بازی فعلی را تمام کن و بعد وارد کانکت شو'});let entry=connectQueue.find(x=>x.userId===uid);if(!entry){entry={userId:uid,joinedAt:Date.now()};connectQueue.push(entry);}connectMatchQueue();const game=[...connectGames.values()].find(g=>g.status!=='finished'&&g.players.some(p=>p.id===uid));res.json(game?{game:connectPublicGame(game,uid)}:{queued:true,position:connectQueue.findIndex(x=>x.userId===uid)+1,queueSize:connectQueue.length,joinedAt:entry.joinedAt,minimumPlayers:3,maximumPlayers:14,waitSeconds:180});});
app.delete('/api/connect/queue',requireAuth,(req,res)=>{const before=connectQueue.length;for(let i=connectQueue.length-1;i>=0;i--)if(connectQueue[i].userId===Number(req.user.id))connectQueue.splice(i,1);res.json({ok:true,removed:before-connectQueue.length});});
app.post('/api/connect/game/:id/leave',requireAuth,requireGameAccess,(req,res)=>{const id=Number(req.params.id),uid=Number(req.user.id),g=connectGames.get(id);if(!g||g.status==='finished'||!g.players.some(p=>Number(p.id)===uid))return res.status(404).json({error:'بازی فعال پیدا نشد'});const leaving=g.players.find(p=>Number(p.id)===uid);g.players=g.players.filter(p=>Number(p.id)!==uid);connectQueue.splice(0,connectQueue.length,...connectQueue.filter(x=>Number(x.userId)!==uid));notify(uid,'از بازی کانکت خارج شدی','خروج از مسابقه ثبت شد.',{action:'connect_game_left',requestId:id});if(Number(g.mainId)===uid||g.players.length<3){connectFinish(g,null,'player_left');}else{g.lastEvent={type:'player_left',userId:uid,playerName:leaving?.name||'بازیکن',at:Date.now()};for(const p of g.players)notify(p.id,'یک بازیکن از کانکت خارج شد',`${leaving?.name||'یک بازیکن'} از مسابقه خارج شد.`,{action:'connect_player_left',requestId:id});connectSave(g);}res.json({ok:true,finished:g.status==='finished',remainingPlayers:g.players.length});});
app.get('/api/connect/game/:id',requireAuth,requireGameAccess,(req,res)=>{const g=connectGames.get(Number(req.params.id));if(!g||!g.players.some(p=>p.id===Number(req.user.id)))return res.status(404).json({error:'بازی پیدا نشد یا دیگر در دسترس نیست'});if(g.status==='playing'&&g.endsAt&&Date.now()>=g.endsAt)connectFinish(g,g.mainId,'timeout');res.json({game:connectPublicGame(g,req.user.id),serverNow:Date.now()});});
app.post('/api/connect/game/:id/secret',requireAuth,requireGameAccess,(req,res)=>{const g=connectGames.get(Number(req.params.id));if(!g||g.mainId!==Number(req.user.id))return res.status(403).json({error:'فقط بازیکن اصلی می‌تواند کلمه را انتخاب کند'});if(g.status!=='word')return res.status(409).json({error:'مرحله انتخاب کلمه تمام شده است'});const word=String(req.body.word||'').normalize('NFKC').replace(/[\u064a]/g,'ی').replace(/[\u0643]/g,'ک').replace(/\s+/g,' ').trim();const letters=Array.from(word);if(letters.length<3||letters.length>30||!/^[\u0600-\u06FF]+(?:\s+[\u0600-\u06FF]+)*$/.test(word))return res.status(400).json({error:'کلمه باید ۳ تا ۳۰ حرف فارسی باشد؛ فاصله بین کلمات مجاز است'});g.secret=word;g.prefix=letters[0];g.status='playing';g.endsAt=Date.now()+300000;g.lastEvent={type:'secret_set',at:Date.now()};connectSave(g);res.json({game:connectPublicGame(g,req.user.id)});});
app.post('/api/connect/game/:id/proposal',requireAuth,requireGameAccess,(req,res)=>{const g=connectGames.get(Number(req.params.id));if(!g||!g.players.some(p=>p.id===Number(req.user.id)))return res.status(404).json({error:'بازی پیدا نشد'});if(g.status!=='playing'||g.mainId===Number(req.user.id))return res.status(403).json({error:'این مرحله برای هم‌تیمی‌هاست'});if(g.proposal)return res.status(409).json({error:'ابتدا راهنمای فعلی باید حل شود'});if(Date.now()>=g.endsAt){connectFinish(g,g.mainId,'timeout');return res.status(409).json({error:'زمان بازی تمام شد'});}const word=String(req.body.word||'').normalize('NFKC').replace(/[\u064a]/g,'ی').replace(/[\u0643]/g,'ک').replace(/\s+/g,' ').trim();const clue=String(req.body.clue||'').trim().slice(0,220);const norm=connectNorm(word);if(!clue||clue.length<4)return res.status(400).json({error:'جمله راهنما را کامل بنویس'});if(!/^[\u0600-\u06FF]+(?:\s+[\u0600-\u06FF]+)*$/.test(word)||Array.from(word).length<2||Array.from(word).length>30)return res.status(400).json({error:'کلمه باید فارسی باشد؛ فاصله بین کلمات مجاز است'});if(!norm.startsWith(connectNorm(g.prefix)))return res.status(400).json({error:`کلمه باید با «${g.prefix}» شروع شود`});if(norm===connectNorm(g.secret)){connectFinish(g,Number(req.user.id),'direct_secret');return res.json({game:connectPublicGame(g,req.user.id),directWin:true});}if(g.burned.includes(norm))return res.status(409).json({error:'این کلمه قبلاً سوخته و قابل استفاده نیست'});g.proposal={word,norm,clue,authorId:Number(req.user.id),createdAt:Date.now(),guesses:[]};g.lastEvent={type:'proposal',at:Date.now()};connectSave(g);res.json({game:connectPublicGame(g,req.user.id)});});
app.post('/api/connect/game/:id/guess-clue',requireAuth,requireGameAccess,(req,res)=>{const g=connectGames.get(Number(req.params.id));if(!g||!g.players.some(p=>p.id===Number(req.user.id)))return res.status(404).json({error:'بازی پیدا نشد'});if(g.status!=='playing'||!g.proposal)return res.status(409).json({error:'راهنمای فعالی وجود ندارد'});if(Number(g.proposal.authorId)===Number(req.user.id))return res.status(403).json({error:'نمی‌توانی کلمه‌ای را که خودت انتخاب کرده‌ای حدس بزنی'});if(Date.now()>=g.endsAt){connectFinish(g,g.mainId,'timeout');return res.status(409).json({error:'زمان بازی تمام شد'});}const rawAnswer=String(req.body.answer||'').normalize('NFKC').replace(/\s+/g,' ').trim();const answer=connectNorm(rawAnswer);if(!answer)return res.status(400).json({error:'حدس خود را وارد کن'});if((g.proposal.guesses||[]).some(x=>connectNorm(x.answer)===answer))return res.status(409).json({error:'این حدس قبلاً ثبت شده؛ کلمه دیگری امتحان کن'});const correct=answer===g.proposal.norm;const main=Number(req.user.id)===g.mainId;let burnedAfterThree=false;if(correct){const old=g.proposal;g.proposal=null;if(main){g.burned.push(old.norm);g.lastEvent={type:'burned',by:'main',at:Date.now(),authorId:old.authorId};}else{const chars=Array.from(g.secret);let n=Array.from(g.prefix).length;while(n<chars.length&&/\s/.test(chars[n]))n++;if(n<chars.length)n++;g.prefix=chars.slice(0,n).join('');g.lastEvent={type:'revealed',by:'team',at:Date.now(),prefix:g.prefix,authorId:old.authorId};}}else{if(!Array.isArray(g.proposal.guesses))g.proposal.guesses=[];g.proposal.guesses.push({userId:Number(req.user.id),name:g.players.find(p=>Number(p.id)===Number(req.user.id))?.name||'بازیکن',answer:rawAnswer,at:Date.now()});g.proposal.wrongAttempts=Number(g.proposal.wrongAttempts||0)+1;if(g.proposal.wrongAttempts>=3){const old=g.proposal;g.burned.push(old.norm);g.proposal=null;g.lastEvent={type:'burned_after_three_misses',by:'team',at:Date.now(),prefix:g.prefix,authorId:old.authorId};burnedAfterThree=true;}}connectSave(g);res.json({correct,game:connectPublicGame(g,req.user.id),message:correct?(main?'درست حدس زدی؛ کلمه راهنما سوخت و حرف تازه‌ای آشکار نشد.':'درست حدس زدی؛ حرف بعدی کلمه اصلی آشکار شد.'):(burnedAfterThree?`سه حدس اشتباه ثبت شد؛ این کلمه راهنما سوخت. حرف جدیدی آشکار نمی‌شود؛ یک هم‌تیمی باید راهنمای تازه‌ای با همان حروف فعلی بسازد.`:`حدس درست نیست؛ ${3-Number(g.proposal?.wrongAttempts||0)} فرصت تا سوختن این کلمه باقی است.`)});});
app.post('/api/connect/game/:id/guess-secret',requireAuth,requireGameAccess,(req,res)=>{const g=connectGames.get(Number(req.params.id));if(!g||!g.players.some(p=>p.id===Number(req.user.id)))return res.status(404).json({error:'بازی پیدا نشد'});if(g.status!=='playing'||g.mainId===Number(req.user.id))return res.status(403).json({error:'فقط هم‌تیمی‌ها می‌توانند کلمه اصلی را حدس بزنند'});if(Date.now()>=g.endsAt){connectFinish(g,g.mainId,'timeout');return res.json({correct:false,game:connectPublicGame(g,req.user.id)});}const answer=connectNorm(req.body.answer);if(answer===connectNorm(g.secret)){connectFinish(g,Number(req.user.id),'team_guess');return res.json({correct:true,game:connectPublicGame(g,req.user.id)});}res.json({correct:false,game:connectPublicGame(g,req.user.id),message:'کلمه اصلی درست نیست؛ دوباره تلاش کن.'});});
app.post('/api/games/join',requireAuth,requireGameAccess,(req,res)=>{
 const type=String(req.body.gameType||'');if(type==='drawguess')return res.status(410).json({error:'بازی نقاشی و حدس در به‌روزرسانی بعدی فعال می‌شود'});const rules=GAME_RULES[type];if(!rules)return res.status(400).json({error:'بازی معتبر نیست'});
 const existing=db.prepare("SELECT r.* FROM game_rooms r JOIN game_players p ON p.room_id=r.id WHERE p.user_id=? AND r.status IN ('waiting','playing') ORDER BY r.id DESC LIMIT 1").get(req.user.id);if(existing){if(existing.game_type!==type)return res.status(409).json({error:'ابتدا از بازی فعلی خارج شو، سپس بازی دیگری انتخاب کن'});return res.json({room:gameRoomState(existing.id,req.user.id)});}
 let room=db.prepare("SELECT * FROM game_rooms WHERE game_type=? AND status='waiting' AND room_code IS NULL ORDER BY id ASC LIMIT 1").get(type);
 if(!room){const deadline=Date.now()+60000;const r=db.prepare("INSERT INTO game_rooms(game_type,status,created_by,deadline,state_json) VALUES(?,'waiting',?,?, '{}')").run(type,req.user.id,deadline);db.prepare("UPDATE game_rooms SET game_code=? WHERE id=?").run('GAME-'+String(r.lastInsertRowid).padStart(8,'0'),r.lastInsertRowid);room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(r.lastInsertRowid);}
 const count=db.prepare('SELECT COUNT(*) n FROM game_players WHERE room_id=?').get(room.id).n;if(count>=rules.max)return res.status(409).json({error:'این لابی پر شده است؛ دوباره تلاش کن'});
 db.prepare('INSERT OR IGNORE INTO game_players(room_id,user_id,position) VALUES(?,?,0)').run(room.id,req.user.id);
 room=startGameIfReady(room.id,false)||room;
 res.json({room:gameRoomState(room.id,req.user.id)});
});
function makeRoomCode(){let code;do{code=String(crypto.randomInt(100000,1000000));}while(db.prepare('SELECT id FROM game_rooms WHERE room_code=? AND status IN (\'waiting\',\'playing\')').get(code));return code;}
app.post('/api/games/private',requireAuth,requireGameAccess,(req,res)=>{
 const type=String(req.body.gameType||'');if(type==='drawguess')return res.status(410).json({error:'بازی نقاشی و حدس در به‌روزرسانی بعدی فعال می‌شود'});if(type==='ludo')return res.status(400).json({error:'بازی منچ از برنامه حذف شده است'});if(!GAME_RULES[type])return res.status(400).json({error:'نوع بازی خصوصی معتبر نیست'});
 const active=db.prepare("SELECT r.id FROM game_rooms r JOIN game_players p ON p.room_id=r.id WHERE p.user_id=? AND r.status IN ('waiting','playing')").get(req.user.id);if(active)return res.status(409).json({error:'اول بازی فعلی‌ات را تمام کن'});
 const code=makeRoomCode(),deadline=Date.now()+86400000;const ins=db.prepare("INSERT INTO game_rooms(game_type,status,created_by,deadline,state_json,room_code) VALUES(?,'waiting',?,?,'{}',?)").run(type,req.user.id,deadline,code);db.prepare('INSERT INTO game_players(room_id,user_id,position) VALUES(?,?,0)').run(ins.lastInsertRowid,req.user.id);res.json({room:gameRoomState(ins.lastInsertRowid,req.user.id)});
});
app.post('/api/games/private/join',requireAuth,requireGameAccess,(req,res)=>{
 const code=String(req.body.code||'').trim().replace(/[۰-۹]/g,d=>String('۰۱۲۳۴۵۶۷۸۹'.indexOf(d))).replace(/[٠-٩]/g,d=>String('٠١٢٣٤٥٦٧٨٩'.indexOf(d)));if(!/^\d{6}$/.test(code))return res.status(400).json({error:'کد اتاق باید دقیقاً ۶ رقم باشد'});const room=db.prepare("SELECT * FROM game_rooms WHERE room_code=? AND status='waiting'").get(code);if(!room)return res.status(404).json({error:'اتاق با این کد پیدا نشد یا بازی شروع شده است'});if(room.game_type==='drawguess')return res.status(410).json({error:'بازی نقاشی و حدس در به‌روزرسانی بعدی فعال می‌شود'});
 const active=db.prepare("SELECT r.id FROM game_rooms r JOIN game_players p ON p.room_id=r.id WHERE p.user_id=? AND r.status IN ('waiting','playing')").get(req.user.id);if(active)return res.status(409).json({error:'اول بازی فعلی‌ات را تمام کن'});
 const rules=GAME_RULES[room.game_type],count=db.prepare('SELECT COUNT(*) n FROM game_players WHERE room_id=?').get(room.id).n;if(count>=rules.max)return res.status(409).json({error:'ظرفیت اتاق پر شده است'});db.prepare('INSERT OR IGNORE INTO game_players(room_id,user_id,position) VALUES(?,?,0)').run(room.id,req.user.id);// اتاق خصوصی تا زمانی که سازنده شروع نکند در انتظار می‌ماند.
 res.json({room:gameRoomState(room.id,req.user.id)});
});
app.post('/api/games/room/:id/start',requireAuth,requireGameAccess,(req,res)=>{const id=Number(req.params.id);const room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(!room||!room.room_code)return res.status(404).json({error:'اتاق خصوصی پیدا نشد'});if(room.created_by!==req.user.id)return res.status(403).json({error:'فقط سازنده اتاق می‌تواند بازی را شروع کند'});if(room.status!=='waiting')return res.status(409).json({error:'این اتاق قبلاً شروع شده است'});const count=db.prepare('SELECT COUNT(*) n FROM game_players WHERE room_id=?').get(id).n;if(count<GAME_RULES[room.game_type].min)return res.status(409).json({error:'برای شروع، بازیکنان کافی نیستند'});startGameIfReady(id,true);res.json({room:gameRoomState(id,req.user.id)});});
app.post('/api/games/room/:id/lobby-message',requireAuth,requireGameAccess,(req,res)=>{const id=Number(req.params.id),room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);if(!room||!room.room_code||room.status!=='waiting')return res.status(404).json({error:'اتاق انتظار در دسترس نیست'});if(!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو این اتاق نیستی'});const body=String(req.body.body||'').trim().slice(0,500);if(!body)return res.status(400).json({error:'پیام خالی است'});const state=JSON.parse(room.state_json||'{}');state.lobbyMessages=Array.isArray(state.lobbyMessages)?state.lobbyMessages:[];state.lobbyMessages.push({userId:req.user.id,name:userById(req.user.id)?.display_name||'بازیکن',body,at:Date.now()});state.lobbyMessages=state.lobbyMessages.slice(-80);db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),id);res.json({ok:true,room:gameRoomState(id,req.user.id)});});
function processGameTurnTimeout(roomId){
 let room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(Number(roomId));
 if(!room||room.status!=='playing')return;
 let state=JSON.parse(room.state_json||'{}');
 const players=db.prepare('SELECT user_id,choice FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(room.id);
 if(!players.length)return;
 const now=Date.now(),misses=state.missedTurns||{};
 if(room.game_type==='advanced'){
  // Bots must be processed before timeout logic. Bot IDs are intentionally not
  // rows in game_players, so the generic timeout code would otherwise reset a
  // bot turn to the first human and make the turn indicator appear stuck.
  runAdvancedBots(room.id);
  room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(room.id);
  if(!room||room.status!=='playing')return;
  state=JSON.parse(room.state_json||'{}');
  const botSet=new Set((state.botIds||[]).map(Number));
  const current=Number(state.turn);
  if(botSet.has(current))return;
  if(!state.turnStartedAt||now-Number(state.turnStartedAt)<15000)return;
  const order=(state.turnOrder||players.map(p=>p.user_id)).map(Number);
  const ix=order.indexOf(current);
  if(ix<0){state.turn=players[0]?.user_id||null;state.turnStartedAt=now;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);runAdvancedBots(room.id);return;}
  const next=order[(ix+1)%order.length];
  if(next===undefined||next===null)return;
  state.turn=next;state.turnStartedAt=now;state.turnReadyAt=now+((botSet.has(Number(next)))?700:1000);state.lastTimeout={userId:current};
  db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);
  if(current>0)notify(current,'مهلت حرکت تمام شد','حرکتی انجام ندادی؛ نوبت به حریف رسید.');
  if(next>0)notify(next,'نوبت تو شد','نوبت توست؛ ۱۵ ثانیه فرصت داری.');
  runAdvancedBots(room.id);return;
 }
 if(room.game_type==='classic'){
  if(!state.turnStartedAt||now-Number(state.turnStartedAt)<15000)return;
  // Classic games use the same bot metadata as advanced games. Define the set
  // in this branch too; the previous code referenced a block-scoped variable
  // that only exists inside the advanced branch, crashing every classic timeout.
  const botSet=new Set((state.botIds||[]).map(Number));
  const order=(state.turnOrder||players.map(p=>Number(p.user_id))).map(Number);
  const current=Number(state.turn),ix=order.indexOf(current);
  if(ix<0){state.turn=order[0]??null;state.turnStartedAt=now;state.turnReadyAt=now+1000;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);return;}
  const next=order[(ix+1)%order.length];
  if(next===undefined||next===null)return;
  state.turn=next;state.turnStartedAt=now;state.turnReadyAt=now+(botSet.has(Number(next))?700:1000);state.lastTimeout={userId:current};db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);
  if(current>0)notify(current,'مهلت حرکت تمام شد','حرکتی انجام ندادی؛ نوبت به حریف رسید.');if(next>0)notify(next,'نوبت تو شد','نوبت توست؛ ۱۵ ثانیه فرصت داری.');return;
 }
 if(room.game_type==='drawguess'){
  const activity=state.activityAt||{};let changed=false;
  for(const p of players){const uid=Number(p.user_id);if(!activity[uid])activity[uid]=now;if(now-Number(activity[uid])<30000)continue;
   misses[uid]=(Number(misses[uid])||0)+1;activity[uid]=now;changed=true;
   if(misses[uid]>=3){db.prepare('DELETE FROM game_players WHERE room_id=? AND user_id=?').run(room.id,uid);delete misses[uid];notify(uid,'اخراج از بازی','سه بار در مهلت ۳۰ ثانیه‌ای فعالیت نکردی و از بازی اخراج شدی.');}
   else notify(uid,'مهلت فعالیت بازی',`۳۰ ثانیه فعالیتی نداشتی؛ اخطار ${misses[uid]} از ۳.`);
  }
  if(changed){const remaining=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(room.id);if(remaining.length<=1){finishGame(room.id,remaining[0]?.user_id||null);return;}if(!remaining.some(p=>Number(p.user_id)===Number(state.drawer))){state.drawer=remaining[0].user_id;state.word=['سیب','گربه','دوچرخه','خورشید','خانه','ماهی','درخت','هواپیما','کتاب','کفش','تلفن','کوه'][Math.floor(Math.random()*12)];state.strokes=[];state.guesses=[];state.roundEndsAt=now+80000;}state.activityAt=activity;state.missedTurns=misses;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);}
  return;
 }
 if(!state.turnStartedAt||now-Number(state.turnStartedAt)<30000)return;
 if(room.game_type==='rps'){
  let changed=false;
  for(const p of players){if(p.choice)continue;const uid=Number(p.user_id);misses[uid]=(Number(misses[uid])||0)+1;changed=true;if(misses[uid]>=3){db.prepare('DELETE FROM game_players WHERE room_id=? AND user_id=?').run(room.id,uid);delete misses[uid];notify(uid,'اخراج از بازی','سه بار در مهلت ۳۰ ثانیه‌ای انتخاب نکردی و از بازی اخراج شدی.');}else notify(uid,'مهلت انتخاب تمام شد',`انتخابی نزدی؛ اخطار ${misses[uid]} از ۳.`);}
  if(!changed)return;
  const remaining=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(room.id);if(remaining.length<=1){finishGame(room.id,remaining[0]?.user_id||null);return;}
  db.prepare('UPDATE game_players SET choice=NULL WHERE room_id=?').run(room.id);state.missedTurns=misses;state.turnStartedAt=now;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);return;
 }
 const current=Number(state.turn),idx=players.findIndex(p=>Number(p.user_id)===current);if(idx<0){state.turn=players[0].user_id;state.turnStartedAt=now;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);return;}
 const count=(Number(misses[current])||0)+1;misses[current]=count;
 if(count>=3){db.prepare('DELETE FROM game_players WHERE room_id=? AND user_id=?').run(room.id,current);delete misses[current];const remaining=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(room.id);if(remaining.length<=1){finishGame(room.id,remaining[0]?.user_id||null);return;}state.turn=remaining.find(p=>players.findIndex(x=>Number(x.user_id)===Number(p.user_id))>idx)?.user_id||remaining[0].user_id;}
 else state.turn=players[(idx+1)%players.length]?.user_id||current;
 state.missedTurns=misses;state.turnStartedAt=now;state.lastTimeout={userId:current,strikes:count,removed:count>=3};db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),room.id);
 if(count<3)notify(current,'مهلت حرکت تمام شد',`حرکتی نزدی؛ اخطار ${count} از ۳؛ نوبت به بازیکن بعدی رسید.`);
}
app.get('/api/games/room/:id',requireAuth,(req,res)=>{let room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(Number(req.params.id));if(!room)return res.status(404).json({error:'لابی پیدا نشد'});if(!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(room.id,req.user.id))return res.status(403).json({error:'عضو این لابی نیستی'});processGameTurnTimeout(room.id);room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(room.id);if(room.status==='waiting'&&Date.now()>=Number(room.deadline)){const count=db.prepare('SELECT COUNT(*) n FROM game_players WHERE room_id=?').get(room.id).n;if(!room.room_code&&count<GAME_RULES[room.game_type].min&&room.game_type!=='advanced'){db.prepare("UPDATE game_rooms SET status='finished',finished_at=CURRENT_TIMESTAMP,state_json=? WHERE id=?").run(JSON.stringify({noOpponent:true}),room.id);room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(room.id);}else room=startGameIfReady(room.id,true)||room;}if(room.status==='playing'&&room.game_type==='advanced')runAdvancedBots(room.id);room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(room.id);if(room.status==='playing'&&room.game_type==='drawguess'){const st=JSON.parse(room.state_json||'{}');if(st.matchEndsAt&&Date.now()>=Number(st.matchEndsAt)){const scores=st.scores||{};const top=Object.entries(scores).sort((a,b)=>Number(b[1])-Number(a[1]));const winner=top.length&&Number(top[0][1])>0?Number(top[0][0]):null;finishGame(room.id,winner);room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(room.id);}else if(st.roundEndsAt&&Date.now()>=Number(st.roundEndsAt)){const ps=db.prepare('SELECT user_id FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(room.id);const ix=ps.findIndex(p=>p.user_id===Number(st.drawer));st.drawer=ps[(ix+1)%ps.length]?.user_id||st.drawer;st.word=['سیب','گربه','دوچرخه','خورشید','خانه','ماهی','درخت','هواپیما','کتاب','کفش','تلفن','کوه'][Math.floor(Math.random()*12)];st.strokes=[];st.guesses=[];st.activityAt=Object.fromEntries(ps.map(p=>[p.user_id,Date.now()]));st.round=(st.round||1)+1;st.winnerThisRound=null;st.roundEndsAt=Date.now()+80000;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(st),room.id);}}res.json({room:gameRoomState(room.id,req.user.id)});});
app.post('/api/games/room/:id/report',requireAuth,requireGameAccess,(req,res)=>{
 const id=Number(req.params.id),room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(id);
 if(!room||!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو این بازی نیستی'});
 const kind=String(req.body.kind||'').trim();
 const allowed={'gameplay':'گزارش نحوه بازی','inappropriate':'پیام نامناسب در بازی','cheating':'تقلب در بازی'};
 if(!allowed[kind])return res.status(400).json({error:'نوع گزارش را انتخاب کن'});
 const details=String(req.body.details||'').trim().slice(0,1000);
 const dupe=db.prepare("SELECT id FROM reports WHERE reporter_id=? AND target_type='game' AND target_id=? AND category='game' AND reason=? AND status IN ('new','reviewing')").get(req.user.id,String(id),allowed[kind]);
 if(dupe)return res.status(409).json({error:'این گزارش را قبلاً ثبت کرده‌ای'});
 db.prepare("INSERT INTO reports(reporter_id,reported_user_id,category,target_type,target_id,reason,details) VALUES(?,?, 'game','game',?,?,?)").run(req.user.id,null,String(id),allowed[kind],details||('گزارش بازی '+(room.game_code||('GAME-'+String(id).padStart(8,'0')))));
 res.json({ok:true,message:'گزارش بازی ثبت شد'});
});
app.get('/api/games/room/:id/messages',requireAuth,requireGameAccess,(req,res)=>{const id=Number(req.params.id);if(!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو این بازی نیستی'});const messages=db.prepare('SELECT gm.id,gm.room_id,gm.user_id,gm.body,gm.created_at,u.display_name,u.username,u.avatar FROM game_messages gm JOIN users u ON u.id=gm.user_id WHERE gm.room_id=? ORDER BY gm.id DESC LIMIT 80').all(id).reverse();res.json({messages});});
app.post('/api/games/room/:id/messages',requireAuth,requireGameAccess,(req,res)=>{const id=Number(req.params.id),room=db.prepare('SELECT status FROM game_rooms WHERE id=?').get(id);if(!room||!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو این بازی نیستی'});const body=String(req.body.body||'').trim().slice(0,300);if(!body)return res.status(400).json({error:'پیام خالی است'});const r=db.prepare('INSERT INTO game_messages(room_id,user_id,body) VALUES(?,?,?)').run(id,req.user.id,body);const msg=db.prepare('SELECT gm.id,gm.room_id,gm.user_id,gm.body,gm.created_at,u.display_name,u.username,u.avatar FROM game_messages gm JOIN users u ON u.id=gm.user_id WHERE gm.id=?').get(r.lastInsertRowid);io.to('game:'+id).emit('game:chat',msg);res.json({ok:true,message:msg});});

app.post('/api/games/room/:id/action',requireAuth,requireGameAccess,(req,res)=>{
 const roomId=Number(req.params.id);let room=db.prepare('SELECT * FROM game_rooms WHERE id=?').get(roomId);if(!room)return res.status(404).json({error:'لابی پیدا نشد'});if(!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(roomId,req.user.id))return res.status(403).json({error:'عضو این بازی نیستی'});
 if(room.status==='waiting'&&Date.now()>=Number(room.deadline))room=startGameIfReady(roomId,true)||room;if(room.status!=='playing')return res.status(409).json({error:'بازی هنوز شروع نشده؛ منتظر بازیکنان بمان'});
 const rules=GAME_RULES[room.game_type],state=JSON.parse(room.state_json||'{}');const players=db.prepare('SELECT * FROM game_players WHERE room_id=? ORDER BY joined_at,user_id').all(roomId);if(!players.length)return res.status(409).json({error:'بازیکنی در بازی نیست'});
 if(room.game_type==='advanced'){
  if(Number(state.turn)!==req.user.id)return res.status(409).json({error:'هنوز نوبت تو نیست'});
  if(Number(state.turnReadyAt||0)>Date.now())return res.status(409).json({error:'چند لحظه صبر کن تا نوبتت آماده شود'});
  const cp=state.players?.[String(req.user.id)];if(!cp)return res.status(409).json({error:'وضعیت بازیکن پیدا نشد'});
  const ordered=(state.turnOrder||players.map(p=>p.user_id)).map(Number),walls=Array.isArray(state.walls)?state.walls:[],N=12;
  const blocked=(r1,c1,r2,c2,ws=walls)=>{const dr=r2-r1,dc=c2-c1,h=(r,c)=>ws.some(x=>x.o==='h'&&+x.r===r&&+x.c===c),v=(r,c)=>ws.some(x=>x.o==='v'&&+x.r===r&&+x.c===c);if(dr===-1)return h(r2,c2)||h(r2,c2-1);if(dr===1)return h(r1,c1)||h(r1,c1-1);if(dc===-1)return v(r2,c2)||v(r2-1,c2);if(dc===1)return v(r1,c1)||v(r1-1,c1);return true;};
  const occupied=(r,c)=>Object.entries(state.players||{}).find(([uid,x])=>uid!==String(req.user.id)&&+x.position.r===r&&+x.position.c===c);
  const action=String(req.body.action||'');
  if(action==='move'){
   const nr=Number(req.body.row),nc=Number(req.body.col),r=+cp.position.r,c=+cp.position.c;
   if(!Number.isInteger(nr)||!Number.isInteger(nc)||nr<0||nr>=N||nc<0||nc>=N)return res.status(400).json({error:'خانه نامعتبر است'});
   const dr=nr-r,dc=nc-c;let legal=false;
   if(Math.abs(dr)+Math.abs(dc)===1&&!blocked(r,c,nr,nc)&&!occupied(nr,nc))legal=true;
   // In advanced mode a player may jump over a teammate, but never over an opponent.
   if(!legal&&Math.abs(dr)+Math.abs(dc)===2&&(dr===0||dc===0)){
    const mr=r+Math.sign(dr),mc=c+Math.sign(dc),mid=occupied(mr,mc);
    if(mid&&Number(state.players[mid[0]].team)===Number(cp.team)&&!blocked(r,c,mr,mc)&&!blocked(mr,mc,nr,nc)&&!occupied(nr,nc))legal=true;
   }
   if(!legal)return res.status(409).json({error:'این حرکت مجاز نیست؛ از روی حریف نمی‌توانی بپری'});
   cp.position={r:nr,c:nc};
   const goalReached=cp.goal==='top'?nr===0:nr===N-1;
   if(goalReached){completeAdvancedRound(roomId,state,Number(req.user.id));return res.json({room:gameRoomState(roomId,req.user.id)});}
   const ix=ordered.indexOf(req.user.id);state.turn=ordered[(ix+1)%ordered.length];state.turnStartedAt=Date.now();state.turnReadyAt=Date.now()+(((state.botIds||[]).map(Number).includes(Number(state.turn)))?700:1000);
  }else if(action==='wall'){
   if(Number(cp.walls)<=0)return res.status(409).json({error:'دیوار باقی نمانده است'});const o=String(req.body.orientation||''),r=Number(req.body.row),c=Number(req.body.col);
   if(!['h','v'].includes(o)||!Number.isInteger(r)||!Number.isInteger(c)||r<0||r>N-2||c<0||c>N-2)return res.status(400).json({error:'جای دیوار نامعتبر است'});
   if(walls.some(x=>x.o===o&&+x.r===r&&+x.c===c)||(o==='h'&&(walls.some(x=>x.o==='h'&&+x.r===r&&Math.abs(+x.c-c)===1)||walls.some(x=>x.o==='v'&&+x.r===r&&+x.c===c)))||(o==='v'&&(walls.some(x=>x.o==='v'&&+x.c===c&&Math.abs(+x.r-r)===1)||walls.some(x=>x.o==='h'&&+x.r===r&&+x.c===c))))return res.status(409).json({error:'این دیوار با دیوار دیگری تداخل دارد'});
   const test=[...walls,{o,r,c}];const canReach=(pp)=>{const q=[[+pp.position.r,+pp.position.c]],seen=new Set([q[0].join(',')]);while(q.length){const [a,b]=q.shift();if(pp.goal==='top'?a===0:a===N-1)return true;for(const [dr,dc] of [[1,0],[-1,0],[0,1],[0,-1]]){const x=a+dr,y=b+dc,k=x+','+y;if(x<0||y<0||x>=N||y>=N||blocked(a,b,x,y,test)||seen.has(k))continue;seen.add(k);q.push([x,y]);}}return false;};
   if(!Object.values(state.players).every(canReach))return res.status(409).json({error:'این دیوار همه مسیرهای رسیدن به هدف را می‌بندد'});
   const remaining=Math.max(0,Number(cp.walls)-1);for(const teammate of Object.values(state.players||{}))if(Number(teammate.team)===Number(cp.team))teammate.walls=remaining;state.walls=test;const ix=ordered.indexOf(req.user.id);state.turn=ordered[(ix+1)%ordered.length];state.turnStartedAt=Date.now();state.turnReadyAt=Date.now()+(((state.botIds||[]).map(Number).includes(Number(state.turn)))?700:1000);
  }else return res.status(400).json({error:'حرکت نامعتبر است'});
  db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);runAdvancedBots(roomId);return res.json({room:gameRoomState(roomId,req.user.id)});
 }
 if(room.game_type==='classic'){
  if(Number(state.turn)!==req.user.id)return res.status(409).json({error:'هنوز نوبت تو نیست'});
  if(Number(state.turnReadyAt||0)>Date.now())return res.status(409).json({error:'چند لحظه صبر کن تا نوبتت آماده شود'});
  const cp=state.players?.[String(req.user.id)];
  if(!cp)return res.status(409).json({error:'وضعیت بازیکن پیدا نشد'});
  const otherId=players.find(p=>Number(p.user_id)!==Number(req.user.id))?.user_id;
  const op=otherId?state.players?.[String(otherId)]:null;
  const walls=Array.isArray(state.walls)?state.walls:[];
  const has=(o,r,c)=>walls.some(x=>x.o===o&&Number(x.r)===Number(r)&&Number(x.c)===Number(c));
  const blocked=(r1,c1,r2,c2,wallSet=walls)=>{
    const dr=r2-r1,dc=c2-c1;
    const h=(r,c)=>wallSet.some(x=>x.o==='h'&&Number(x.r)===r&&Number(x.c)===c);
    const v=(r,c)=>wallSet.some(x=>x.o==='v'&&Number(x.r)===r&&Number(x.c)===c);
    if(dr===-1)return h(r2,c2)||h(r2,c2-1);
    if(dr===1)return h(r1,c1)||h(r1,c1-1);
    if(dc===-1)return v(r2,c2)||v(r2-1,c2);
    if(dc===1)return v(r1,c1)||v(r1-1,c1);
    return true;
  };
  const same=(a,r,c)=>a&&Number(a.position?.r)===r&&Number(a.position?.c)===c;
  const reachable=(pp,goal,wallSet)=>{
    const start=[Number(pp.position.r),Number(pp.position.c)],q=[start],seen=new Set([start.join(',')]);
    while(q.length){const [r,c]=q.shift();if(r===goal)return true;for(const [dr,dc] of [[-1,0],[1,0],[0,-1],[0,1]]){const nr=r+dr,nc=c+dc;if(nr<0||nr>8||nc<0||nc>8||blocked(r,c,nr,nc,wallSet))continue;const k=`${nr},${nc}`;if(!seen.has(k)){seen.add(k);q.push([nr,nc]);}}}
    return false;
  };
  const action=String(req.body.action||'');
  if(action==='move'){
    const nr=Number(req.body.row),nc=Number(req.body.col),r=Number(cp.position.r),c=Number(cp.position.c);
    if(!Number.isInteger(nr)||!Number.isInteger(nc)||nr<0||nr>8||nc<0||nc>8)return res.status(400).json({error:'خانه نامعتبر است'});
    if(same(cp,nr,nc)||same(op,nr,nc))return res.status(409).json({error:'این خانه قابل انتخاب نیست'});
    let legal=false;
    const dr=nr-r,dc=nc-c;
    if(Math.abs(dr)+Math.abs(dc)===1&&!blocked(r,c,nr,nc))legal=true;
    if(!legal&&op){
      const or=Number(op.position.r),oc=Number(op.position.c);
      if(Math.abs(or-r)+Math.abs(oc-c)===1&&!blocked(r,c,or,oc)){
        const jr=or+(or-r),jc=oc+(oc-c);
        if(jr>=0&&jr<9&&jc>=0&&jc<9&&!blocked(or,oc,jr,jc)&&nr===jr&&nc===jc)legal=true;
        if(!legal){
          const sides=[[-(oc-c),or-r],[oc-c,-(or-r)]];
          for(const [sr,sc] of sides){const jr2=or+sr,jc2=oc+sc;if(jr2>=0&&jr2<9&&jc2>=0&&jc2<9&&!blocked(or,oc,jr2,jc2)&&nr===jr2&&nc===jc2){legal=true;break;}}
        }
      }
    }
    if(!legal)return res.status(409).json({error:'این حرکت مجاز نیست'});
    cp.position={r:nr,c:nc};state.turn=otherId;state.turnStartedAt=Date.now();state.turnReadyAt=0;
    if((cp.goal==='top'&&nr===0)||(cp.goal==='bottom'&&nr===8)){
      db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);finishGame(roomId,req.user.id);return res.json({room:gameRoomState(roomId,req.user.id)});
    }
    db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);
    notify(otherId,'نوبت تو شد','نوبت توست؛ ۱۵ ثانیه فرصت داری.');
  }else if(action==='wall'){
    if(Number(cp.walls)<=0)return res.status(409).json({error:'دیوار باقی نمانده است'});
    const o=String(req.body.orientation||''),r=Number(req.body.row),c=Number(req.body.col);
    if(!['h','v'].includes(o)||!Number.isInteger(r)||!Number.isInteger(c)||r<0||r>7||c<0||c>7)return res.status(400).json({error:'جای دیوار نامعتبر است'});
    if(has(o,r,c))return res.status(409).json({error:'اینجا قبلاً دیوار گذاشته شده است'});
    if((o==='h'&&(has('h',r,c-1)||has('h',r,c+1)||has('v',r,c)))||(o==='v'&&(has('v',r-1,c)||has('v',r+1,c)||has('h',r,c))))return res.status(409).json({error:'این دیوار با دیوار دیگری تداخل دارد'});
    const test=[...walls,{o,r,c}];
    const ps=Object.values(state.players||{});
    if(!ps.every(pp=>reachable(pp,pp.goal==='top'?0:8,test)))return res.status(409).json({error:'این دیوار همه مسیرهای رسیدن به هدف را می‌بندد'});
    cp.walls=Number(cp.walls)-1;state.walls=test;state.turn=otherId;state.turnStartedAt=Date.now();state.turnReadyAt=0;
    db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);notify(otherId,'نوبت تو شد','نوبت توست؛ ۱۵ ثانیه فرصت داری.');
  }else return res.status(400).json({error:'حرکت نامعتبر است'});
 } else if(room.game_type==='connect4'){
  const col=Number(req.body.column);if(!Number.isInteger(col)||col<0||col>6)return res.status(400).json({error:'ستون معتبر نیست'});if(Number(state.turn)!==req.user.id)return res.status(409).json({error:'هنوز نوبت تو نیست'});
  const board=Array.isArray(state.board)?state.board:Array.from({length:6},()=>Array(7).fill(0));let row=-1;for(let rr=5;rr>=0;rr--)if(!board[rr][col]){row=rr;break;}if(row<0)return res.status(409).json({error:'این ستون پر است'});
  const mark=players.findIndex(p=>p.user_id===req.user.id)+1;board[row][col]=mark;
  const dirs=[[0,1],[1,0],[1,1],[1,-1]];let won=false;for(const [dr,dc] of dirs){let n=1;for(const sign of [-1,1]){let rr=row+dr*sign,cc=col+dc*sign;while(rr>=0&&rr<6&&cc>=0&&cc<7&&board[rr][cc]===mark){n++;rr+=dr*sign;cc+=dc*sign;}}if(n>=4)won=true;}
  if(won){db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify({...state,board,lastMove:{row,column:col,userId:req.user.id}}),roomId);finishGame(roomId,req.user.id);}else if(board.every(row=>row.every(Boolean))){db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify({...state,board,draw:true}),roomId);finishGame(roomId,null);}else{const next=players[(players.findIndex(p=>p.user_id===req.user.id)+1)%players.length].user_id;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify({...state,board,turn:next,turnStartedAt:Date.now(),lastMove:{row,column:col,userId:req.user.id}}),roomId);}
 } else if(room.game_type==='drawguess'){
  const now=Date.now();if(state.roundEndsAt&&now>state.roundEndsAt){state.roundEndsAt=now+80000;state.drawer=players[(players.findIndex(p=>p.user_id===Number(state.drawer))+1)%players.length]?.user_id;state.word=['سیب','گربه','دوچرخه','خورشید','خانه','ماهی','درخت','هواپیما','کتاب','کفش'][Math.floor(Math.random()*10)];state.strokes=[];state.guesses=[];state.round=(state.round||1)+1;}
  if(req.body.action==='stroke'){
   if(Number(state.drawer)!==req.user.id)return res.status(403).json({error:'فقط نقاش می‌تواند نقاشی بکشد'});const stroke=req.body.stroke;if(!stroke||!Array.isArray(stroke.points)||stroke.points.length>100)return res.status(400).json({error:'خط نقاشی معتبر نیست'});state.strokes=(state.strokes||[]).slice(-250);state.strokes.push({color:String(stroke.color||'#ef4444').slice(0,20),size:Math.min(16,Math.max(1,Number(stroke.size)||4)),points:stroke.points.slice(0,100).map(pt=>({x:Math.max(0,Math.min(1,Number(pt.x)||0)),y:Math.max(0,Math.min(1,Number(pt.y)||0))}))});
  } else if(req.body.action==='guess'){
   const guess=String(req.body.guess||'').trim().slice(0,80);if(!guess)return res.status(400).json({error:'حدست را بنویس'});if(req.user.id===Number(state.drawer))return res.status(403).json({error:'نقاش نمی‌تواند حدس بزند'});if(guess.replace(/\s/g,'')===String(state.word||'').replace(/\s/g,'')){const elapsed=Math.max(0,80-Math.floor((Number(state.roundEndsAt)-now)/1000));const points=Math.max(10,100-elapsed);state.guesses.push({userId:req.user.id,name:userById(req.user.id)?.display_name,text:'درست حدس زد! 🎉',points,correct:true});state.scores=state.scores||{};state.scores[req.user.id]=(Number(state.scores[req.user.id])||0)+points;if(!room.room_code){syncUserFameAndLevel(req.user.id,{fame:Math.max(0,Number(userById(req.user.id)?.fame||0),Number(userById(req.user.id)?.xp||0))+points});recalcGameLevel(req.user.id);}state.roundEndsAt=now;state.winnerThisRound=req.user.id;}else{state.guesses=(state.guesses||[]).slice(-50);state.guesses.push({userId:req.user.id,name:userById(req.user.id)?.display_name,text:guess,at:now});}
  }else if(req.body.action==='clear'){if(Number(state.drawer)!==req.user.id)return res.status(403).json({error:'فقط نقاش می‌تواند پاک کند'});state.strokes=[];}else return res.status(400).json({error:'حرکت نامعتبر است'});
  if(room.game_type==='drawguess'){state.activityAt=state.activityAt||{};state.activityAt[req.user.id]=Date.now();}db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);
 } else if(room.game_type==='rps'){
  const choice=String(req.body.choice||'');if(!['سنگ','کاغذ','قیچی'].includes(choice))return res.status(400).json({error:'سنگ، کاغذ یا قیچی را انتخاب کن'});const prior=db.prepare('SELECT choice FROM game_players WHERE room_id=? AND user_id=?').get(roomId,req.user.id);if(prior?.choice)return res.status(409).json({error:'انتخابت ثبت شده؛ تا پایان دور صبر کن'});db.prepare('UPDATE game_players SET choice=? WHERE room_id=? AND user_id=?').run(choice,roomId,req.user.id);
  const refreshed=db.prepare('SELECT * FROM game_players WHERE room_id=?').all(roomId);if(!(refreshed.length===2&&refreshed.every(p=>p.choice))){state.turnStartedAt=Date.now();db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify(state),roomId);}if(refreshed.length===2&&refreshed.every(p=>p.choice)){const a=refreshed[0],b=refreshed[1];let winner=null;if(a.choice!==b.choice){winner=({'سنگ':'قیچی','کاغذ':'سنگ','قیچی':'کاغذ'}[a.choice]===b.choice)?a.user_id:b.user_id;}if(winner)finishGame(roomId,winner);else{db.prepare('UPDATE game_players SET choice=NULL WHERE room_id=?').run(roomId);db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify({...state,round:(state.round||1)+1,turnStartedAt:Date.now()}),roomId);}}
 } else {
  if(Number(state.turn)!==req.user.id)return res.status(409).json({error:'هنوز نوبت تو نیست'});const roll=1+Math.floor(Math.random()*6),p=db.prepare('SELECT * FROM game_players WHERE room_id=? AND user_id=?').get(roomId,req.user.id);let pos=Number(p.position);if(room.game_type==='ludo'){if(pos===0){if(roll===6)pos=1;}else if(pos+roll<=rules.target)pos+=roll;}else{if(pos+roll<=rules.target)pos+=roll;const b=state.board||{},jump=(b.ladders||{})[pos]||(b.snakes||{})[pos];if(jump)pos=jump;}db.prepare('UPDATE game_players SET position=?,roll=? WHERE room_id=? AND user_id=?').run(pos,roll,roomId,req.user.id);if(pos>=rules.target){finishGame(roomId,req.user.id);}else{const ix=players.findIndex(x=>x.user_id===req.user.id),next=(room.game_type==='ludo'&&roll===6)?req.user.id:players[(ix+1)%players.length].user_id;db.prepare('UPDATE game_rooms SET state_json=? WHERE id=?').run(JSON.stringify({...state,turn:next,turnStartedAt:Date.now(),lastRoll:{userId:req.user.id,roll,position:pos}}),roomId);}
 }
 res.json({room:gameRoomState(roomId,req.user.id)});
});
const challengeCatalog=[
 {id:'play1',category:'بازی و برد',title:'انجام ۱ بازی',scope:'daily',metric:'playsDay',target:1,coins:60,fame:20,sequence:'plays'},
 {id:'play3',category:'بازی و برد',title:'انجام ۳ بازی',scope:'daily',metric:'playsDay',target:3,coins:120,fame:40,sequence:'plays'},
 {id:'play5',category:'بازی و برد',title:'انجام ۵ بازی',scope:'daily',metric:'playsDay',target:5,coins:200,fame:60,sequence:'plays'},
 {id:'play10',category:'بازی و برد',title:'انجام ۱۰ بازی',scope:'daily',metric:'playsDay',target:10,coins:400,fame:120,sequence:'plays'},
 {id:'win1',category:'بازی و برد',title:'بردن ۱ بازی',scope:'daily',metric:'winsDay',target:1,coins:100,fame:30,sequence:'wins'},
 {id:'win3',category:'بازی و برد',title:'بردن ۳ بازی',scope:'daily',metric:'winsDay',target:3,coins:200,fame:60,sequence:'wins'},
 {id:'win5',category:'بازی و برد',title:'بردن ۵ بازی',scope:'daily',metric:'winsDay',target:5,coins:360,fame:100,sequence:'wins'},
 {id:'login1',category:'ورود روزانه',title:'ورود روزانه',scope:'daily',metric:'loginDays',target:1,coins:40,fame:10,sequence:'login'},
 {id:'login3',category:'ورود روزانه',title:'ورود ۳ روز متوالی',scope:'permanent',metric:'streak',target:3,coins:200,fame:60,sequence:'login'},
 {id:'login7',category:'ورود روزانه',title:'ورود ۷ روز متوالی',scope:'permanent',metric:'streak',target:7,coins:600,fame:200,sequence:'login'},
 {id:'invite1',category:'دعوت دوستان',title:'دعوت ۱ نفر معتبر',scope:'permanent',metric:'referrals',target:1,coins:1000,fame:200},
 {id:'invite2',category:'دعوت دوستان',title:'دعوت ۲ نفر معتبر',scope:'permanent',metric:'referrals',target:2,coins:2000,fame:400},
 {id:'invite4',category:'دعوت دوستان',title:'دعوت ۴ نفر معتبر',scope:'permanent',metric:'referrals',target:4,coins:6000,fame:1000},
 {id:'invite5',category:'دعوت دوستان',title:'دعوت ۵ نفر معتبر',scope:'permanent',metric:'referrals',target:5,coins:10000,fame:1600},
 {id:'invite10',category:'دعوت دوستان',title:'دعوت ۱۰ نفر معتبر',scope:'permanent',metric:'referrals',target:10,coins:20000,fame:3000},
 {id:'profile',category:'پروفایل و اجتماع',title:'تکمیل پروفایل',scope:'permanent',metric:'profileComplete',target:1,coins:200,fame:60},
 {id:'avatar',category:'پروفایل و اجتماع',title:'انتخاب عکس پروفایل',scope:'permanent',metric:'avatar',target:1,coins:100,fame:30},
 {id:'bio',category:'پروفایل و اجتماع',title:'نوشتن بیوگرافی',scope:'permanent',metric:'bio',target:1,coins:100,fame:30},
 {id:'follow1',category:'پروفایل و اجتماع',title:'دنبال کردن اولین کاربر',scope:'permanent',metric:'follows',target:1,coins:60,fame:20},
 {id:'follow5',category:'پروفایل و اجتماع',title:'دنبال کردن ۵ کاربر',scope:'permanent',metric:'follows',target:5,coins:200,fame:60},
 {id:'likes1',category:'پروفایل و اجتماع',title:'دریافت اولین لایک',scope:'permanent',metric:'likesIn',target:1,coins:60,fame:20},
 {id:'likes10',category:'پروفایل و اجتماع',title:'دریافت ۱۰ لایک',scope:'permanent',metric:'likesIn',target:10,coins:500,fame:150},
 {id:'weekplays',category:'چالش‌های هفتگی',title:'انجام ۳۰ بازی در هفته',scope:'weekly',metric:'playsWeek',target:30,coins:2000,fame:600},
 {id:'weekwins',category:'چالش‌های هفتگی',title:'بردن ۱۰ بازی در هفته',scope:'weekly',metric:'winsWeek',target:10,coins:1600,fame:500},
 {id:'level5',category:'سطح و افتخار',title:'رسیدن به سطح ۵',scope:'permanent',metric:'level',target:5,coins:400,fame:100},
 {id:'level10',category:'سطح و افتخار',title:'رسیدن به سطح ۱۰',scope:'permanent',metric:'level',target:10,coins:1000,fame:300},
 {id:'games100',category:'سطح و افتخار',title:'انجام ۱۰۰ بازی',scope:'permanent',metric:'playsAll',target:100,coins:1000,fame:10000},
 {id:'games500',category:'سطح و افتخار',title:'انجام ۵۰۰ بازی',scope:'permanent',metric:'playsAll',target:500,coins:5000,fame:100000},
 {id:'coins1000',category:'سطح و افتخار',title:'جمع‌آوری ۱٬۰۰۰ سکه',scope:'permanent',metric:'coinsEarned',target:1000,coins:300,fame:100},
 {id:'missions10',category:'سطح و افتخار',title:'تکمیل ۱۰ مأموریت',scope:'permanent',metric:'missions',target:10,coins:1000,fame:300}
];
const challengeMetricsAllowed=new Set(['playsDay','winsDay','playsWeek','winsWeek','playsAll','loginDays','streak','referrals','profileComplete','avatar','bio','follows','likesIn','level','coinsEarned','missions']);
function allChallenges(){return [...challengeCatalog,...db.prepare('SELECT * FROM custom_challenges WHERE active=1 AND (starts_at IS NULL OR starts_at<=CURRENT_TIMESTAMP) AND (ends_at IS NULL OR ends_at>=CURRENT_TIMESTAMP) ORDER BY id').all().map(c=>({id:'custom_'+c.id,customId:c.id,category:c.category,title:c.title,scope:c.scope,metric:c.metric,target:c.target,coins:c.reward_coins,fame:c.reward_fame,active:!!c.active}))].map(c=>({...c,fame:Math.max(0,Math.floor(Number(c.fame)||0))*100}))}
function challengePeriod(c){const now=iranClock();if(c.scope==='daily')return 'daily:'+iranKey(now);if(c.scope==='weekly'){const d=new Date(now);d.setUTCDate(d.getUTCDate()-((d.getUTCDay()+6)%7));return 'weekly:'+iranKey(d)}return 'all';}
function challengeMetrics(uid){const now=iranClock(),today=iranKey(now),dayStart=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' '),dayEnd=new Date(iranMidnightUtc(now).getTime()+86400000).toISOString().slice(0,19).replace('T',' '),weekStart=new Date(iranMidnightUtc(now).getTime()-6*86400000).toISOString().slice(0,19).replace('T',' ');const n=(sql,...a)=>Number(db.prepare(sql).get(...a)?.n||0);const days=db.prepare('SELECT day_key FROM user_login_days WHERE user_id=? ORDER BY day_key DESC').all(uid).map(x=>x.day_key);let streak=0;let cur=new Date(iranClock());for(let i=0;i<days.length;i++){const expect=iranKey(cur);if(days.includes(expect)){streak++;cur.setUTCDate(cur.getUTCDate()-1)}else if(i===0&&days.includes(iranKey(new Date(cur.getTime()-86400000)))){cur.setUTCDate(cur.getUTCDate()-1);i--;continue}else break;}
 return {playsDay:n('SELECT COUNT(*) n FROM game_results WHERE user_id=? AND created_at>=? AND created_at<?',uid,dayStart,dayEnd),winsDay:n("SELECT COUNT(*) n FROM game_results WHERE user_id=? AND result='win' AND created_at>=? AND created_at<?",uid,dayStart,dayEnd),playsWeek:n('SELECT COUNT(*) n FROM game_results WHERE user_id=? AND created_at>=?',uid,weekStart),winsWeek:n("SELECT COUNT(*) n FROM game_results WHERE user_id=? AND result='win' AND created_at>=?",uid,weekStart),playsAll:n('SELECT COUNT(*) n FROM game_results WHERE user_id=?',uid),loginDays:days.includes(today)?1:0,streak,referrals:n('SELECT COUNT(*) n FROM referrals WHERE inviter_id=?',uid),profileComplete:(()=>{const u=userById(uid);return !!(u&&u.display_name&&u.avatar&&u.bio)})()?1:0,avatar:userById(uid)?.avatar?1:0,bio:userById(uid)?.bio?.trim()?1:0,follows:n('SELECT COUNT(*) n FROM follows WHERE follower_id=?',uid),likesIn:n('SELECT COUNT(*) n FROM likes WHERE to_id=?',uid),level:Number(userById(uid)?.level||1),coinsEarned:n("SELECT COALESCE(SUM(CASE WHEN amount>0 THEN amount ELSE 0 END),0) n FROM transactions WHERE user_id=?",uid),missions:n('SELECT COUNT(*) n FROM challenge_claims WHERE user_id=?',uid)};}
app.get('/api/admin/challenges',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی ندارید'});res.json({challenges:db.prepare('SELECT * FROM custom_challenges ORDER BY id DESC').all(),metrics:[...challengeMetricsAllowed]})});
app.post('/api/admin/challenges',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'ساخت مأموریت اختصاصی فقط برای مالک/سازنده است'});const b=req.body||{},title=String(b.title||'').trim().slice(0,100),category=String(b.category||'چالش اختصاصی مدیر').trim().slice(0,60),metric=String(b.metric||''),target=Math.floor(Number(b.target)),scope=['daily','weekly','permanent'].includes(b.scope)?b.scope:'permanent',coins=Math.floor(Number(b.coins)),fame=Math.floor(Number(b.fame));if(title.length<3||!challengeMetricsAllowed.has(metric)||!Number.isInteger(target)||target<1||target>1000000||!Number.isInteger(coins)||coins<0||coins>1000000||!Number.isInteger(fame)||fame<0||fame>1000000)return res.status(400).json({error:'عنوان، شرط و جایزه معتبر وارد کن'});const starts=b.startsAt?new Date(b.startsAt):null,ends=b.endsAt?new Date(b.endsAt):null;if((starts&&!Number.isFinite(starts.getTime()))||(ends&&!Number.isFinite(ends.getTime()))||(starts&&ends&&starts>=ends))return res.status(400).json({error:'زمان شروع و پایان معتبر نیست'});const r=db.prepare('INSERT INTO custom_challenges(title,category,metric,target,scope,reward_coins,reward_fame,starts_at,ends_at,active,created_by) VALUES(?,?,?,?,?,?,?,?,?,1,?)').run(title,category,metric,target,scope,coins,fame,starts?.toISOString()||null,ends?.toISOString()||null,req.user.id);res.json({ok:true,id:r.lastInsertRowid,message:'مأموریت اختصاصی ساخته شد'});});
app.patch('/api/admin/challenges/:id',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی ندارید'});const id=Number(req.params.id),active=req.body?.active===true?1:0;const r=db.prepare('UPDATE custom_challenges SET active=? WHERE id=?').run(active,id);if(!r.changes)return res.status(404).json({error:'مأموریت پیدا نشد'});res.json({ok:true,active:!!active})});
app.delete('/api/admin/challenges/:id',requireAuth,(req,res)=>{if(!['owner','creator'].includes(req.user.role))return res.status(403).json({error:'دسترسی ندارید'});const r=db.prepare('DELETE FROM custom_challenges WHERE id=?').run(Number(req.params.id));if(!r.changes)return res.status(404).json({error:'مأموریت پیدا نشد'});res.json({ok:true})});
app.get('/api/challenges',requireAuth,(req,res)=>{const metrics=challengeMetrics(req.user.id);const rows=allChallenges().map(c=>{const period=challengePeriod(c),claimed=!!db.prepare('SELECT 1 FROM challenge_claims WHERE user_id=? AND challenge_id=? AND period_key=?').get(req.user.id,c.id,period);const sequence=c.sequence?allChallenges().filter(x=>x.sequence===c.sequence).sort((a,b)=>a.target-b.target):[];const next=sequence.find(x=>!db.prepare('SELECT 1 FROM challenge_claims WHERE user_id=? AND challenge_id=? AND period_key=?').get(req.user.id,x.id,challengePeriod(x)));const locked=!!(c.sequence&&next&&next.id!==c.id);const progress=Math.min(c.target,Number(metrics[c.metric]||0));return {...c,period,progress,claimed,locked,completed:progress>=c.target,available:progress>=c.target&&!claimed&&!locked,updateLabel:'بروزرسانی بعدی'};});res.json({challenges:rows,metrics,serverTime:new Date().toISOString()});});
app.post('/api/challenges/:id/claim',requireAuth,(req,res)=>{const c=allChallenges().find(x=>x.id===String(req.params.id));if(!c)return res.status(404).json({error:'مأموریت پیدا نشد'});const period=challengePeriod(c),metrics=challengeMetrics(req.user.id);if(c.sequence){const sequence=allChallenges().filter(x=>x.sequence===c.sequence).sort((a,b)=>a.target-b.target);const next=sequence.find(x=>!db.prepare('SELECT 1 FROM challenge_claims WHERE user_id=? AND challenge_id=? AND period_key=?').get(req.user.id,x.id,challengePeriod(x)));if(next&&next.id!==c.id)return res.status(409).json({error:'ابتدا مرحله قبلی را کامل کن'});}if(Number(metrics[c.metric]||0)<c.target)return res.status(400).json({error:'شرط این مأموریت هنوز از سمت سرور تکمیل نشده است'});try{atomic(()=>{db.prepare('INSERT INTO challenge_claims(user_id,challenge_id,period_key,reward_coins,reward_fame) VALUES(?,?,?,?,?)').run(req.user.id,c.id,period,c.coins,c.fame);addTx(req.user.id,c.coins,'challenge_reward','جایزه مأموریت: '+c.title);syncUserFameAndLevel(req.user.id,{fame:Math.max(0,Number(userById(req.user.id)?.fame||0))+c.fame});});}catch(e){return res.status(409).json({error:'این جایزه قبلاً دریافت شده یا ذخیره آن ناموفق بود'});}notify(req.user.id,'جایزه چالش',`${c.title}: ${c.coins} سکه و ${c.fame} شهرت دریافت کردی.`);res.json({ok:true,reward:{coins:c.coins,fame:c.fame},user:safeUser(userById(req.user.id))});});

app.get('/api/games/leaderboard',requireAuth,(req,res)=>{
 const period=['daily','weekly','all'].includes(req.query.period)?req.query.period:'daily';const where=period==='daily'?"AND gr.created_at>=datetime('now','start of day')":period==='weekly'?"AND gr.created_at>=datetime('now','-7 days')":'';
 const rows=db.prepare(`SELECT u.id,u.username,u.display_name,u.avatar,u.level, SUM(CASE WHEN gr.result='win' THEN 1 ELSE 0 END) wins, COUNT(gr.id) games, COALESCE(SUM(gr.fame_delta),0) fame FROM game_results gr JOIN users u ON u.id=gr.user_id WHERE 1=1 ${where} GROUP BY u.id ORDER BY wins DESC,fame DESC,games DESC LIMIT 50`).all();res.json({period,users:rows});
});
app.post('/api/games/leaderboard/claim',requireAuth,(req,res)=>{
 const period=String(req.body.period||'');if(!['daily','weekly'].includes(period))return res.status(400).json({error:'بازه جایزه معتبر نیست'});const where=period==='daily'?"AND gr.created_at>=datetime('now','start of day')":"AND gr.created_at>=datetime('now','-7 days')";const winners=db.prepare(`SELECT u.id,SUM(CASE WHEN gr.result='win' THEN 1 ELSE 0 END) wins,COALESCE(SUM(gr.fame_delta),0) fame FROM game_results gr JOIN users u ON u.id=gr.user_id WHERE 1=1 ${where} GROUP BY u.id ORDER BY wins DESC,fame DESC LIMIT 3`).all();const place=winners.findIndex(x=>x.id===req.user.id)+1;if(!place)return res.status(400).json({error:'در رتبه‌های جایزه‌دار نیستی'});const key=period+':'+(period==='daily'?new Date().toISOString().slice(0,10):new Date(Date.now()-((new Date().getUTCDay()+6)%7)*86400000).toISOString().slice(0,10));if(db.prepare('SELECT id FROM game_reward_claims WHERE user_id=? AND period_key=?').get(req.user.id,key))return res.status(409).json({error:'جایزه این بازه قبلاً دریافت شده'});
 if(period==='daily'){const amount=Math.max(0,Number(getSetting('game_daily_reward_'+place))||0);addTx(req.user.id,amount,'game_leaderboard_reward',`جایزه رتبه ${place} روزانه`);}else{const days=[10,7,3][place-1];const until=Date.parse(req.user.pro_until||'')>Date.now()?Date.parse(req.user.pro_until):Date.now();db.prepare('UPDATE users SET pro_until=? WHERE id=?').run(new Date(until+days*86400000).toISOString(),req.user.id);}
 db.prepare('INSERT INTO game_reward_claims(user_id,period_key,place,reward_type) VALUES(?,?,?,?)').run(req.user.id,key,place,period==='daily'?'coins':'pro_days');res.json({ok:true,place,user:safeUser(userById(req.user.id)),message:period==='daily'?'جایزه سکه دریافت شد':`اشتراک پرو ${[10,7,3][place-1]} روزه فعال شد`});
});
// Distribute game leaderboard rewards automatically at Iran midnight (UTC+03:30).
function iranClock(){return new Date(Date.now()+210*60000)}
function iranKey(d){return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`}
function iranMidnightUtc(d){return new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate())-210*60000)}
function distributeLeaderboardRewards(){
 const now=iranClock(),today=iranKey(now),midnight=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' '),previousDay=new Date(now.getTime()-86400000),dailyDate=iranKey(previousDay),dailyStart=iranMidnightUtc(previousDay).toISOString().slice(0,19).replace('T',' ');
 const dailyKey='auto-daily:'+dailyDate;
 if(!db.prepare('SELECT 1 FROM daily_ranking_distributions WHERE period_key=?').get(dailyKey)){
  const winners=db.prepare('SELECT u.id, COALESCE(SUM(fe.amount),0) fame FROM fame_events fe JOIN users u ON u.id=fe.user_id WHERE fe.created_at>=? AND fe.created_at<? GROUP BY u.id ORDER BY fame DESC,u.id ASC LIMIT 3').all(dailyStart,midnight);
  winners.forEach((w,i)=>{const place=i+1,amount=Math.max(0,Number(getSetting('game_daily_reward_'+place))||[1000,500,250][i]),diamonds=[50,20,10][i],oldClaim=db.prepare('SELECT reward_type FROM game_reward_claims WHERE user_id=? AND period_key=?').get(w.id,dailyKey);if(!oldClaim){addTx(w.id,amount,'game_leaderboard_reward',`جایزه خودکار رتبه ${place} روزانه`);db.prepare('INSERT OR IGNORE INTO game_reward_claims(user_id,period_key,place,reward_type) VALUES(?,?,?,?)').run(w.id,dailyKey,place,'coins_and_diamonds');}if(!oldClaim||oldClaim.reward_type==='coins'){db.prepare('UPDATE users SET diamonds=diamonds+? WHERE id=?').run(diamonds,w.id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(w.id,diamonds,'daily_ranking_diamonds',`جایزه ${place} روزانه: الماس`);notify(w.id,'جایزه روزانه بازی',`رتبه ${place} روزانه؛ ${oldClaim?'فقط '+diamonds+' الماس باقی‌مانده':amount+' سکه و '+diamonds+' الماس'} به‌صورت خودکار واریز شد.`);}});
  db.prepare('INSERT OR IGNORE INTO daily_ranking_distributions(period_key) VALUES(?)').run(dailyKey);
 }
 // Activity rewards are paid as coins at 21:00 Iran time, for fame earned since Iran midnight.
 const activityKey='activity-coins:'+today;
 if(now.getUTCHours()>=21&&!db.prepare('SELECT 1 FROM daily_ranking_distributions WHERE period_key=?').get(activityKey)){
  const activityStart=iranMidnightUtc(now).toISOString().slice(0,19).replace('T',' ');
  const activityEnd=new Date(iranMidnightUtc(now).getTime()+21*3600000).toISOString().slice(0,19).replace('T',' ');
  const earners=db.prepare('SELECT user_id, SUM(amount) fame FROM fame_events WHERE created_at>=? AND created_at<? GROUP BY user_id').all(activityStart,activityEnd);
  for(const e of earners){const coins=Math.floor(Math.max(0,Number(e.fame)||0)/500);if(!coins)continue;const exists=db.prepare('SELECT 1 FROM daily_activity_coin_distributions WHERE period_key=? AND user_id=?').get(activityKey,e.user_id);if(exists)continue;atomic(()=>{db.prepare('UPDATE users SET coins=coins+? WHERE id=?').run(coins,e.user_id);db.prepare('INSERT INTO transactions(user_id,amount,kind,note) VALUES(?,?,?,?)').run(e.user_id,coins,'daily_activity_coins',`پاداش فعالیت روزانه: ${coins} سکه در برابر ${Math.floor(e.fame)} شهرت`);db.prepare('INSERT INTO daily_activity_coin_distributions(period_key,user_id,coins) VALUES(?,?,?)').run(activityKey,e.user_id,coins);});notify(e.user_id,'پاداش سکه فعالیت روزانه',`به اندازه فعالیت امروزت، ${coins} سکه به حسابت واریز شد.`);}
  db.prepare('INSERT OR IGNORE INTO daily_ranking_distributions(period_key) VALUES(?)').run(activityKey);
 }
 // Weekly rewards are delivered on Monday at 00:00 Iran time.
 const day=now.getUTCDay(); if(day===1){const weeklyKey='auto-weekly:'+today;if(!db.prepare('SELECT id FROM game_reward_claims WHERE period_key=? LIMIT 1').get(weeklyKey)){const start=new Date(iranMidnightUtc(now).getTime()-7*86400000).toISOString().slice(0,19).replace('T',' ');const winners=db.prepare('SELECT u.id, SUM(CASE WHEN gr.result="win" THEN 1 ELSE 0 END) wins, COALESCE(SUM(gr.fame_delta),0) fame FROM game_results gr JOIN users u ON u.id=gr.user_id WHERE gr.created_at>=? AND gr.created_at<? GROUP BY u.id ORDER BY wins DESC,fame DESC LIMIT 3').all(start,midnight);winners.forEach((w,i)=>{const days=[10,7,3][i],u=userById(w.id),until=Date.parse(u?.pro_until||'')>Date.now()?Date.parse(u.pro_until):Date.now();db.prepare('UPDATE users SET pro_until=? WHERE id=?').run(new Date(until+days*86400000).toISOString(),w.id);db.prepare('INSERT OR IGNORE INTO game_reward_claims(user_id,period_key,place,reward_type) VALUES(?,?,?,?)').run(w.id,weeklyKey,i+1,'pro_days');notify(w.id,'جایزه هفتگی بازی',`رتبه ${i+1}؛ ${days} روز اشتراک پرو به‌صورت خودکار فعال شد.`);});}}
}
setInterval(()=>{try{distributeLeaderboardRewards()}catch(e){console.error('Leaderboard reward scheduler:',e.message)}},30000);
try{distributeLeaderboardRewards()}catch(e){console.error('Leaderboard reward scheduler:',e.message)}
function cleanupExpiredChatImages(){
 const cutoff=Date.now()-3*86400000;
 const candidates=[
  ...db.prepare("SELECT id,media_url FROM messages WHERE deleted=0 AND type IN ('image','gif') AND media_url LIKE '/uploads/%'").all().map(x=>({table:'messages',id:x.id,url:x.media_url})),
  ...db.prepare("SELECT id,media_url FROM chat_group_messages WHERE deleted=0 AND type IN ('image','gif') AND media_url LIKE '/uploads/%'").all().map(x=>({table:'chat_group_messages',id:x.id,url:x.media_url}))
 ];
 for(const item of candidates){
  const rel=String(item.url||'').replace(/^\/uploads\//,'');
  const file=path.resolve(uploadsDir,rel);
  if(!rel||!file.startsWith(path.resolve(uploadsDir)+path.sep)||!fs.existsSync(file))continue;
  try{
   if(fs.statSync(file).mtimeMs<=cutoff){fs.unlinkSync(file);db.prepare(`UPDATE ${item.table} SET media_url='' WHERE id=?`).run(item.id);}
  }catch(e){console.warn('chat image cleanup:',e.message)}
 }
}
try{cleanupExpiredChatImages()}catch(e){console.error('Initial chat image cleanup:',e.message)}
setInterval(()=>{try{cleanupExpiredChatImages()}catch(e){console.error('Chat image cleanup:',e.message)}},6*60*60*1000);

// اسم‌فامیل آنلاین ACE CLUB — وضعیت مسابقه در سرور نگهداری می‌شود.
db.exec(`
CREATE TABLE IF NOT EXISTS nf_matches(
 id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT NOT NULL DEFAULT 'waiting',
 created_by INTEGER NOT NULL, created_at INTEGER NOT NULL, lobby_deadline INTEGER NOT NULL,
 started_at INTEGER, finished_at INTEGER, round_no INTEGER NOT NULL DEFAULT 0,
 round_deadline INTEGER, state_json TEXT NOT NULL DEFAULT '{}', winner_id INTEGER
);
CREATE TABLE IF NOT EXISTS nf_players(
 match_id INTEGER NOT NULL, user_id INTEGER NOT NULL, joined_at INTEGER NOT NULL,
 ready INTEGER NOT NULL DEFAULT 0, character_id TEXT, power_uses INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(match_id,user_id)
);
CREATE TABLE IF NOT EXISTS nf_answers(
 match_id INTEGER NOT NULL, round_no INTEGER NOT NULL, user_id INTEGER NOT NULL,
 answers_json TEXT NOT NULL DEFAULT '{}', locked INTEGER NOT NULL DEFAULT 0,
 submitted_at INTEGER, PRIMARY KEY(match_id,round_no,user_id)
);
CREATE TABLE IF NOT EXISTS nf_votes(
 match_id INTEGER NOT NULL, round_no INTEGER NOT NULL, voter_id INTEGER NOT NULL,
 target_user_id INTEGER NOT NULL, category TEXT NOT NULL, invalid INTEGER NOT NULL DEFAULT 1,
 created_at INTEGER NOT NULL, PRIMARY KEY(match_id,round_no,voter_id,target_user_id,category)
);
CREATE TABLE IF NOT EXISTS nf_results(
 match_id INTEGER NOT NULL, user_id INTEGER NOT NULL, place INTEGER NOT NULL,
 score INTEGER NOT NULL DEFAULT 0, fame_awarded INTEGER NOT NULL DEFAULT 0,
 coins_awarded INTEGER NOT NULL DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY(match_id,user_id)
);
CREATE TABLE IF NOT EXISTS nf_dictionary(
 id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL, answer TEXT NOT NULL,
 normalized TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, created_by INTEGER,
 UNIQUE(category,normalized)
);
CREATE TABLE IF NOT EXISTS nf_power_events(
 id INTEGER PRIMARY KEY AUTOINCREMENT, match_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 power_id TEXT NOT NULL, target_user_id INTEGER, round_no INTEGER NOT NULL,
 payload_json TEXT NOT NULL DEFAULT '{}', created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
`);
const NF_CATEGORIES=[
 {id:'name',label:'اسم'},{id:'family',label:'فامیل'},{id:'city',label:'شهر'},{id:'country',label:'کشور'},{id:'food',label:'غذا'},{id:'animal',label:'حیوان'},{id:'object',label:'اشیا'},{id:'color',label:'رنگ'},{id:'job',label:'شغل'},{id:'fruit',label:'میوه'},{id:'plant',label:'گل و گیاه'},{id:'vehicle',label:'وسیله نقلیه'}
];
const NF_LETTERS=['آ','ا','ب','پ','ت','ج','چ','ح','خ','د','ر','ز','س','ش','ع','ف','ق','ک','گ','ل','م','ن','و','ه','ی'];
const NF_CHARACTERS=[
 {id:'scholar',name:'دانشمند',rarity:'نادر',description:'یک راهنمایی محدود برای یک پاسخ',power:'hint'},
 {id:'guardian',name:'نگهبان',rarity:'حماسی',description:'محافظت در برابر یک اثر ویژه در مسابقه',power:'shield'},
 {id:'sprinter',name:'تندرو',rarity:'کمیاب',description:'در یک دور ۱۰ ثانیه زمان بیشتر',power:'time'},
 {id:'strategist',name:'استراتژیست',rarity:'نادر',description:'نمایش دسته‌های دور آینده',power:'preview'},
 {id:'challenger',name:'چالشگر',rarity:'حماسی',description:'در صورت کسب حداقل ۵ پاسخ معتبر، ۵ امتیاز اضافه',power:'bonus'}
];
const nfPickRoundCategories=()=>[...NF_CATEGORIES].sort(()=>Math.random()-.5).slice(0,4);
const nfNormalize=v=>String(v||'').normalize('NFKC').replace(/[يى]/g,'ی').replace(/ك/g,'ک').replace(/[\u064B-\u065F\u0670]/g,'').replace(/[\u200c\u200d]/g,' ').replace(/\s+/g,' ').trim().toLowerCase();
const nfSafeJson=v=>{try{return JSON.parse(v||'{}')}catch{return {}}};
function nfMatchState(matchId,userId){
 let m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(matchId);if(!m)return null;
 let st=nfSafeJson(m.state_json),players=db.prepare(`SELECT p.user_id,p.joined_at,p.ready,p.character_id,p.power_uses,u.username,u.display_name,u.avatar,u.level FROM nf_players p JOIN users u ON u.id=p.user_id WHERE p.match_id=? ORDER BY p.joined_at,p.user_id`).all(matchId);
 if(m.status==='waiting'&&Date.now()>=m.lobby_deadline){
   if(players.length>=2){nfStartMatch(matchId);m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(matchId);st=nfSafeJson(m.state_json);}
   else{db.prepare("UPDATE nf_matches SET status='finished',finished_at=?,state_json=? WHERE id=? AND status='waiting'").run(Date.now(),JSON.stringify({failed:'not_enough_players'}),matchId);m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(matchId);st=nfSafeJson(m.state_json);}
 }
 if(m.status==='playing'&&m.round_deadline&&Date.now()>=m.round_deadline){nfCloseRound(matchId);m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(matchId);st=nfSafeJson(m.state_json);}
 if(m.status==='review'&&m.round_deadline&&Date.now()>=m.round_deadline){nfFinalizeReview(matchId);m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(matchId);st=nfSafeJson(m.state_json);}
 const my=db.prepare('SELECT * FROM nf_answers WHERE match_id=? AND round_no=? AND user_id=?').get(matchId,m.round_no,userId);
 const answerRows=db.prepare('SELECT user_id,answers_json,locked,submitted_at FROM nf_answers WHERE match_id=? AND round_no=?').all(matchId,m.round_no);
 const answers=(m.status==='finished'||m.status==='review')?answerRows.map(a=>({userId:a.user_id,answers:nfSafeJson(a.answers_json),locked:!!a.locked,submittedAt:a.submitted_at,votes:db.prepare('SELECT COUNT(*) n FROM nf_votes WHERE match_id=? AND round_no=? AND target_user_id=? AND category=? AND invalid=1').get(matchId,m.round_no,a.user_id,'name')?.n||0})):answerRows.map(a=>({userId:a.user_id,locked:!!a.locked,submittedAt:a.submitted_at}));
 return {id:m.id,status:m.status,createdAt:m.created_at,reviewDeadline:m.status==='review'?m.round_deadline:null,ownCharacter:NF_CHARACTERS.find(c=>c.id===players.find(p=>Number(p.user_id)===Number(userId))?.character_id)||null,reviewVotes:db.prepare('SELECT target_user_id,category,invalid FROM nf_votes WHERE match_id=? AND round_no=? AND voter_id=?').all(matchId,m.round_no,userId),lobbyDeadline:m.lobby_deadline,startedAt:m.started_at,roundNo:m.round_no,roundDeadline:m.round_deadline,letter:st.letter||null,categories:st.categories||NF_CATEGORIES,roundsTotal:st.roundsTotal||5,players:players.map(p=>({...p,ready:!!p.ready,character:NF_CHARACTERS.find(c=>c.id===p.character_id)||null,powerUses:p.power_uses})),answers,ownAnswers:nfSafeJson(my?.answers_json),ownLocked:!!my?.locked,scoreboard:st.scoreboard||{},winnerId:m.winner_id,results:m.status==='finished'?db.prepare('SELECT r.*,u.display_name,u.avatar FROM nf_results r JOIN users u ON u.id=r.user_id WHERE r.match_id=? ORDER BY r.place').all(matchId):[],failed:st.failed||null,settings:st.settings||{roundSeconds:60,rounds:5}};
}
function nfStartMatch(id){
 const m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='waiting'").get(id);if(!m)return false;
 const ps=db.prepare('SELECT user_id FROM nf_players WHERE match_id=? ORDER BY joined_at,user_id').all(id);if(ps.length<2)return false;
 const now=Date.now(),roundsTotal=5,roundSeconds=Math.max(30,Math.min(180,Number(getSetting('nf_round_seconds'))||60)),st={roundsTotal,roundSeconds,roundNo:1,letter:NF_LETTERS[Math.floor(Math.random()*NF_LETTERS.length)],categories:nfPickRoundCategories(),scoreboard:Object.fromEntries(ps.map(p=>[p.user_id,0])),settings:{roundSeconds,rounds:roundsTotal},roundStartedAt:now,usedPowers:{},characterDrafts:{}};
 for(const p of ps){const ch=NF_CHARACTERS[Math.floor(Math.random()*NF_CHARACTERS.length)];db.prepare('UPDATE nf_players SET character_id=?,ready=1 WHERE match_id=? AND user_id=?').run(ch.id,id,p.user_id);}
 const changed=db.prepare("UPDATE nf_matches SET status='playing',started_at=?,round_no=1,round_deadline=?,state_json=? WHERE id=? AND status='waiting'").run(now,now+roundSeconds*1000,JSON.stringify(st),id);
 if(changed.changes){for(const p of ps){db.prepare('INSERT OR IGNORE INTO nf_answers(match_id,round_no,user_id,answers_json,locked) VALUES(?,1,?,\'{}\',0)').run(id,p.user_id);notify(p.user_id,'مسابقه اسم‌فامیل شروع شد',`حرف این دور: ${st.letter}؛ ۶۰ ثانیه فرصت داری. برای ورود به بازی، اعلان را باز کن.`,{action:'namefamile_start',requestId:id});}}
 return !!changed.changes;
}
function nfCloseRound(id){
 const m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='playing'").get(id);if(!m)return false;
 const st=nfSafeJson(m.state_json),now=Date.now();
 db.prepare('UPDATE nf_answers SET locked=1 WHERE match_id=? AND round_no=?').run(id,m.round_no);
 st.reviewStartedAt=now;st.reviewSeconds=20;
 db.prepare("UPDATE nf_matches SET status='review',round_deadline=?,state_json=? WHERE id=? AND status='playing'").run(now+20000,JSON.stringify(st),id);
 return true;
}
function nfFinalizeReview(id){
 const m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='review'").get(id);if(!m)return false;
 const st=nfSafeJson(m.state_json),players=db.prepare('SELECT user_id,character_id,joined_at FROM nf_players WHERE match_id=?').all(id),rows=db.prepare('SELECT * FROM nf_answers WHERE match_id=? AND round_no=?').all(id,m.round_no),parsed=rows.map(r=>({row:r,answers:nfSafeJson(r.answers_json)}));
 const counts={};for(const cat of (st.categories||NF_CATEGORIES)){for(const x of parsed){const v=nfNormalize(x.answers[cat.id]),first=v.replace(/^[آا]/,'ا').charAt(0),letter=String(st.letter||'').replace(/^[آا]/,'ا');if(v&&first===letter)counts[cat.id+'|'+v]=(counts[cat.id+'|'+v]||0)+1;}}
 for(const x of parsed){let score=0;for(const cat of (st.categories||NF_CATEGORIES)){const raw=String(x.answers[cat.id]||'').trim(),v=nfNormalize(raw);if(!v)continue;const first=v.replace(/^[آا]/,'ا').charAt(0),letter=String(st.letter||'').replace(/^[آا]/,'ا');if(first!==letter)continue;
  const exists=db.prepare('SELECT 1 FROM nf_dictionary WHERE category=? AND normalized=? AND active=1').get(cat.id,v),hasDictionary=db.prepare('SELECT 1 FROM nf_dictionary WHERE category=? AND active=1 LIMIT 1').get(cat.id);if(hasDictionary&&!exists)continue;
  const eligible=Math.max(1,players.length-1),threshold=Math.max(1,Math.ceil(eligible/2)),votes=db.prepare('SELECT COUNT(*) n FROM nf_votes WHERE match_id=? AND round_no=? AND target_user_id=? AND category=? AND invalid=1').get(id,m.round_no,x.row.user_id,cat.id).n;
  if(votes>=threshold)continue;
  score+=(counts[cat.id+'|'+v]===1?10:5);
 }
 const player=players.find(p=>p.user_id===x.row.user_id),ch=NF_CHARACTERS.find(c=>c.id===player?.character_id);if(ch?.power==='bonus'&&(st.categories||NF_CATEGORIES).filter(c=>nfNormalize(x.answers[c.id])).length>=4)score+=5;
 st.scoreboard[String(x.row.user_id)]=Number(st.scoreboard[String(x.row.user_id)]||0)+score;
 }
 if(m.round_no>=Number(st.roundsTotal||5)){
  const ranked=players.map(p=>({user_id:p.user_id,score:Number(st.scoreboard[String(p.user_id)]||0)})).sort((a,b)=>b.score-a.score||(Number(players.find(p=>p.user_id===a.user_id)?.joined_at)||0)-(Number(players.find(p=>p.user_id===b.user_id)?.joined_at)||0)||a.user_id-b.user_id),winner=ranked[0];st.finished=true;
  db.prepare("UPDATE nf_matches SET status='finished',finished_at=?,winner_id=?,round_deadline=NULL,state_json=? WHERE id=? AND status='review'").run(Date.now(),winner?.user_id||null,JSON.stringify(st),id);
  ranked.forEach((p,i)=>{const win=i===0,fame=win?10000:0,coins=win?100:0;db.prepare('INSERT OR IGNORE INTO nf_results(match_id,user_id,place,score,fame_awarded,coins_awarded) VALUES(?,?,?,?,?,?)').run(id,p.user_id,i+1,p.score,fame,coins);if(win){atomic(()=>{addTx(p.user_id,100,'namefamile_win','جایزه برد اسم‌فامیل');const u=userById(p.user_id);syncUserFameAndLevel(p.user_id,{fame:Math.max(0,Number(u?.fame||0),Number(u?.xp||0))+10000});});notify(p.user_id,'پیروزی در اسم‌فامیل','۱۰۰ سکه و ۱۰٬۰۰۰ شهرت به حسابت اضافه شد.');}else notify(p.user_id,'پایان اسم‌فامیل','مسابقه به پایان رسید؛ نتیجه و رتبه‌ات را ببین.');});return true;
 }
 const next=m.round_no+1,now=Date.now();st.roundNo=next;st.letter=NF_LETTERS[Math.floor(Math.random()*NF_LETTERS.length)];st.categories=nfPickRoundCategories();st.roundStartedAt=now;delete st.reviewStartedAt;
 db.prepare("UPDATE nf_matches SET status='playing',round_no=?,round_deadline=?,state_json=? WHERE id=? AND status='review'").run(next,now+Number(st.roundSeconds||60)*1000,JSON.stringify(st),id);
 for(const p of players)db.prepare('INSERT OR IGNORE INTO nf_answers(match_id,round_no,user_id,answers_json,locked) VALUES(?,?,?,\'{}\',0)').run(id,next,p.user_id);
 return true;
}
app.get('/api/namefamile/config',requireAuth,(req,res)=>res.json({categories:NF_CATEGORIES,characters:NF_CHARACTERS,letters:NF_LETTERS,settings:{minPlayers:2,maxPlayers:8,lobbySeconds:120,roundSeconds:Math.max(30,Math.min(180,Number(getSetting('nf_round_seconds'))||60)),rounds:5,winCoins:100,winFame:10000}}));
app.post('/api/namefamile/join',requireAuth,(req,res)=>{
 const active=db.prepare("SELECT m.id FROM nf_matches m JOIN nf_players p ON p.match_id=m.id WHERE p.user_id=? AND m.status IN ('waiting','playing') ORDER BY m.id DESC LIMIT 1").get(req.user.id);
 if(active)return res.json({ok:true,match:nfMatchState(active.id,req.user.id),rejoined:true});
 let m=db.prepare("SELECT m.id,m.created_at,m.lobby_deadline FROM nf_matches m WHERE m.status='waiting' AND m.lobby_deadline>? AND (SELECT COUNT(*) FROM nf_players p WHERE p.match_id=m.id)<8 ORDER BY m.id ASC LIMIT 1").get(Date.now());
 if(!m){const now=Date.now(),r=db.prepare("INSERT INTO nf_matches(status,created_by,created_at,lobby_deadline,state_json) VALUES('waiting',?,?,?,'{}')").run(req.user.id,now,now+120000);m={id:r.lastInsertRowid,created_at:now,lobby_deadline:now+120000};}
 try{atomic(()=>{const n=db.prepare('SELECT COUNT(*) n FROM nf_players WHERE match_id=?').get(m.id).n;if(n>=8)throw new Error('مسابقه پر شده است');db.prepare('INSERT INTO nf_players(match_id,user_id,joined_at) VALUES(?,?,?)').run(m.id,req.user.id,Date.now());if(n+1>=8)nfStartMatch(m.id);});}catch(e){return res.status(409).json({error:e.message||'ورود به مسابقه ناموفق بود'});}
 res.json({ok:true,match:nfMatchState(m.id,req.user.id)});
});
app.get('/api/namefamile/match/:id',requireAuth,(req,res)=>{const id=Number(req.params.id);if(!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو این مسابقه نیستی'});res.json({match:nfMatchState(id,req.user.id)});});
app.post('/api/namefamile/match/:id/leave',requireAuth,(req,res)=>{const id=Number(req.params.id),m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(id);if(!m||!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(id,req.user.id))return res.status(404).json({error:'مسابقه پیدا نشد'});if(m.status==='playing')return res.status(409).json({error:'بعد از شروع مسابقه امکان خروج بدون ثبت نتیجه وجود ندارد'});db.prepare('DELETE FROM nf_players WHERE match_id=? AND user_id=?').run(id,req.user.id);const n=db.prepare('SELECT COUNT(*) n FROM nf_players WHERE match_id=?').get(id).n;if(!n)db.prepare("UPDATE nf_matches SET status='finished',finished_at=?,state_json='{\"failed\":\"empty_lobby\"}' WHERE id=?").run(Date.now(),id);res.json({ok:true});});
app.post('/api/namefamile/match/:id/answer',requireAuth,(req,res)=>{
 const id=Number(req.params.id),m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='playing'").get(id);if(!m)return res.status(409).json({error:'این دور فعال نیست'});if(Date.now()>=m.round_deadline){nfCloseRound(id);return res.status(409).json({error:'زمان دور تمام شد؛ پاسخ‌ها قفل شدند'});}
 if(!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو مسابقه نیستی'});
 const old=db.prepare('SELECT * FROM nf_answers WHERE match_id=? AND round_no=? AND user_id=?').get(id,m.round_no,req.user.id);if(old?.locked)return res.status(409).json({error:'پاسخ‌ها قبلاً ثبت نهایی شده‌اند'});
 const input=req.body.answers;if(!input||typeof input!=='object'||Array.isArray(input))return res.status(400).json({error:'پاسخ‌ها نامعتبرند'});
 const answers={};for(const c of (nfSafeJson(m.state_json).categories||NF_CATEGORIES))answers[c.id]=String(input[c.id]||'').trim().slice(0,100);
 db.prepare('INSERT INTO nf_answers(match_id,round_no,user_id,answers_json,locked,submitted_at) VALUES(?,?,?,?,0,?) ON CONFLICT(match_id,round_no,user_id) DO UPDATE SET answers_json=excluded.answers_json,submitted_at=excluded.submitted_at WHERE nf_answers.locked=0').run(id,m.round_no,req.user.id,JSON.stringify(answers),Date.now());
 res.json({ok:true,saved:true});
});
app.post('/api/namefamile/match/:id/submit',requireAuth,(req,res)=>{const id=Number(req.params.id),m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='playing'").get(id);if(!m)return res.status(409).json({error:'این دور فعال نیست'});if(Date.now()>=m.round_deadline){nfCloseRound(id);return res.status(409).json({error:'زمان تمام شده است'});}if(!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو مسابقه نیستی'});const a=db.prepare('SELECT * FROM nf_answers WHERE match_id=? AND round_no=? AND user_id=?').get(id,m.round_no,req.user.id);if(!a)return res.status(400).json({error:'ابتدا پاسخ‌ها را ذخیره کن'});const answers=nfSafeJson(a.answers_json),activeCats=nfSafeJson(m.state_json).categories||NF_CATEGORIES,missing=activeCats.filter(c=>!String(answers[c.id]||'').trim());if(missing.length)return res.status(400).json({error:'برای ثبت نهایی، هر ۴ دسته را پر کن: '+missing.map(c=>c.label).join('، ')});db.prepare('UPDATE nf_answers SET locked=1,submitted_at=? WHERE match_id=? AND round_no=? AND user_id=? AND locked=0').run(Date.now(),id,m.round_no,req.user.id);nfCloseRound(id);res.json({ok:true,locked:true,roundEnded:true,reviewSeconds:20});});
app.post('/api/namefamile/match/:id/vote',requireAuth,(req,res)=>{
 const id=Number(req.params.id),m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='review'").get(id);if(!m)return res.status(409).json({error:'زمان بررسی پاسخ‌ها فعال نیست'});if(Date.now()>=m.round_deadline){nfFinalizeReview(id);return res.status(409).json({error:'زمان بررسی تمام شده است'});}
 if(!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(id,req.user.id))return res.status(403).json({error:'عضو مسابقه نیستی'});
 const target=Number(req.body.targetUserId),category=String(req.body.category||''),invalid=req.body.invalid===false?0:1;
 if(target===Number(req.user.id)||!(NF_CATEGORIES.some(c=>c.id===category)&&((nfSafeJson(m.state_json).categories||NF_CATEGORIES).some(c=>c.id===category)))||!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(id,target))return res.status(400).json({error:'رأی نامعتبر است'});
 db.prepare('INSERT INTO nf_votes(match_id,round_no,voter_id,target_user_id,category,invalid,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(match_id,round_no,voter_id,target_user_id,category) DO UPDATE SET invalid=excluded.invalid,created_at=excluded.created_at').run(id,m.round_no,req.user.id,target,category,invalid,Date.now());res.json({ok:true});
});
app.post('/api/namefamile/match/:id/power',requireAuth,(req,res)=>{
 const id=Number(req.params.id),power=String(req.body.power||''),m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status='playing'").get(id);if(!m)return res.status(409).json({error:'مسابقه فعال نیست'});
 const player=db.prepare('SELECT * FROM nf_players WHERE match_id=? AND user_id=?').get(id,req.user.id);if(!player)return res.status(403).json({error:'عضو مسابقه نیستی'});if(player.power_uses>=1)return res.status(409).json({error:'قدرت ویژه‌ات را قبلاً استفاده کرده‌ای'});
 const ch=NF_CHARACTERS.find(c=>c.id===player.character_id);if(!ch||ch.power!==power)return res.status(403).json({error:'این قدرت با شخصیت اختصاص‌یافته سازگار نیست'});
 const st=nfSafeJson(m.state_json);if(st.usedPowers?.[req.user.id])return res.status(409).json({error:'قدرت قبلاً ثبت شده است'});
 const payload={};if(power==='time'){db.prepare('UPDATE nf_matches SET round_deadline=round_deadline+10000 WHERE id=? AND round_no=?').run(id,m.round_no);payload.extraSeconds=10;}else if(power==='preview'){payload.nextCategories=NF_CATEGORIES.slice(0,4).map(c=>c.label);}else if(power==='hint'){payload.hint='پاسخ باید با حرف نمایش‌داده‌شده آغاز شود؛ پاسخ کوتاه و روشن وارد کن.';}else if(power==='shield'){payload.protected=true;}else if(power==='bonus'){payload.bonus=5;}
 st.usedPowers=st.usedPowers||{};st.usedPowers[req.user.id]=power;db.prepare('UPDATE nf_matches SET state_json=? WHERE id=?').run(JSON.stringify(st),id);db.prepare('UPDATE nf_players SET power_uses=power_uses+1 WHERE match_id=? AND user_id=?').run(id,req.user.id);db.prepare('INSERT INTO nf_power_events(match_id,user_id,power_id,round_no,payload_json) VALUES(?,?,?,?,?)').run(id,req.user.id,power,m.round_no,JSON.stringify(payload));res.json({ok:true,power,payload});
});
app.get('/api/namefamile/history',requireAuth,(req,res)=>{const results=db.prepare('SELECT r.*,m.created_at,m.finished_at FROM nf_results r JOIN nf_matches m ON m.id=r.match_id WHERE r.user_id=? ORDER BY r.match_id DESC LIMIT 100').all(req.user.id);const stats={played:results.length,wins:results.filter(x=>x.place===1).length,score:results.reduce((a,x)=>a+Number(x.score||0),0),fame:results.reduce((a,x)=>a+Number(x.fame_awarded||0),0),coins:results.reduce((a,x)=>a+Number(x.coins_awarded||0),0)};res.json({results,stats});});
// Read-only live spectators: never expose hidden answers, Connect secret, or active clue word.
app.get('/api/admin/games/active-extra',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const rows=[];for(const g of connectGames.values())if(g.status!=='finished')rows.push({kind:'connect',id:Number(g.id),gameName:'کانکت',status:g.status,players:(g.players||[]).length,createdAt:g.startedAt||g.createdAt||null});
 for(const m of db.prepare("SELECT id,status,created_at,started_at,round_no FROM nf_matches WHERE status IN ('waiting','playing','review') ORDER BY id DESC LIMIT 200").all())rows.push({kind:'namefamile',id:Number(m.id),gameName:'اسم‌وفامیل',status:m.status,players:db.prepare('SELECT COUNT(*) n FROM nf_players WHERE match_id=?').get(m.id).n,createdAt:m.started_at||m.created_at,roundNo:m.round_no});
 rows.sort((a,b)=>String(b.createdAt||'').localeCompare(String(a.createdAt||'')));res.json({rows});
});
app.post('/api/admin/games/active-extra/:kind/:id/close',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const kind=String(req.params.kind||''),id=Number(req.params.id),reason=String(req.body.reason||'').trim().slice(0,500);if(!reason)return res.status(400).json({error:'نوشتن دلیل پایان مسابقه الزامی است'});if(!Number.isInteger(id)||id<1)return res.status(400).json({error:'شناسه بازی نامعتبر است'});
 if(kind==='connect'){const g=connectGames.get(id);if(!g)return res.status(404).json({error:'بازی کانکت پیدا نشد'});connectFinish(g,null,reason);return res.json({ok:true});}
 if(kind==='namefamile'){const m=db.prepare("SELECT * FROM nf_matches WHERE id=? AND status IN ('waiting','playing','review')").get(id);if(!m)return res.status(404).json({error:'مسابقه اسم‌وفامیل فعال پیدا نشد'});db.prepare("UPDATE nf_matches SET status='finished',finished_at=?,round_deadline=NULL,state_json=? WHERE id=?").run(Date.now(),JSON.stringify({adminClosed:true,reason}),id);const players=db.prepare('SELECT user_id FROM nf_players WHERE match_id=?').all(id);for(const player of players)notify(player.user_id,'مسابقه اسم‌وفامیل توسط مدیریت پایان یافت',reason,{action:'namefamile_admin_close',requestId:id});return res.json({ok:true});}
 return res.status(400).json({error:'نوع بازی پشتیبانی نمی‌شود'});
});
app.get('/api/admin/game-history-extra',requireAuth,requirePermission('lobby.view'),(req,res)=>{
 const rows=[];
 for(const g of connectGames.values())rows.push({kind:'connect',id:Number(g.id),gameName:'کانکت',status:g.status==='finished'?'finished':g.status,createdAt:g.startedAt||g.createdAt||null,finishedAt:g.finishedAt||null,players:(g.players||[]).length,winnerName:g.winnerName||'',reason:g.reason||''});
 for(const m of db.prepare("SELECT id,status,created_at,started_at,finished_at,winner_id,round_no FROM nf_matches ORDER BY id DESC LIMIT 300").all())rows.push({kind:'namefamile',id:Number(m.id),gameName:'اسم‌وفامیل',status:m.status,createdAt:m.started_at||m.created_at,finishedAt:m.finished_at||null,players:db.prepare('SELECT COUNT(*) n FROM nf_players WHERE match_id=?').get(m.id).n,winnerId:m.winner_id,roundNo:m.round_no});
 rows.sort((a,b)=>String(b.createdAt||'').localeCompare(String(a.createdAt||'')));res.json({rows:rows.slice(0,300)});
});
app.get('/api/games/live',requireAuth,(req,res)=>{
 const items=[];
 for(const g of connectGames.values()) if(g.status!=='finished') items.push({kind:'connect',id:g.id,gameName:'کانکت',status:g.status==='word'?'در حال انتخاب کلمه':'در حال اجرا',players:g.players.length,joined:g.players.some(p=>Number(p.id)===Number(req.user.id)),createdAt:g.startedAt});
 for(const m of db.prepare("SELECT id,status,created_at,started_at,round_no FROM nf_matches WHERE status IN ('waiting','playing','review') ORDER BY id DESC LIMIT 100").all()) items.push({kind:'namefamile',id:m.id,gameName:'اسم‌فامیل',status:m.status==='waiting'?'در انتظار بازیکن':m.status==='review'?'بررسی پاسخ‌ها':'در حال اجرا',players:db.prepare('SELECT COUNT(*) n FROM nf_players WHERE match_id=?').get(m.id).n,joined:!!db.prepare('SELECT 1 FROM nf_players WHERE match_id=? AND user_id=?').get(m.id,req.user.id),createdAt:m.started_at||m.created_at,roundNo:m.round_no});
 for(const r of db.prepare("SELECT r.id,r.game_type,r.status,r.created_at,r.started_at,(SELECT COUNT(*) FROM game_players p WHERE p.room_id=r.id) players FROM game_rooms r WHERE r.status IN ('waiting','playing') ORDER BY r.id DESC LIMIT 100").all()) items.push({kind:'koodiro',id:r.id,gameName:r.game_type==='advanced'?'کودیرو پیشرفته':'کودیرو کلاسیک',status:r.status==='waiting'?'در انتظار بازیکن':'در حال اجرا',players:r.players,joined:!!db.prepare('SELECT 1 FROM game_players WHERE room_id=? AND user_id=?').get(r.id,req.user.id),createdAt:r.started_at||r.created_at});
 items.sort((a,b)=>String(b.createdAt||'').localeCompare(String(a.createdAt||'')));res.json({games:items});
});
app.get('/api/games/live/:kind/:id',requireAuth,(req,res)=>{
 const id=Number(req.params.id),kind=String(req.params.kind||'');if(!Number.isInteger(id)||id<1)return res.status(400).json({error:'شناسه بازی نامعتبر است'});
 if(kind==='connect'){const g=connectGames.get(id);if(!g)return res.status(404).json({error:'بازی پیدا نشد'});return res.json({game:{id:g.id,kind,status:g.status,startedAt:g.startedAt,endsAt:g.endsAt,players:g.players.map(p=>({name:p.name,avatar:p.avatar,role:p.id===g.mainId?'بازیکن اصلی':'هم‌تیمی'})),prefix:g.prefix,secretLength:g.secret?Array.from(g.secret).length:0,clue:g.proposal?.clue||null,clueAuthor:g.proposal?g.players.find(p=>p.id===g.proposal.authorId)?.name||'بازیکن':null,burnedCount:g.burned.length,lastEvent:g.lastEvent,winnerName:g.winnerName||'',reason:g.reason||''}});}
 if(kind==='namefamile'){const m=db.prepare('SELECT * FROM nf_matches WHERE id=?').get(id);if(!m)return res.status(404).json({error:'بازی پیدا نشد'});const st=nfSafeJson(m.state_json);const players=db.prepare('SELECT p.user_id,u.display_name,u.username,u.avatar FROM nf_players p JOIN users u ON u.id=p.user_id WHERE p.match_id=? ORDER BY p.joined_at').all(id).map(p=>({id:p.user_id,name:p.display_name||p.username,avatar:p.avatar}));const result={id:m.id,kind,status:m.status,createdAt:m.created_at,startedAt:m.started_at,roundNo:m.round_no,roundsTotal:st.roundsTotal||5,letter:st.letter||null,categories:st.categories||[],players,winnerId:m.winner_id};if(m.status==='review'||m.status==='finished'){result.scoreboard=st.scoreboard||{};result.answers=db.prepare('SELECT a.user_id,a.answers_json,a.locked,u.display_name,u.username FROM nf_answers a JOIN users u ON u.id=a.user_id WHERE a.match_id=? AND a.round_no=?').all(id,m.round_no).map(a=>({userId:a.user_id,name:a.display_name||a.username,answers:nfSafeJson(a.answers_json),locked:!!a.locked}));}return res.json({game:result});}
 if(kind==='koodiro'){const room=gameRoomState(id,null);if(!room)return res.status(404).json({error:'بازی پیدا نشد'});const st=room.state||{};return res.json({game:{id,kind,status:room.status,gameType:room.gameType,players:(room.players||[]).map(p=>({id:p.id,name:p.name||p.displayName||'بازیکن',avatar:p.avatar})),state:st,winnerId:room.winnerId}});}
 return res.status(400).json({error:'نوع بازی پشتیبانی نمی‌شود'});
});
app.get('/api/namefamile/rules',requireAuth,(req,res)=>res.json({rules:[{title:'ورود و لابی',text:'هر مسابقه ۲ تا ۸ بازیکن واقعی دارد. لابی حداکثر ۱۲۰ ثانیه منتظر می‌ماند؛ با ورود نفر هشتم شروع فوری است و با پایان زمان، فقط با حداقل دو بازیکن شروع می‌شود.'},{title:'دورها و زمان',text:'مسابقه پیش‌فرض ۵ دور دارد؛ هر دور ۶۰ ثانیه است. ساعت سرور مرجع است و پاسخ پس از قفل‌شدن قابل تغییر نیست.'},{title:'دسته‌ها',text:'در هر یک از ۵ دور، سرور به‌صورت تصادفی ۴ دسته از میان اسم، فامیل، شهر، کشور، غذا، حیوان، اشیا، رنگ، شغل، میوه، گل و گیاه و وسیله نقلیه انتخاب می‌کند. پاسخ باید با حرف همان دور آغاز شود.'},{title:'امتیاز',text:'پاسخ خالی یا با حرف اشتباه صفر امتیاز؛ پاسخ معتبر یکتا ۱۰ امتیاز و پاسخ معتبر تکراری بین بازیکنان ۵ امتیاز دارد. اگر واژه‌نامه برای دسته‌ای مدیریت شده باشد، پاسخ باید در آن باشد.'},{title:'جایزه',text:'برنده با بالاترین امتیاز تعیین می‌شود؛ در تساوی، ورود زودتر به مسابقه اولویت دارد. جایزه برد: ۱۰۰ سکه و ۱۰٬۰۰۰ شهرت، فقط یک بار برای هر نتیجه مسابقه.'},{title:'قطع اتصال و ضدتقلب',text:'پاسخ‌های ذخیره‌شده در سرور باقی می‌مانند و پس از اتصال مجدد بازیابی می‌شوند. امتیاز و جایزه سمت سرور محاسبه می‌شوند؛ ساعت دستگاه تعیین‌کننده نیست.'},{title:'شخصیت‌ها و قدرت‌ها',text:'هر بازیکن در شروع شخصیت تصادفی می‌گیرد و یک‌بار می‌تواند قدرت مختص شخصیت خود را استفاده کند. قدرت‌ها کمکی هستند و جایگزین اعتبارسنجی پاسخ نمی‌شوند.'}]}));
app.get('/api/admin/namefamile/dictionary',requireAuth,requirePermission('lobby.view'),(req,res)=>res.json({words:db.prepare('SELECT * FROM nf_dictionary ORDER BY category,normalized LIMIT 2000').all()}));
app.post('/api/admin/namefamile/dictionary',requireAuth,requirePermission('lobby.view'),(req,res)=>{const category=String(req.body.category||''),answer=String(req.body.answer||'').trim().slice(0,100),normalized=nfNormalize(answer);if(!(NF_CATEGORIES.some(c=>c.id===category)&&((nfSafeJson(m.state_json).categories||NF_CATEGORIES).some(c=>c.id===category)))||!answer||normalized.length<2)return res.status(400).json({error:'دسته یا واژه نامعتبر است'});db.prepare('INSERT INTO nf_dictionary(category,answer,normalized,created_by) VALUES(?,?,?,?) ON CONFLICT(category,normalized) DO UPDATE SET answer=excluded.answer,active=1').run(category,answer,normalized,req.user.id);res.json({ok:true});});
app.delete('/api/admin/namefamile/dictionary/:id',requireAuth,requirePermission('lobby.view'),(req,res)=>{db.prepare('UPDATE nf_dictionary SET active=0 WHERE id=?').run(Number(req.params.id));res.json({ok:true});});
app.get('/api/admin/namefamile/settings',requireAuth,requirePermission('lobby.view'),(req,res)=>res.json({rounds:5,roundSeconds:Math.max(30,Math.min(180,Number(getSetting('nf_round_seconds'))||60)),categories:NF_CATEGORIES}));
app.patch('/api/admin/namefamile/settings',requireAuth,requirePermission('lobby.view'),(req,res)=>{const rounds=Number(req.body.rounds),seconds=Number(req.body.roundSeconds);if(!Number.isInteger(rounds)||rounds<1||rounds>10||!Number.isInteger(seconds)||seconds<30||seconds>180)return res.status(400).json({error:'تعداد دور باید ۱ تا ۱۰ و زمان هر دور ۳۰ تا ۱۸۰ ثانیه باشد'});setSetting('nf_rounds',String(rounds));setSetting('nf_round_seconds',String(seconds));res.json({ok:true,rounds,roundSeconds:seconds});});

app.get('*',(req,res)=>res.sendFile(path.join(ROOT,'public','index.html')));
// The first successful registration becomes the owner; never seed a default password.
server.listen(PORT,'0.0.0.0',()=>console.log(`Dor Dor running at http://127.0.0.1:${PORT}`));

}
main().catch(err => { console.error('Dor Dor startup failed:', err); process.exitCode = 1; });
