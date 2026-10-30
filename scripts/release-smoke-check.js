'use strict';
// Static release guardrails; this is not a substitute for HTTP/integration tests.
const fs = require('fs');
const assert = require('assert');
const server = fs.readFileSync('server.js', 'utf8');
const app = fs.readFileSync('public/app.js', 'utf8');
const css = fs.readFileSync('public/style.css', 'utf8');
const checks = [
  ['group join requests API', /app\.post\('\/api\/groups\/:id\/join'/.test(server) && /app\.post\('\/api\/groups\/:id\/join-requests\/:requestId\/respond'/.test(server)],
  ['group join authorization', /فقط مدیران گروه می‌توانند درخواست را بررسی کنند/.test(server)],
  ['group mute persistence', /INSERT INTO group_user_settings\(group_id,user_id,muted/.test(server)],
  ['group member mute route does not shadow personal mute', (server.match(/app\.post\('\/api\/groups\/:id\/mute'/g)||[]).length===1 && /app\.post\('\/api\/groups\/:id\/members\/:userId\/mute'/.test(server)],
  ['group mode edit persistence', /UPDATE chat_groups SET name=\?,description=\?,avatar=\?,access_mode=\? WHERE id=\?/.test(server)],
  ['admin game history pagination', /LIMIT \$\{limit\} OFFSET \$\{offset\}/.test(server) && /COUNT\(\*\) n FROM game_rooms r/.test(server)],
  ['game history includes departed players', /FROM game_results WHERE room_id=\?/.test(server) && /UNION SELECT user_id,created_at AS joined_at,room_id FROM game_results/.test(server)],
  ['cosmetic coin purchase path', /const currency=item\.kind==='frame'\?'coins':'diamonds'/.test(server)],
  ['daily fame reward uses coins at 21 Iran time', /activity-coins:'\+today/.test(server) && /daily_activity_coin_distributions/.test(server) && /daily_activity_coins/.test(server)],
  ['APK upload endpoint', /app\.post\('\/api\/admin\/app-release'/.test(server)],
  ['APK download endpoint', /app\.get\('\/api\/app-release\/:id\/download'/.test(server)],
  ['install page link', /data-store-tab="cosmetics"/.test(app) && /نصب اپلیکیشن/.test(app)],
  ['group invite resumes after login', /async function resumeInitialGroupLink\(\)/.test(app) && /if\(initialGroupPath\)await resumeInitialGroupLink\(\)/.test(app)],
  ['hash routes survive refresh and support requested aliases', /async function resumeInitialHashRoute\(\)/.test(app) && /if\(initialHashRoute\)await resumeInitialHashRoute\(\)/.test(app) && /if\(h==='shop'\)return \{view:'store'\}/.test(app) && /if\(h==='groups'\)return \{view:'chats',groupTab:true\}/.test(app)],
  ['admin cosmetic picker shows frame coin currency', /i\.kind==='frame'\?'🪙':'💎'/.test(app)],
  ['all catalog frame palettes styled', ['neon','gold','ice','rose','ruby','emerald','rainbow','cosmic','plasma','aurora','sakura','sunset','ocean','royal','chrome','fire','mint','violet','opal','galaxy'].every(x=>css.includes('avatar-frame-'+x))],
  ['CONNECT player can explicitly leave an active match', /app\.post\('\/api\/connect\/game\/:id\/leave'/.test(server) && /function leaveGame\(\)/.test(fs.readFileSync('public/connect-game.js','utf8')) && /data-connect-leave/.test(fs.readFileSync('public/connect-game.js','utf8'))],
  ['CONNECT hides home navigation during active match', /connect-mode\.connect-in-match \.bottomnav\{display:none!important\}/.test(css) && /connect-mode\.connect-in-match \.topbar \.back-btn\{display:none!important\}/.test(css)],
  ['private chat composer controls remain visible on mobile', /chat-compose>textarea#messageText\{display:block!important/.test(css) && /chat-compose>label\.iconbtn\{display:grid!important/.test(css) && /chat-compose>\.voice-coming-soon\{display:inline-flex!important/.test(css)],
];
for (const [name, ok] of checks) {
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}`);
  assert.ok(ok, `Static check failed: ${name}`);
}
console.log(`\n${checks.length}/${checks.length} static checks passed. No live database or HTTP integration is exercised by this script.`);
