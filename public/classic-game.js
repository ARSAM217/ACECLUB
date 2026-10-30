/* ACE CLUB — بازی کلاسیک (کوریور) */
(function(){
  const fa=n=>Number(n||0).toLocaleString('fa-IR');
  const icon=n=>`<img class="classic-icon" src="/assets/ace-ui/${n}.svg" alt="">`;
  let classicPoll=null, classicChatPoll=null, classicChatSeen=0;
  const clearTimers=()=>{if(classicPoll){clearInterval(classicPoll);classicPoll=null}if(classicChatPoll){clearInterval(classicChatPoll);classicChatPoll=null}};
  const guideText=`<h2>راهنمای بازی</h2><button class="classic-guide-close" data-classic-guide-close>×</button><section><h3>هدف</h3><p>مهره ی آبی رنگت از پایین شروع میکنه و باید بره بالا مهره ی قرمز رنگ حریف از بالا شروع میکنه و باید بیاد پایین تو بازی حرفه ای (۲) به (۲) اولین هم تیمی ای که به خط پایان برسه تیمش برنده میشه.</p></section><section><h3>حرکت</h3><p>هر نوبت مهره ات رو به یکی از خونههای اطرافت در چهار جهت اصلی بالا پایین چپ یا راست حرکت بده یا از روی حریف بپر.</p><p>تو بازی حرفه ای (۲) به (۲) هم تیمیها میتونن از روی هم رد بشن.</p></section><section><h3>دیوار</h3><p>تو نوبت خودت میتونی بجای حرکت کردن به دیوار افقی یا عمودی به اندازه ۲ خونه بذاری دیوارها نباید همه ی راه های رسیدن به هدف رو ببندن.</p></section><section><h3>مدها</h3><p>مد کلاسیک برای بازی تک نفره مناسبه تخته ۹ در ۹ داره و هر بازیکن ۱۰ دیوار داره</p><p>مد حرفه ای برای بازی تیمی ۲ به ۲ ساخته شده تخته ۱۱ در ۱۱ داره و ۲۰ دیوار بصورت مشترک در اختیار دو بازیکن قرار میگیره</p></section><section><h3>با دوستات بازی کن</h3><p>تو قسمت بازی با دوستان میتونی تا ۳ دوست رو به بازی دعوت کنی قبل از شروع بازی مشخص کن کی هم تیمیه و کی حریف</p></section>`;
  function gameGuide(){
    document.querySelector('.classic-guide-overlay')?.remove();
    const layer=document.createElement('div');layer.className='classic-guide-overlay';
    layer.innerHTML=`<section class="classic-guide-sheet">${guideText}</section>`;
    layer.addEventListener('click',e=>{if(e.target===layer||e.target.closest('[data-classic-guide-close]'))layer.remove()});document.body.append(layer);
  }
  function gameSettings(){
    document.querySelector('.classic-soon-overlay')?.remove();
    const layer=document.createElement('div');layer.className='classic-soon-overlay';layer.innerHTML=`<section class="classic-soon-sheet">${icon('game-settings')}<h3>تنظیمات بازی</h3><b>آپدیت به‌زودی</b><p>تنظیمات اختصاصی بازی در بروزرسانی بعدی فعال می‌شود.</p><button class="btn" data-classic-soon-close>متوجه شدم</button></section>`;
    layer.addEventListener('click',e=>{if(e.target===layer||e.target.closest('[data-classic-soon-close]'))layer.remove()});document.body.append(layer);
  }
  function gameReport(){
    document.querySelector('.classic-report-overlay')?.remove();
    const layer=document.createElement('div');layer.className='classic-report-overlay';
    layer.innerHTML=`<section class="classic-report-sheet"><button class="classic-report-close" data-report-close>×</button>${icon('game-report')}<h3>گزارش بازی</h3><p class="muted">موضوع گزارش را انتخاب کن.</p><div class="classic-report-options"><button data-game-report="gameplay"><span>⚖️</span><b>گزارش نحوه بازی</b><small>مشکل یا رفتار نادرست در روند بازی</small></button><button data-game-report="inappropriate"><span>💬</span><b>پیام نامناسب در بازی</b><small>گزارش پیام توهین‌آمیز یا نامناسب</small></button><button data-game-report="cheating"><span>🛡️</span><b>تقلب در بازی</b><small>مشکوک به استفاده از تقلب یا سوءاستفاده</small></button></div><div class="classic-report-form" hidden><b id="classicReportTitle">گزارش</b><textarea id="classicReportDetails" maxlength="1000" placeholder="توضیحات (اختیاری)..."></textarea><div class="classic-report-form-actions"><button class="btn secondary" data-report-back>بازگشت</button><button class="btn" data-report-submit>ارسال گزارش</button></div></div></section>`;
    let selected='';
    layer.addEventListener('click',async e=>{
      if(e.target===layer||e.target.closest('[data-report-close]')){layer.remove();return}
      const option=e.target.closest('[data-game-report]');
      if(option){selected=option.dataset.gameReport;const labels={gameplay:'گزارش نحوه بازی',inappropriate:'پیام نامناسب در بازی',cheating:'تقلب در بازی'};$('#classicReportTitle').textContent=labels[selected];layer.querySelector('.classic-report-options').hidden=true;layer.querySelector('.classic-report-form').hidden=false;layer.querySelector('#classicReportDetails')?.focus();return}
      if(e.target.closest('[data-report-back]')){layer.querySelector('.classic-report-options').hidden=false;layer.querySelector('.classic-report-form').hidden=true;return}
      if(e.target.closest('[data-report-submit]')){const details=layer.querySelector('#classicReportDetails')?.value?.trim()||'';try{await api(`/games/room/${window.activeGameRoom}/report`,{method:'POST',body:{kind:selected,details}});layer.remove();toast('گزارش بازی ثبت شد')}catch(err){toast(err.message)}}
    });
    document.body.append(layer);
  }
  function gameComingSoon(title,iconName){return `<button class="classic-action-card classic-disabled" type="button" disabled>${icon(iconName)}<span><b>${title}</b><small>آپدیت به‌زودی</small></span><em>به‌زودی</em></button>`}
  function gameHubCard(slug,title,desc,tag,cls=''){
    return `<button class="ace-game-hub-card ${cls}" data-hub-open="${slug}"><span class="ace-hub-logo"><img src="/assets/ace-ui/game-${slug}.svg" alt="لوگوی ${title}"></span><span class="ace-hub-copy"><b>${title}</b><small>${desc}</small></span><em>${tag} ←</em></button>`;
  }
  window.gamesScreen=async function(){
    clearTimers();view='games';document.body.classList.remove('namefamile-mode','classic-game-mode','connect-mode');
    shell(`${topbar('بازی‌ها')}<main class="content ace-game-hub-page">
      <section class="ace-game-hub-hero"><img src="/assets/ace-ui/game-hub.svg" alt="لوگوی بازی‌های ACE CLUB"><div><span class="classic-update-badge">مرکز بازی‌ها</span><h1>بازی موردعلاقه‌ات رو انتخاب کن</h1><p>هر بازی بخش اختصاصی، قوانین و رقابت آنلاین خودش را دارد.</p></div></section>
      <section class="ace-game-hub-list">
        ${gameHubCard('namefamile','اسم‌فامیل','رقابت کلمه‌ای آنلاین · ۲ تا ۸ بازیکن · جایزه ویژه','ورود به بازی','ace-hub-namefamile')}
        ${gameHubCard('koodiro','کودیرو','تمام بازی‌های تخته‌ای و رقابتی کودیرو در یک بخش','مشاهده بازی‌ها','ace-hub-koodiro')}
        ${gameHubCard('connect','کانکت','یک بازیکن در برابر همه · حدس کلمه · ۳ تا ۱۴ نفر','شروع سریع','ace-hub-connect')}
        ${gameHubCard('falman','بازی‌های من','بازی‌هایی که اکنون در آن‌ها عضو مسابقه هستی','بازی‌های فعال','ace-hub-falman')}
      </section>
      <div class="ace-hub-note"><img src="/assets/ace-ui/game-safe.svg" alt=""><span>پیشرفت و جوایز بازی‌ها به حساب ACE CLUB متصل می‌ماند.</span></div>
    </main>`);
    document.querySelectorAll('[data-hub-open]').forEach(btn=>btn.addEventListener('click',()=>{if(btn.dataset.hubOpen==='falman')return window.myActiveGamesScreen?.();api('/games/live').then(d=>{if((d.games||[]).some(g=>g.joined)){toast('ابتدا از بازی فعلی خارج شو، سپس بازی دیگری انتخاب کن');return window.myActiveGamesScreen?.()}if(btn.dataset.hubOpen==='namefamile')return window.nameFamileScreen?.();if(btn.dataset.hubOpen==='koodiro')return window.koodiroGamesScreen?.();if(btn.dataset.hubOpen==='connect')return window.connectGameScreen?.()}).catch(err=>toast(err.message))}));
  };
  window.myActiveGamesScreen=async function(){
    clearTimers();view='games';document.body.classList.remove('namefamile-mode','classic-game-mode','connect-mode');
    shell(`${topbar('بازی‌های من')}<main class="content ace-game-hub-page"><section class="ace-game-hub-hero"><img src="/assets/ace-ui/game-hub.svg" alt=""><div><span class="classic-update-badge">بازی‌های من</span><h1>بازی‌های فعال من</h1><p>مسابقه‌هایی که در حال حاضر عضو آن‌ها هستی؛ از همین‌جا دوباره وارد بازی شو.</p></div></section><div class="card"><button class="btn secondary block" id="myGamesRefresh">به‌روزرسانی فهرست</button><div id="myActiveGamesList" class="falman-live-list"><div class="empty">در حال دریافت بازی‌های تو…</div></div></div><button class="btn secondary block" data-my-games-back>بازگشت به مرکز بازی‌ها</button></main>`);
    const load=async()=>{const box=$('#myActiveGamesList');if(!box)return;box.innerHTML='<div class="empty">در حال دریافت بازی‌های تو…</div>';try{const d=await api('/games/live');const games=(Array.isArray(d.games)?d.games:[]).filter(g=>g.joined);box.innerHTML=games.map(g=>`<article class="adminrow falman-live-row"><div class="grow"><b>🎮 ${esc(g.gameName||g.kind||'بازی')} · #${Number(g.id)}</b><div class="muted small">${esc(g.status||'در حال اجرا')} · ${Number(g.players||0).toLocaleString('fa-IR')} بازیکن</div></div><button class="btn" data-my-game-enter="${esc(g.kind)}" data-my-game-id="${Number(g.id)}">ورود به بازی</button></article>`).join('')||'<div class="empty">در حال حاضر در هیچ مسابقهٔ فعالی عضو نیستی.</div>';box.querySelectorAll('[data-my-game-enter]').forEach(b=>b.onclick=async()=>{const kind=b.dataset.myGameEnter,id=Number(b.dataset.myGameId);try{if(kind==='connect')return window.connectOpenGame?.(id);if(kind==='namefamile')return window.nameFamileResumeMatch?.(id);if(kind==='koodiro'){await api('/games/room/'+id);window.activeGameRoom=id;view='gameRoom';return classicGameRoomScreen(id)}toast('ورود مستقیم به این نوع بازی در دسترس نیست.')}catch(err){toast(err.message)}})}catch(err){box.innerHTML=`<div class="empty">${esc(err.message||'بارگذاری بازی‌ها ناموفق بود')}</div>`}};
    $('#myGamesRefresh').onclick=()=>load().catch(err=>toast(err.message));document.querySelectorAll('[data-my-games-back]').forEach(b=>b.onclick=()=>gamesScreen());await load();
  };
  window.falManScreen=window.myActiveGamesScreen;
  window.koodiroGamesScreen=async function(){
    clearTimers();view='games';document.body.classList.remove('namefamile-mode');
    shell(`${topbar('کودیرو')}<main class="content ace-koodiro-page">
      <section class="ace-game-hub-hero koodiro-hero"><img src="/assets/ace-ui/game-koodiro.svg" alt="لوگوی اختصاصی کودیرو"><div><span class="classic-update-badge">بخش کودیرو</span><h1>بازی‌های کودیرو</h1><p>بازی‌های تخته‌ای و رقابت‌های چندنفره را از اینجا انتخاب کن.</p></div></section>
      <section class="classic-game-list">
        <h2 class="classic-mode-heading">♟️ بازی‌های کودیرو</h2>
        <button class="classic-action-card classic-active" data-classic-start><span class="classic-action-logo"><img src="/assets/ace-ui/game-classic.svg" alt="لوگوی بازی کلاسیک"></span><span><b>کودیرو کلاسیک</b><small>۱ به ۱ آنلاین · تخته ۹×۹ · ۱۰ دیوار</small></span><em>شروع بازی ←</em></button>
        <button class="classic-action-card classic-active" data-advanced-start><span class="classic-action-logo"><img src="/assets/ace-ui/game-advanced.svg" alt="لوگوی بازی پیشرفته"></span><span><b>کودیرو پیشرفته</b><small>۴ نفره · تخته ۱۲×۱۲ · ۲۰ دیوار مشترک · تا ۳ برد دست</small></span><em>شروع بازی ←</em></button>
        <button class="classic-action-card" data-classic-guide><span class="classic-action-logo"><img src="/assets/ace-ui/game-guide.svg" alt="لوگوی راهنما"></span><span><b>راهنمای کودیرو</b><small>قوانین و روش بازی</small></span><em>مشاهده ←</em></button>
        <button class="classic-action-card" data-koodiro-back><span class="classic-action-logo"><img src="/assets/ace-ui/game-back.svg" alt="بازگشت"></span><span><b>بازگشت به انتخاب بازی</b><small>اسم‌فامیل یا کودیرو</small></span><em>بازگشت ←</em></button>
      </section>
    </main>`);
    $('[data-classic-start]')?.addEventListener('click',async()=>{try{const d=await api('/games/join',{method:'POST',body:{gameType:'classic'}});window.activeGameRoom=d.room.id;viewStack.push('games');view='gameRoom';await classicGameRoomScreen(d.room.id)}catch(err){toast(err.message)}});
    $('[data-advanced-start]')?.addEventListener('click',async()=>{try{const d=await api('/games/join',{method:'POST',body:{gameType:'advanced'}});window.activeGameRoom=d.room.id;viewStack.push('games');view='gameRoom';await classicGameRoomScreen(d.room.id)}catch(err){toast(err.message)}});
    $('[data-classic-guide]')?.addEventListener('click',gameGuide);
    $('[data-koodiro-back]')?.addEventListener('click',()=>gamesScreen());
  };
  window.liveGamesScreen=async function(){
    if(window.aceLiveSpectatorTimer){clearInterval(window.aceLiveSpectatorTimer);window.aceLiveSpectatorTimer=null;}
    const draw=async()=>{const d=await api('/games/live');const names={connect:'کانکت',namefamile:'اسم‌فامیل',koodiro:'کودیرو'};
      shell(`${topbar('پخش زنده بازی‌ها')}<main class="content"><button class="btn secondary" data-live-back>← بازگشت به بازی‌ها</button><div class="card"><h2>📡 بازی‌های در حال اجرا</h2><p class="muted small">تماشاگر فقط وضعیت و جریان مجاز بازی را می‌بیند؛ نمی‌تواند حرکت کند، پاسخ ثبت کند یا روی نتیجه اثر بگذارد.</p><div class="admin-game-filters"><input id="liveGameSearch" class="field" placeholder="جستجوی نام بازی…"><select id="liveGameType" class="field"><option value="">همه بازی‌ها</option><option value="connect">کانکت</option><option value="namefamile">اسم‌فامیل</option><option value="koodiro">کودیرو</option></select></div><div id="liveGameList">${(d.games||[]).map(g=>`<div class="adminrow live-game-row" data-kind="${esc(g.kind)}" data-name="${esc(g.gameName)}"><div class="grow"><b>🔴 ${esc(g.gameName)} · #${g.id}</b><div class="muted small">${esc(g.status)} · ${fa(g.players)} بازیکن · ${esc(g.createdAt||'')}</div></div><button class="btn" data-watch-kind="${esc(g.kind)}" data-watch-id="${g.id}">تماشای زنده</button></div>`).join('')||'<div class="empty">الان بازی عمومیِ در حال اجرا پیدا نشد. این صفحه را بعداً دوباره باز کن.</div>'}</div></div></main>`);
      $('#liveGameSearch').oninput=filter;$('#liveGameType').onchange=filter;function filter(){const q=$('#liveGameSearch').value.trim().toLowerCase(),type=$('#liveGameType').value;document.querySelectorAll('.live-game-row').forEach(r=>r.style.display=(!q||r.dataset.name.toLowerCase().includes(q))&&(!type||r.dataset.kind===type)?'':'none')}
      $('[data-live-back]')?.addEventListener('click',()=>{clearInterval(window.aceLiveSpectatorTimer);window.aceLiveSpectatorTimer=null;gamesScreen()});document.querySelectorAll('[data-watch-kind]').forEach(b=>b.onclick=()=>window.watchLiveGame?.(b.dataset.watchKind,Number(b.dataset.watchId)));
    };try{await draw();window.aceLiveSpectatorTimer=setInterval(()=>{if(view==='games')draw().catch(()=>{})},5000)}catch(err){toast(err.message)}
  };
  window.watchLiveGame=async function(kind,id){if(window.aceLiveSpectatorTimer){clearInterval(window.aceLiveSpectatorTimer);window.aceLiveSpectatorTimer=null;}let stopped=false;const draw=async()=>{if(stopped)return;const d=await api(`/games/live/${encodeURIComponent(kind)}/${id}`),g=d.game;let body='';
      if(kind==='connect'){body=`<p><b>مرحله:</b> ${esc(g.status==='word'?'انتخاب کلمه':g.status==='playing'?'در حال اجرا':'پایان‌یافته')}</p><p><b>حروف آشکارشده:</b> ${esc(g.prefix||'هنوز انتخاب نشده')} · طول کلمه: ${fa(g.secretLength||0)}</p><p><b>راهنمای فعلی:</b> ${esc(g.clue||'در حال آماده‌سازی راهنما')}</p><p class="muted small">کلمه مخفی و کلمه راهنمای قابل حدس عمداً به تماشاگر نشان داده نمی‌شود تا تقلب ممکن نباشد.</p>`;}
      else if(kind==='namefamile'){body=`<p><b>دور:</b> ${fa(g.roundNo||0)} از ${fa(g.roundsTotal||5)} · <b>حرف:</b> ${esc(g.letter||'—')}</p><p><b>دسته‌ها:</b> ${(g.categories||[]).map(c=>esc(c.label||c.id)).join('، ')||'در انتظار شروع دور'}</p>${g.answers?`<h3>پاسخ‌های ثبت‌شده (دور بررسی/پایان)</h3>${g.answers.map(a=>`<div class="adminrow"><div class="grow"><b>${esc(a.name)}</b><div class="muted small">${Object.entries(a.answers||{}).map(([k,v])=>`${esc(k)}: ${esc(v)}`).join(' · ')}</div></div></div>`).join('')}`:'<p class="muted small">پاسخ‌های بازیکنان تا مرحله بررسی/پایان مخفی هستند.</p>'}`;}
      else{const st=g.state||{};body=`<p><b>نوع:</b> ${esc(g.gameType==='advanced'?'پیشرفته':'کلاسیک')}</p><p class="muted small">نمایش وضعیت تخته و بازیکنان؛ کنترل‌های بازی برای تماشاگر غیرفعال است.</p><pre class="live-game-state">${esc(JSON.stringify(st,null,2).slice(0,7000))}</pre>`;}
      shell(`${topbar('تماشای زنده · '+(kind==='connect'?'کانکت':kind==='namefamile'?'اسم‌فامیل':'کودیرو'))}<main class="content"><button class="btn secondary" data-watch-back>← فهرست بازی‌ها</button><div class="card"><div class="classic-update-badge">🔴 تماشاگر · فقط مشاهده</div><h2>${esc(kind==='connect'?'کانکت':kind==='namefamile'?'اسم‌فامیل':'کودیرو')} #${id}</h2><div class="adminrow"><div class="grow"><b>بازیکنان</b><div class="muted small">${(g.players||[]).map(p=>esc(p.name||p.display_name||'بازیکن')).join('، ')||'—'}</div></div><span class="pill">${esc(g.status||'')}</span></div>${body}<button class="btn block" data-watch-back>بازگشت به فهرست بازی‌ها</button></div></main>`);
      document.querySelectorAll('[data-watch-back]').forEach(b=>b.onclick=()=>{stopped=true;window.liveGamesScreen?.()});};try{await draw();window.aceLiveSpectatorTimer=setInterval(()=>{if(view==='games')draw().catch(()=>{})},2000)}catch(err){toast(err.message);window.liveGamesScreen?.()}
  };
  function playersMap(room){return room.players||[]}
  function stateOf(room){return room.state||{}}
  function wallKey(w){return `${w.o[0]}:${w.r}:${w.c}`}
  function hasWall(state,o,r,c){return (state.walls||[]).some(w=>w.o===o&&Number(w.r)===r&&Number(w.c)===c)}
  function moveBlocked(state,r1,c1,r2,c2){const dr=r2-r1,dc=c2-c1,w=state.walls||[],h=(r,c)=>w.some(x=>x.o==='h'&&Number(x.r)===r&&Number(x.c)===c),v=(r,c)=>w.some(x=>x.o==='v'&&Number(x.r)===r&&Number(x.c)===c);if(dr===-1)return h(r2,c2)||h(r2,c2-1);if(dr===1)return h(r1,c1)||h(r1,c1-1);if(dc===-1)return v(r2,c2)||v(r2-1,c2);if(dc===1)return v(r1,c1)||v(r1-1,c1);return true;}
  function legalMoves(room){
    const st=stateOf(room),meP=room.players.find(p=>Number(p.user_id)===Number(me.id)),opp=room.players.find(p=>Number(p.user_id)!==Number(me.id));if(!meP)return [];
    const out=[];const n=room.gameType==='advanced'?12:9;const r=Number(meP.position?.r??(n-1)),c=Number(meP.position?.c??4);const dirs=[[-1,0],[1,0],[0,-1],[0,1]];
    if(room.gameType==='advanced'){for(const [dr,dc] of dirs){const nr=r+dr,nc=c+dc;if(nr<0||nr>=n||nc<0||nc>=n||moveBlocked(st,r,c,nr,nc))continue;const occupant=room.players.find(p=>Number(p.user_id)!==Number(me.id)&&+p.position?.r===nr&&+p.position?.c===nc);if(!occupant)out.push([nr,nc]);else if(Number(occupant.team)===Number(meP.team)){const jr=nr+dr,jc=nc+dc;if(jr>=0&&jr<n&&jc>=0&&jc<n&&!moveBlocked(st,nr,nc,jr,jc)&&!room.players.some(p=>+p.position?.r===jr&&+p.position?.c===jc))out.push([jr,jc]);}}return out;}
    for(const [dr,dc] of dirs){let nr=r+dr,nc=c+dc;if(nr<0||nr>=n||nc<0||nc>=n||moveBlocked(st,r,c,nr,nc))continue;
      if(opp&&Number(opp.position?.r)===nr&&Number(opp.position?.c)===nc){const jr=nr+dr,jc=nc+dc;if(jr>=0&&jr<n&&jc>=0&&jc<n&&!moveBlocked(st,nr,nc,jr,jc)){out.push([jr,jc]);continue}
        for(const [sr,sc] of [[-dc,dr],[dc,-dr]]){const sr0=nr+sr,sc0=nc+sc;if(sr0>=0&&sr0<n&&sc0>=0&&sc0<n&&!moveBlocked(st,nr,nc,sr0,sc0))out.push([sr0,sc0]);}
      } else out.push([nr,nc]);
    }return out;
  }
  function reachesGoal(st,start,goal){
    const q=[start],seen=new Set([start.join(',')]);while(q.length){const [r,c]=q.shift();if(r===goal)return true;for(const [dr,dc] of [[-1,0],[1,0],[0,-1],[0,1]]){const nr=r+dr,nc=c+dc;const n=st?.players&&Object.keys(st.players).length>2?12:9;if(nr<0||nr>=n||nc<0||nc>=n||moveBlocked(st,r,c,nr,nc))continue;const k=`${nr},${nc}`;if(!seen.has(k)){seen.add(k);q.push([nr,nc])}}}return false;
  }
  function clientWallLegal(room,o,r,c){
    const st=stateOf(room);const n=room.gameType==='advanced'?12:9;if(r<0||r>n-2||c<0||c>n-2)return false;
    if(hasWall(st,o,r,c))return false;
    // دیوارهای هم‌جهت نباید روی هم بیفتند و دو دیوار عمود نباید از وسط هم عبور کنند.
    if(o==='h' && (hasWall(st,'h',r,c-1)||hasWall(st,'h',r,c+1)||hasWall(st,'v',r,c)))return false;
    if(o==='v' && (hasWall(st,'v',r-1,c)||hasWall(st,'v',r+1,c)||hasWall(st,'h',r,c)))return false;
    const walls=[...(st.walls||[]),{o,r,c}];const test={...st,walls};const ps=room.players;
    for(const p of ps){const pr=Number(p.position?.r??(p===ps[0]?n-1:0)),pc=Number(p.position?.c??4),goal=p.goal==='top'?0:n-1;if(!reachesGoalSized(test,[pr,pc],goal,n))return false;}
    return true;
  }
  function reachesGoalSized(st,start,goal,n){const q=[start],seen=new Set([start.join(',')]);while(q.length){const [r,c]=q.shift();if(r===goal)return true;for(const [dr,dc] of [[-1,0],[1,0],[0,-1],[0,1]]){const nr=r+dr,nc=c+dc;if(nr<0||nr>=n||nc<0||nc>=n||moveBlocked(st,r,c,nr,nc))continue;const k=`${nr},${nc}`;if(!seen.has(k)){seen.add(k);q.push([nr,nc]);}}}return false;}
  function wallSlots(room){
    const n=room.gameType==='advanced'?12:9,slots=[];for(let r=0;r<n-1;r++)for(let c=0;c<n-1;c++){if(clientWallLegal(room,'h',r,c))slots.push(`h:${r}:${c}`);if(clientWallLegal(room,'v',r,c))slots.push(`v:${r}:${c}`)}return slots;
  }
  function boardHtml(room,wallMode){
    const st=stateOf(room),players=playersMap(room),lm=new Set(legalMoves(room).map(x=>x.join(','))),orientation=window.classicWallOrientation||'h',n=room.gameType==='advanced'?12:9,tracks=n*2-1;
    const legalWallKeys=wallMode?new Set(wallSlots(room).filter(k=>k.startsWith(orientation+':'))):new Set();
    let cells='';
    for(let r=0;r<n;r++)for(let c=0;c<n;c++){
      const p=players.find(x=>Number(x.position?.r)===r&&Number(x.position?.c)===c),goal=r===0||r===n-1,colors={red:'#ff607b',blue:'#35aaff',green:'#38e0a0',yellow:'#ffd34d'},color=p?(p.color||((Number(p.user_id)===Number(me.id))?'blue':'red')):'';
      const cls=['classic-cell',goal?'goal-cell':'',p?'occupied':'',p&&Number(p.user_id)===Number(me.id)?'blue-piece':'',lm.has(`${r},${c}`)?'move-target':''].filter(Boolean).join(' ');
      cells+=`<button class="${cls}" data-classic-cell="${r}:${c}" style="grid-column:${c*2+1};grid-row:${r*2+1};" aria-label="خانه ${fa(r+1)} ${fa(c+1)}">${p?`<span class="classic-piece" ${p.isBot?'':`data-profile="${p.user_id}" title="مشاهده پروفایل"`} style="background:${colors[color]||colors.blue};color:${colors[color]||colors.blue}"></span>`:''}</button>`;
    }
    let walls='';
    for(const w of (st.walls||[])){
      const o=w.o,r=Number(w.r),c=Number(w.c);
      if(o==='h') walls+=`<span class="classic-wall-visual horizontal" style="grid-column:${c*2+1} / span 3;grid-row:${r*2+2};" aria-hidden="true"></span>`;
      else walls+=`<span class="classic-wall-visual vertical" style="grid-column:${c*2+2};grid-row:${r*2+1} / span 3;" aria-hidden="true"></span>`;
    }
    if(wallMode){
      for(const key of legalWallKeys){const [o,r,c]=key.split(':');const pos=o==='h'?`grid-column:${Number(c)*2+1} / span 3;grid-row:${Number(r)*2+2};`:`grid-column:${Number(c)*2+2};grid-row:${Number(r)*2+1} / span 3;`;walls+=`<button class="classic-wall-target ${o==='h'?'horizontal':'vertical'}" data-classic-wall="${o}:${r}:${c}" style="${pos}" aria-label="جای مجاز دیوار"></button>`;}
    }
    return `<div class="classic-board-frame"><div class="classic-board-grid" style="grid-template-columns:repeat(${tracks},minmax(0,1fr));grid-template-rows:repeat(${tracks},minmax(0,1fr));">${cells}<div class="classic-wall-layer" style="grid-template-columns:repeat(${tracks},minmax(0,1fr));grid-template-rows:repeat(${tracks},minmax(0,1fr));">${walls}</div></div></div>`;
  }

  function classicRoomMarkup(room){
    const st=stateOf(room),players=playersMap(room),mine=players.find(p=>Number(p.user_id)===Number(me.id)),other=players.find(p=>Number(p.user_id)!==Number(me.id));
    if(room.status==='waiting'){if(room.gameType==='advanced'){const slots=Array.from({length:4},(_,i)=>players[i]);return `<section class="classic-waiting-page"><header class="classic-match-head">${icon('game-settings')}<div><span class="classic-update-badge">۲ به ۲</span><h2>در انتظار ۴ بازیکن...</h2><small>اگر تا ۶۰ ثانیه بازیکن کافی پیدا نشود، ربات‌های حرفه‌ای جای خالی را پر می‌کنند.</small></div></header><div class="classic-waiting-art">${icon('game-classic')}</div><div class="classic-advanced-wait-slots">${slots.map((p,i)=>`<div class="classic-player-slot"><div class="classic-empty-avatar" style="border-color:${['#ff607b','#35aaff','#38e0a0','#ffd34d'][i]}">${p?avatar(p):'?'}</div><b ${p&&!p.isBot?`data-profile="${p.user_id}"`:''} class="${p?.equipped_name_color?'ace-name-'+p.equipped_name_color:''}">${p?esc(Number(p.user_id)===Number(me.id)?'تو':p.display_name||p.username):'جایگاه آزاد'}</b><small>${p?'آماده':'در انتظار بازیکن'}</small></div>`).join('')}</div><div class="classic-wait-note">یارها تصادفی تعیین می‌شوند؛ بازی تا رسیدن یک تیم به ۳ برد ادامه دارد؛ بعد از هر دست صفحه از نو چیده می‌شود و بردهای تیم‌ها ثبت می‌ماند.</div><button class="btn secondary block" data-classic-leave-wait>خروج از اتاق انتظار</button></section>`}return `<section class="classic-waiting-page"><header class="classic-match-head">${icon('game-classic')}<div><span class="classic-update-badge">آپدیت شد</span><h2>در انتظار حریف...</h2><small>اگر تا ۶۰ ثانیه حریف پیدا نشود، لابی خودکار بسته می‌شود.</small></div></header><div class="classic-waiting-art">${icon('game-classic')}</div><div class="classic-vs-card"><div class="classic-player-slot blue-slot">${avatar(mine||me)}<b>${esc((mine||me).display_name||'تو')}</b><small>بازیکن ۱ · آماده</small></div><div class="classic-vs-logo">VS</div><div class="classic-player-slot red-slot">${other?avatar(other):'<span class="classic-empty-avatar">?</span>'}<b>${other?esc(other.display_name):'در انتظار حریف'}</b><small>${other?'بازیکن ۲ · آماده':'هنوز وارد نشده'}</small></div></div><div class="classic-wait-note">${icon('game-classic')} <span>با ورود بازیکن دوم، بازی بدون تأخیر آغاز می‌شود.</span></div><button class="btn secondary block" data-classic-leave-wait>خروج از اتاق انتظار</button></section>`;}
    const turn=Number(st.turn)===Number(me.id),left=Math.max(0,15000-(Date.now()-Number(st.turnStartedAt||Date.now()))),secs=Math.ceil(left/1000),wallMode=window.classicWallMode===true,meWalls=Number(mine?.walls??(room.gameType==='advanced'?20:10)),oppWalls=Number(other?.walls??(room.gameType==='advanced'?20:10));
    return `<section class="classic-game-page">${room.gameType==='advanced'?`<div class="classic-instruction">تیم قرمز/آبی در برابر سبز/زرد · دست ${fa(st.round||1)} · هدف ۳ برد · بردها: ${fa(st.roundWins?.[0]||0)} - ${fa(st.roundWins?.[1]||0)}</div>`:''}<header class="classic-game-top"><div class="classic-top-actions"><button class="classic-top-btn" data-classic-guide>${icon('game-guide')}<span>راهنما</span></button><button class="classic-top-btn" data-classic-settings>${icon('game-settings')}<span>تنظیمات</span><small>آپدیت به‌زودی</small></button></div><div><div class="classic-turn-title ${turn?'your-turn':''}">${turn?'نوبت تو':'نوبت حریف'}<span class="classic-timer">${fa(secs)}</span></div><button class="classic-report-link" data-classic-report>${icon('game-report')} گزارش بازی</button></div></header><div class="classic-board-wrap">${boardHtml(room,wallMode)}</div><div class="classic-instruction">${turn?(wallMode?'برای گذاشتن دیوار، جای روشن را لمس کن.':'مهره را حرکت بده یا حالت دیوار را انتخاب کن.'):'صبر کن تا نوبت حریف تمام شود.'}</div><div class="classic-actions"><button class="classic-wall-choice ${wallMode&&window.classicWallOrientation==='v'?'active':''}" data-classic-wall-mode data-orientation="v" ${!turn||meWalls<=0?'disabled':''}>${icon('game-wall-v')}<span>دیوار عمودی</span><small>${fa(meWalls)} دیوار</small></button><button class="classic-wall-choice ${wallMode&&window.classicWallOrientation==='h'?'active':''}" data-classic-wall-mode data-orientation="h" ${!turn||meWalls<=0?'disabled':''}>${icon('game-wall-h')}<span>دیوار افقی</span><small>${fa(meWalls)} دیوار</small></button><button class="classic-wall-choice ${!wallMode?'active':''}" data-classic-move-mode ${!turn?'disabled':''}>${icon('game-classic')}<span>حرکت مهره</span><small>${turn?'انتخاب حرکت':'نوبت حریف'}</small></button></div><div class="classic-chat-card"><div class="classic-quick"><button data-classic-quick="موفق باشی! 🍀">موفق باشی! 🍀</button><button data-classic-quick="حرکت خوبی بود 🔥">حرکت خوبی بود 🔥</button><button data-classic-quick="زودتر بازی کن ⏳">زودتر بازی کن ⏳</button></div><div id="classicChatMessages"></div><form id="classicChatForm"><textarea id="classicChatInput" maxlength="300" rows="1" enterkeyhint="send" autocomplete="off" autocorrect="on" spellcheck="true" placeholder="پیامت رو بنویس..."></textarea><button type="submit">➤</button></form></div><button class="classic-leave-btn" data-classic-leave>خروج از بازی</button></section>`;
  }
  async function renderClassicRoot(room){
    const root=$('#classicRoot');if(!root)return;root.innerHTML=classicRoomMarkup(room);
    // Waiting rooms return before the gameplay handlers are installed, so bind the
    // leave action explicitly here. This fixes the inert "خروج از اتاق انتظار" button.
    if(room.status==='waiting'){
      root.onclick=async e=>{
        const leave=e.target.closest('[data-classic-leave-wait]');
        if(!leave)return;
        if(leave.disabled)return;
        leave.disabled=true;leave.textContent='در حال خروج…';
        try{
          const out=await api(`/games/room/${room.id}/leave`,{method:'POST',body:{}});
          clearTimers();window.activeGameRoom=null;document.body.classList.remove('classic-game-mode');view='games';
          toast(out.message||'از اتاق انتظار خارج شدی');
          await gamesScreen();
        }catch(err){
          toast(err.message||'خروج از اتاق انتظار انجام نشد');
          leave.disabled=false;leave.textContent='خروج از اتاق انتظار';
        }
      };
      return;
    }
    const mine=room.players.find(p=>Number(p.user_id)===Number(me.id));if(!mine)return;
    $('#classicRoot').onclick=async e=>{
      const g=e.target.closest('[data-classic-guide]');if(g){gameGuide();return}
      const s=e.target.closest('[data-classic-settings]');if(s){gameSettings();return}const rp=e.target.closest('[data-classic-report]');if(rp){gameReport();return}
      const lm=e.target.closest('[data-classic-move-mode]');if(lm){window.classicWallMode=false;window.classicWallOrientation='h';refreshClassic();return}
      const wm=e.target.closest('[data-classic-wall-mode]');if(wm){window.classicWallMode=true;window.classicWallOrientation=wm.dataset.orientation||'h';refreshClassic();return}
      const cell=e.target.closest('[data-classic-cell]');if(cell&&!window.classicWallMode){const [r,c]=cell.dataset.classicCell.split(':').map(Number);try{const out=await api(`/games/room/${room.id}/action`,{method:'POST',body:{action:'move',row:r,col:c}});if(out.room?.status==='finished')return showClassicFinished(out.room);await refreshClassic(out.room)}catch(err){toast(err.message)}return}
      const wall=e.target.closest('[data-classic-wall]');if(wall&&window.classicWallMode){const [o,r,c]=wall.dataset.classicWall.split(':');try{const out=await api(`/games/room/${room.id}/action`,{method:'POST',body:{action:'wall',orientation:o,row:Number(r),col:Number(c)}});window.classicWallMode=false;if(out.room?.status==='finished')return showClassicFinished(out.room);await refreshClassic(out.room)}catch(err){toast(err.message)}return}
      const q=e.target.closest('[data-classic-quick]');if(q){sendClassicChat(q.dataset.classicQuick);return}
      const leave=e.target.closest('[data-classic-leave]');if(leave){confirmClassicLeave(room.id);return}
      const lw=e.target.closest('[data-classic-leave-wait]');if(lw){try{await api(`/games/room/${room.id}/leave`,{method:'POST',body:{}});clearTimers();view='games';window.activeGameRoom=null;gamesScreen()}catch(err){toast(err.message)}return}
    };
    const chatForm=$('#classicChatForm');if(chatForm)chatForm.onsubmit=e=>{e.preventDefault();const v=$('#classicChatInput')?.value?.trim();if(v)sendClassicChat(v)};const chatInput=$('#classicChatInput');if(chatInput){chatInput.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();chatForm?.requestSubmit();}});}
    loadClassicChat(room.id);classicChatPoll=setInterval(()=>loadClassicChat(room.id),1200);
  }
  function updateClassicPlayingUi(room){
    const root=$('#classicRoot');if(!root)return;
    const st=stateOf(room),players=playersMap(room),mine=players.find(p=>Number(p.user_id)===Number(me.id)),turn=Number(st.turn)===Number(me.id),turnPlayer=players.find(p=>Number(p.user_id)===Number(st.turn)),turnLabel=turn?'نوبت تو':(turnPlayer?.isBot?`نوبت ${turnPlayer.display_name||'ربات'}`:turnPlayer?.display_name?`نوبت ${turnPlayer.display_name}`:'نوبت حریف'),readyLeft=Math.max(0,Number(st.turnReadyAt||0)-Date.now()),left=readyLeft>0?readyLeft:Math.max(0,15000-(Date.now()-Number(st.turnStartedAt||Date.now()))),secs=Math.ceil(left/1000),wallMode=window.classicWallMode===true,meWalls=Number(mine?.walls??(room.gameType==='advanced'?20:10));
    const title=root.querySelector('.classic-turn-title');if(title){title.classList.toggle('your-turn',turn);title.innerHTML=`${esc(readyLeft>0?(turn?'آماده‌سازی نوبت تو':`آماده‌سازی ${turnPlayer?.isBot?'حرکت ربات':'نوبت بازیکن'}`):turnLabel)}<span class="classic-timer">${fa(secs)}</span>`;}
    const board=root.querySelector('.classic-board-wrap');if(board)board.innerHTML=boardHtml(room,wallMode);
    const instruction=root.querySelector('.classic-instruction');if(instruction)instruction.textContent=readyLeft>0?'چند لحظه صبر کن؛ حرکت بعدی به‌زودی انجام می‌شود.':turn?(wallMode?'برای گذاشتن دیوار، جای روشن را لمس کن.':'مهره را حرکت بده یا حالت دیوار را انتخاب کن.'):'صبر کن تا نوبت حریف تمام شود.';
    root.querySelectorAll('[data-classic-wall-mode]').forEach(btn=>{btn.disabled=!turn||readyLeft>0||meWalls<=0;btn.classList.toggle('active',wallMode&&(window.classicWallOrientation||'h')===btn.dataset.orientation);const small=btn.querySelector('small');if(small)small.textContent=`${fa(meWalls)} دیوار`;});
    const moveBtn=root.querySelector('[data-classic-move-mode]');if(moveBtn){moveBtn.disabled=!turn||readyLeft>0;moveBtn.classList.toggle('active',!wallMode);const small=moveBtn.querySelector('small');if(small)small.textContent=turn?'انتخاب حرکت':'نوبت حریف';}
  }
  async function refreshClassic(roomFrom){try{const d=roomFrom?{room:roomFrom}:await api('/games/room/'+window.activeGameRoom);const room=d.room;if(room.status==='finished')return showClassicFinished(room);const root=$('#classicRoot');if(!root)return;if(room.status==='playing'){updateClassicPlayingUi(room);}else{root.innerHTML=classicRoomMarkup(room);bindClassicRoot(room);}}catch(err){toast(err.message)}}
  function bindClassicRoot(room){const root=$('#classicRoot');if(!root)return;root.onclick=async e=>{
    const profile=e.target.closest('[data-profile]');if(profile){e.stopPropagation();window.aceOpenPublicProfile?.(Number(profile.dataset.profile));return}
    const g=e.target.closest('[data-classic-guide]');if(g)return gameGuide();const s=e.target.closest('[data-classic-settings]');if(s)return gameSettings();const rp=e.target.closest('[data-classic-report]');if(rp)return gameReport();const wm=e.target.closest('[data-classic-wall-mode]');if(wm){window.classicWallMode=true;window.classicWallOrientation=wm.dataset.orientation||'h';refreshClassic();return}const mm=e.target.closest('[data-classic-move-mode]');if(mm){window.classicWallMode=false;refreshClassic();return}
    const cell=e.target.closest('[data-classic-cell]');if(cell&&!window.classicWallMode){const [r,c]=cell.dataset.classicCell.split(':').map(Number);try{const out=await api(`/games/room/${room.id}/action`,{method:'POST',body:{action:'move',row:r,col:c}});if(out.room.status==='finished')showClassicFinished(out.room);else refreshClassic(out.room)}catch(err){toast(err.message)}return}
    const wall=e.target.closest('[data-classic-wall]');if(wall&&window.classicWallMode){const [o,r,c]=wall.dataset.classicWall.split(':');try{const out=await api(`/games/room/${room.id}/action`,{method:'POST',body:{action:'wall',orientation:o,row:Number(r),col:Number(c)}});window.classicWallMode=false;if(out.room.status==='finished')showClassicFinished(out.room);else refreshClassic(out.room)}catch(err){toast(err.message)}return}
    const q=e.target.closest('[data-classic-quick]');if(q){sendClassicChat(q.dataset.classicQuick);return}const leave=e.target.closest('[data-classic-leave]');if(leave){confirmClassicLeave(room.id);return}
  };const f=$('#classicChatForm');if(f)f.onsubmit=e=>{e.preventDefault();const v=$('#classicChatInput')?.value?.trim();if(v)sendClassicChat(v)};const ci=$('#classicChatInput');if(ci){ci.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();f?.requestSubmit();}});}
  }
  async function loadClassicChat(roomId){try{const d=await api(`/games/room/${roomId}/messages`);const box=$('#classicChatMessages');if(!box)return;box.innerHTML=(d.messages||[]).map(m=>`<div class="classic-chat-line ${Number(m.user_id)===Number(me.id)?'mine':''}"><b>${esc(m.display_name)}</b><span>${esc(m.body)}</span></div>`).join('');box.scrollTop=box.scrollHeight}catch{}}
  async function sendClassicChat(body){try{const i=$('#classicChatInput');const keepFocus=document.activeElement===i;await api(`/games/room/${window.activeGameRoom}/messages`,{method:'POST',body:{body}});if(i){i.value='';if(keepFocus)i.focus({preventScroll:true});}loadClassicChat(window.activeGameRoom)}catch(err){toast(err.message)}}
  async function confirmClassicLeave(id){
    const layer=document.createElement('div');layer.className='classic-confirm-overlay';layer.innerHTML=`<section class="classic-confirm"><div class="classic-confirm-icon">⚠</div><h3>خروج از بازی</h3><p>با خروج از بازی، هزینه خروج از موجودی‌ات کم می‌شود.</p><div class="classic-cost-list"><span>اولویت ۱: <b>۱ الماس</b></span><span>اگر الماس نداشتی: <b>۱۰۰ سکه</b></span><span>اگر سکه هم نداشتی: <b>۲۰۰۰ شهرت</b></span></div><div class="classic-confirm-actions"><button class="btn secondary" data-classic-cancel>لغو</button><button class="btn danger" data-classic-confirm>تأیید</button></div></section>`;layer.onclick=async e=>{if(e.target===layer||e.target.closest('[data-classic-cancel]')){layer.remove();return}if(e.target.closest('[data-classic-confirm]')){try{const out=await api(`/games/room/${id}/leave`,{method:'POST',body:{}});layer.remove();clearTimers();toast(out.message||'خروج انجام شد');view='games';window.activeGameRoom=null;gamesScreen()}catch(err){toast(err.message)}}};document.body.append(layer);
  }
  function showClassicFinished(room){clearTimers();const st=stateOf(room),myState=st.players?.[String(me.id)],win=room.gameType==='advanced'?!!myState&&Number(myState.team)===Number(st.players?.[String(room.winnerId)]?.team):Number(room.winnerId)===Number(me.id);const root=$('#classicRoot');if(!root)return;root.innerHTML=`<section class="classic-result"><div class="classic-result-logo">${icon(win?'game-win':'game-classic')}</div><span class="classic-update-badge">بازی تمام شد</span><h1>${win?'🎉 بردی!':'این بازی تمام شد'}</h1><p>${win?(room.gameType==='advanced'?'۱۰ الماس و ۱۰۰۰ شهرت به حسابت اضافه شد.':'۵ الماس و ۵۰۰ شهرت به حسابت اضافه شد.'):(room.gameType==='advanced'?'تیم حریف برنده شد.':'حریف برنده شد.')}</p><div class="classic-result-rewards">${win?(room.gameType==='advanced'?'<b>💎 +۱۰ الماس</b><b>✨ +۱۰۰۰ شهرت</b>':'<b>💎 +۵ الماس</b><b>✨ +۵۰۰ شهرت</b>'):''}</div><button class="btn block" data-classic-back>بازگشت به بازی‌ها</button></section>`;root.onclick=e=>{if(e.target.closest('[data-classic-back]')){view='games';window.activeGameRoom=null;gamesScreen()}}}
  async function classicGameRoomScreen(id){
    clearTimers();window.activeGameRoom=id;window.classicWallMode=false;document.body.classList.add('classic-game-mode');shell(`<main id="classicRoot" class="classic-root"><div class="empty">در حال ورود به بازی…</div></main>`);
    try{const d=await api('/games/room/'+id);if(!['classic','advanced'].includes(d.room.gameType)){toast('این نوع بازی در این صفحه پشتیبانی نمی‌شود');view='games';return gamesScreen()};await renderClassicRoot(d.room);if(d.room.status==='waiting'){classicPoll=setInterval(async()=>{try{const x=await api('/games/room/'+id);if(x.room.status==='playing'){clearTimers();await renderClassicRoot(x.room);classicPoll=setInterval(()=>refreshClassic(),700)}else if(x.room.status==='finished'){clearTimers();if(x.room.state?.noOpponent){toast('تا ۶۰ ثانیه حریفی پیدا نشد؛ لابی بسته شد.');window.activeGameRoom=null;document.body.classList.remove('classic-game-mode');view='games';await gamesScreen()}else showClassicFinished(x.room)}}catch{}},1000)}else{classicPoll=setInterval(()=>refreshClassic(),700)}}catch(err){toast(err.message);view='games';gamesScreen()}
  }
  window.gameRoomScreen=async function(id){try{const d=await api('/games/room/'+id);if(['classic','advanced'].includes(d.room.gameType))return classicGameRoomScreen(id);toast('این نوع بازی در این صفحه پشتیبانی نمی‌شود');view='games';return gamesScreen()}catch(err){toast('این نوع بازی دیگر در برنامه فعال نیست');view='games';return gamesScreen()}};
  window.addEventListener('beforeunload',clearTimers);
})();
