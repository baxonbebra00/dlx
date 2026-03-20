const puppeteer = require('puppeteer-core');
const http = require('http');
const https = require('https');
const path = require('path');
const fs = require('fs');

const PROFILES = [
  'ID_1',
  'ID_2',
  'ID_3',
];

const API_KEY = '7826253a44d5ad197ad0f9cc95d564b400773e7a44c2d5bb';

const TG_TOKEN = '8679703791:AAG9if6keUSRufK65ebZByeRLrZUSkiH-U8';
const TG_CHATS = ['7479507723', '5510639654'];

let MODE = process.argv[2] || 'check';

const CONFIG = {
  LIKE_POSTS: [1, 3, 5, 7],
  MAX_FOLLOWS_PER_PROFILE: 12,
  RETRY: 3,
  BATCH: () => MODE === 'check' ? 20 + Math.floor(Math.random() * 10) : 8 + Math.floor(Math.random() * 5),
  DELAY: {
    get afterOpen()    { return MODE === 'check' ? [1500, 2500] : [3000, 5000]; },
    get betweenLikes() { return [800, 1500]; },
    get betweenUsers() { return MODE === 'check' ? [1000, 2000] : [3000, 6000]; },
    get afterLike()    { return [800, 1200]; },
    get retry()        { return [1500, 2500]; },
    get betweenStart() { return MODE === 'check' ? [3000, 5000] : [5000, 8000]; },
    get afterDMCheck() { return MODE === 'check' ? [300, 600] : [1000, 2000]; },
  },
};

const USERS_FILE  = path.join(__dirname, 'users.txt');
const DONE_FILE   = path.join(__dirname, 'done.txt');
const FAILED_FILE = path.join(__dirname, 'failed.txt');
const OPEN_DM_FILE = path.join(__dirname, 'open_dm.txt');
const CLOSED_DM_FILE = path.join(__dirname, 'closed_dm.txt');
const HISTORY_FILE = path.join(__dirname, 'rate_history.json');
const LOG_FILE    = path.join(__dirname, 'log_' + new Date().toISOString().slice(0,16).replace(/[T:]/g,'-') + '.txt');
const logStream   = fs.createWriteStream(LOG_FILE, { flags: 'a' });

function log(msg, level, id) {
  level = level || 'INFO';
  const p = id ? '[' + id.slice(-4) + '] ' : '';
  const line = '[' + new Date().toLocaleTimeString() + '] [' + level.padEnd(5) + '] ' + p + msg;
  console.log(line);
  logStream.write(line + '\n');
}

function banner(msg) {
  const line = '\n' + '='.repeat(56) + '\n  ' + msg + '\n' + '='.repeat(56);
  console.log(line);
  logStream.write(line + '\n');
}

function tg(msg) {
  for (const chatId of TG_CHATS) {
    const payload = JSON.stringify({ chat_id: chatId, text: msg, parse_mode: 'HTML' });
    const req = https.request({
      hostname: 'api.telegram.org',
      path: '/bot' + TG_TOKEN + '/sendMessage',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
    });
    req.on('error', () => {});
    req.write(payload);
    req.end();
  }
}

function loadHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) return JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
  } catch (e) {}
  return {
    events: [],           // [{type, profile, likesAtMoment, followsAtMoment, ts}]
    safeLikes: 800,       // стартовый безопасный лимит лайков/день
    safeFollows: 48,      // стартовый безопасный лимит подписок/день
    baseLikeCooldown: 10,  // мин — базовый кулдаун лайков
    baseFollowCooldown: 5, // мин — базовый кулдаун подписок
    totalRuns: 0,
  };
}

function saveHistory(h) {
  try { fs.writeFileSync(HISTORY_FILE, JSON.stringify(h, null, 2), 'utf8'); } catch (e) {}
}

const history = loadHistory();
history.totalRuns++;

(function learnFromHistory() {
  const week = Date.now() - 7 * 24 * 3600 * 1000;
  const recent = history.events.filter((e) => e.ts > week);
  if (!recent.length) return;

  const likeRLs = recent.filter((e) => e.type === 'like_rl');
  const followRLs = recent.filter((e) => e.type === 'follow_rl');

  if (likeRLs.length >= 2) {
    const minLikes = Math.min(...likeRLs.map((e) => e.likesAtMoment || 800));
    history.safeLikes = Math.max(Math.floor(minLikes * 0.8), 100); // не ниже 100
    log('Обучение: лимит лайков снижен до ' + history.safeLikes + '/день (RL при ' + minLikes + ')');
  } else if (likeRLs.length === 0 && history.safeLikes < 800) {
    history.safeLikes = Math.min(Math.floor(history.safeLikes * 1.1), 800);
    log('Обучение: лимит лайков восстановлен до ' + history.safeLikes + '/день (0 RL за неделю)');
  }
  if (followRLs.length >= 2) {
    const minFollows = Math.min(...followRLs.map((e) => e.followsAtMoment || 48));
    history.safeFollows = Math.max(Math.floor(minFollows * 0.8), 12); // не ниже 12
    log('Обучение: лимит подписок снижен до ' + history.safeFollows + '/день (RL при ' + minFollows + ')');
  } else if (followRLs.length === 0 && history.safeFollows < 48) {
    history.safeFollows = Math.min(Math.floor(history.safeFollows * 1.1), 48);
    log('Обучение: лимит подписок восстановлен до ' + history.safeFollows + '/день (0 RL за неделю)');
  }

  if (likeRLs.length > 3) {
    history.baseLikeCooldown = Math.min(10 + likeRLs.length, 30); // 10 + кол-во RL, макс 30
    log('Обучение: кулдаун лайков = ' + history.baseLikeCooldown + ' мин (' + likeRLs.length + ' RL за неделю)');
  } else if (likeRLs.length === 0 && history.baseLikeCooldown > 10) {
    history.baseLikeCooldown = 10; // сброс к базе если нет RL
    log('Обучение: кулдаун лайков сброшен до 10 мин (0 RL за неделю)');
  }
  if (followRLs.length > 3) {
    history.baseFollowCooldown = Math.min(5 + followRLs.length, 20);
    log('Обучение: кулдаун подписок = ' + history.baseFollowCooldown + ' мин (' + followRLs.length + ' RL за неделю)');
  } else if (followRLs.length === 0 && history.baseFollowCooldown > 5) {
    history.baseFollowCooldown = 5;
    log('Обучение: кулдаун подписок сброшен до 5 мин (0 RL за неделю)');
  }

  saveHistory(history);
})();

const rateLimiter = {
  likesToday: 0,
  followsToday: 0,
  likeRL: 0,
  followRL: 0,
  safeLikesPerDay: history.safeLikes,
  safeFollowsPerDay: history.safeFollows,

  getLikeCooldown() {
    const base = history.baseLikeCooldown * 60 * 1000;
    const mult = 1 + this.likeRL * 0.5;
    return Math.min(base * mult, 40 * 60 * 1000);
  },
  getFollowCooldown() {
    const base = history.baseFollowCooldown * 60 * 1000;
    const mult = 1 + this.followRL * 0.7;
    return Math.min(base * mult, 30 * 60 * 1000);
  },

  recordRL(type, profileId) {
    history.events.push({
      type: type,
      profile: profileId ? profileId.slice(-4) : '?',
      likesAtMoment: this.likesToday,
      followsAtMoment: this.followsToday,
      ts: Date.now(),
    });
    const month = Date.now() - 30 * 24 * 3600 * 1000;
    history.events = history.events.filter((e) => e.ts > month);
    saveHistory(history);
  },

  canLike() { return this.likesToday < this.safeLikesPerDay; },
  canFollow() { return this.followsToday < this.safeFollowsPerDay; },

  getUserDelay() {
    const likeRatio = this.likesToday / this.safeLikesPerDay;
    if (likeRatio > 0.8) return [8000, 12000];
    if (likeRatio > 0.6) return [5000, 8000];
    return CONFIG.DELAY.betweenUsers;
  },
  formatCooldown(ms) {
    const min = Math.round(ms / 60000);
    return min + ' мин';
  },
  status() {
    return 'Лайки: ' + this.likesToday + '/' + this.safeLikesPerDay +
      ' | Подписки: ' + this.followsToday + '/' + this.safeFollowsPerDay +
      ' | RL: ' + this.likeRL + 'L/' + this.followRL + 'F' +
      ' | Запусков: ' + history.totalRuns;
  },
};

const startTime = Date.now();
let processed = 0;
let statDmOpen = 0;
let statDmClosed = 0;
let statSkipped = 0;
let statFollows = 0;
let stopping = false;
let lastTgStatus = 0; // когда последний раз слали статус в TG
const TG_STATUS_INTERVAL = 50; // каждые N обработанных юзеров — статус в TG

const profileRLCount = {};  // { profileId: number }
const burnedProfiles = new Set(); // заблокированные до конца прогона
const MAX_RL_PER_PROFILE = 3; // после 3 RL — стоп профиля

const xDetector = {
  lastReset: 0,     // unix ts когда лимит сбросится (из заголовка)
  last429: 0,       // когда последний 429
  count429: 0,      // сколько 429 за сессию
  banned: false,    // обнаружен бан

  getWaitMs() {
    if (!this.lastReset) return 0;
    const now = Math.floor(Date.now() / 1000);
    const wait = (this.lastReset - now) * 1000;
    return wait > 0 ? wait : 0;
  },

  formatWait() {
    const ms = this.getWaitMs();
    if (!ms) return 'неизвестно';
    const min = Math.ceil(ms / 60000);
    if (min >= 60) return Math.floor(min / 60) + 'ч ' + (min % 60) + 'мин';
    return min + ' мин';
  },

  reset() {
    this.lastReset = 0;
    this.last429 = 0;
    this.count429 = 0;
  },
};

function attachNetworkDetector(page, profileId) {
  page.on('response', (response) => {
    try {
      const status = response.status();
      const url = response.url();
      if (!url.includes('x.com/i/api') && !url.includes('twitter.com/i/api')) return;

      if (status === 429) {
        xDetector.count429++;
        xDetector.last429 = Date.now();
        const headers = response.headers();
        const reset = headers['x-rate-limit-reset'];
        if (reset) {
          xDetector.lastReset = parseInt(reset, 10);
          const waitMin = Math.ceil(xDetector.getWaitMs() / 60000);
          log('🔴 429 от X API — лимит сбросится через ' + waitMin + ' мин (reset: ' + new Date(xDetector.lastReset * 1000).toLocaleTimeString() + ')', 'WARN', profileId);
        } else {
          log('🔴 429 от X API — без заголовка reset', 'WARN', profileId);
        }
      }

      if (status === 403 && url.includes('/api/')) {
        log('🔴 403 от X API — возможно ограничение аккаунта', 'WARN', profileId);
      }
    } catch (e) { /* игнорируем ошибки в обработчике */ }
  });
}

async function checkAccountRestriction(page, profileId) {
  const restriction = await safeEval(page, () => {
    const body = document.body ? document.body.innerText : '';

    if (body.includes('Your account is suspended') || body.includes('Account suspended')) {
      return { type: 'suspended', msg: 'Account suspended' };
    }

    if (body.includes('temporarily limited') || body.includes('Temporarily restricted')) {
      return { type: 'restricted', msg: 'Temporarily restricted' };
    }

    if (body.includes('Your account has been locked') || body.includes('account is locked')) {
      return { type: 'locked', msg: 'Account locked — требуется верификация' };
    }

    if (body.includes('Rate limit exceeded') || body.includes('rate limit exceeded')) {
      return { type: 'ratelimit_page', msg: 'Rate limit exceeded page' };
    }
    const h1 = document.querySelector('h1');
    if (h1 && (h1.textContent.includes('Try again') || h1.textContent.includes('Something went wrong'))) {
      return { type: 'ratelimit_page', msg: h1.textContent.slice(0, 80) };
    }

    if (body.includes('arkose') || document.querySelector('iframe[src*="arkose"]') ||
        document.querySelector('iframe[src*="challenge"]')) {
      return { type: 'captcha', msg: 'Captcha/challenge detected' };
    }

    const errorH = document.querySelector('[data-testid="error-detail"]');
    if (errorH) {
      return { type: 'error_page', msg: errorH.textContent.slice(0, 100) };
    }

    return null;
  }, 8000);

  if (!restriction) return null;

  log('🚨 ОБНАРУЖЕНО: ' + restriction.type + ' — ' + restriction.msg, 'ERROR', profileId);
  tg('🚨 <b>' + restriction.type.toUpperCase() + '</b>\nПрофиль: ' + profileId.slice(-4) + '\n' + restriction.msg);
  rateLimiter.recordRL(restriction.type, profileId);

  return restriction;
}

process.on('SIGINT', () => {
  if (stopping) { process.exit(1); }
  stopping = true;
  log('\nCtrl+C — завершаю текущего юзера и выхожу...', 'WARN');
});
process.on('SIGTERM', () => { stopping = true; });

process.on('unhandledRejection', (err) => {
  log('Необработанная ошибка: ' + (err && err.message ? err.message : String(err)), 'ERROR');
});
process.on('uncaughtException', (err) => {
  log('Критическая ошибка: ' + err.message, 'ERROR');
  tg('💀 <b>Критическая ошибка</b>\n' + err.message.slice(0, 200));
  saveHistory(history);
  logStream.end();
  process.exit(1);
});

function eta(remaining) {
  if (!processed) return '—';
  const secPerAcc = (Date.now() - startTime) / 1000 / processed;
  const secLeft = secPerAcc * remaining;
  const h = Math.floor(secLeft / 3600);
  const m = Math.floor((secLeft % 3600) / 60);
  return (h ? h + 'ч ' : '') + m + 'мин';
}

function elapsed() {
  const sec = Math.floor((Date.now() - startTime) / 1000);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return (h ? h + 'ч ' : '') + m + 'мин';
}

const rnd  = (range) => range[0] + Math.floor(Math.random() * (range[1] - range[0] + 1));
const wait = (range) => new Promise((r) => setTimeout(r, rnd(range)));

const human = {
  async moveToElement(page, el) {
    try {
      const box = await withTimeout(el.boundingBox(), 3000, 'boundingBox');
      if (!box) return;
      const x = box.x + box.width * (0.3 + Math.random() * 0.4);
      const y = box.y + box.height * (0.3 + Math.random() * 0.4);
      await page.mouse.move(x, y, { steps: 5 + Math.floor(Math.random() * 10) });
    } catch (e) { /* не критично */ }
  },

  async click(page, el) {
    await this.moveToElement(page, el);
    await wait([50, 150]); // человек не кликает мгновенно после наведения
    try {
      const box = await withTimeout(el.boundingBox(), 3000, 'boundingBox');
      if (!box) { await el.click(); return; }
      const x = box.x + box.width * (0.3 + Math.random() * 0.4);
      const y = box.y + box.height * (0.3 + Math.random() * 0.4);
      await page.mouse.click(x, y, { delay: 30 + Math.floor(Math.random() * 70) }); // mousedown→mouseup задержка
    } catch (e) {
      try { await el.click(); } catch (e2) { /* fallback */ }
    }
  },

  async scroll(page) {
    const distance = 200 + Math.floor(Math.random() * 400); // 200-600px
    const behavior = Math.random() > 0.3 ? 'smooth' : 'auto'; // 70% smooth
    await safeEval(page, `window.scrollBy({ top: ${distance}, behavior: '${behavior}' })`, 3000);
    await wait([200, 500]);
  },

  async idleMove(page) {
    try {
      const x = 200 + Math.floor(Math.random() * 600);
      const y = 200 + Math.floor(Math.random() * 400);
      await page.mouse.move(x, y, { steps: 3 + Math.floor(Math.random() * 8) });
    } catch (e) { /* не критично */ }
  },

  shouldPause(n) {
    if (n > 0 && n % (5 + Math.floor(Math.random() * 8)) === 0) return [3000, 8000];
    return null;
  },

  shuffleLikeOrder(posts) {
    if (Math.random() > 0.2) return [...posts];
    const arr = [...posts];
    for (let i = 0; i < 1 + Math.floor(Math.random() * 2); i++) {
      const a = Math.floor(Math.random() * arr.length);
      const b = Math.floor(Math.random() * arr.length);
      [arr[a], arr[b]] = [arr[b], arr[a]];
    }
    return arr;
  },
};

function withTimeout(promise, ms, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Таймаут ' + (label || '') + ' (' + ms + 'мс)')), ms);
    promise.then(
      (v) => { clearTimeout(timer); resolve(v); },
      (e) => { clearTimeout(timer); reject(e); }
    );
  });
}

async function safeEval(page, fn, timeout) {
  try { return await withTimeout(page.evaluate(fn), timeout || 15000, 'evaluate'); }
  catch (e) { log('safeEval: ' + e.message, 'WARN'); return null; }
}

async function safeQuery(page, selector, timeout) {
  try { return await withTimeout(page.$(selector), timeout || 10000, '$(' + selector + ')'); }
  catch (e) { return null; }
}

async function safeQueryAll(page, selector, timeout) {
  try { return await withTimeout(page.$$(selector), timeout || 10000, '$$(' + selector + ')'); }
  catch (e) { return []; }
}

function readLines(file) {
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8')
    .split('\n')
    .map((l) => l.trim().replace(/^@/, '').split(/[\s,]/)[0])
    .filter((l) => l && !l.startsWith('#'));
}

function loadUsers() {
  const users = readLines(USERS_FILE);
  if (!users.length) throw new Error('Файл users.txt не найден или пустой');
  return users;
}

function loadDone()   { return new Set(readLines(DONE_FILE)); }
function loadFailed() { return new Set(readLines(FAILED_FILE)); }

function markDone(username) {
  fs.appendFileSync(DONE_FILE, username + '\n', 'utf8');
  removeFailed(username);
}

function markFailed(username) {
  const failed = loadFailed();
  if (!failed.has(username)) fs.appendFileSync(FAILED_FILE, username + '\n', 'utf8');
}

function markOpenDM(username) {
  const existing = readLines(OPEN_DM_FILE);
  if (!existing.includes(username)) {
    fs.appendFileSync(OPEN_DM_FILE, username + '\n', 'utf8');
  }
}

function markClosedDM(username) {
  const existing = readLines(CLOSED_DM_FILE);
  if (!existing.includes(username)) {
    fs.appendFileSync(CLOSED_DM_FILE, username + '\n', 'utf8');
  }
}

function removeFailed(username) {
  if (!fs.existsSync(FAILED_FILE)) return;
  const lines = readLines(FAILED_FILE).filter((u) => u !== username);
  fs.writeFileSync(FAILED_FILE, lines.join('\n') + (lines.length ? '\n' : ''), 'utf8');
}

function buildQueue(allUsers) {
  const done   = loadDone();
  const failed = loadFailed();
  const dmOpen = new Set(readLines(OPEN_DM_FILE));
  const dmClosed = new Set(readLines(CLOSED_DM_FILE));

  if (MODE === 'like') {
    const likeQueue = [...dmClosed].filter((u) => !done.has(u));
    log('Режим LIKE: ' + likeQueue.length + ' юзеров с закрытыми DM для лайков');
    return likeQueue;
  }

  if (MODE === 'check') {
    const checkQueue = allUsers.filter((u) => !done.has(u) && !failed.has(u) && !dmOpen.has(u) && !dmClosed.has(u));
    log('Режим CHECK: ' + checkQueue.length + ' юзеров для проверки DM');
    return checkQueue;
  }

  const pending = allUsers.filter((u) => !done.has(u) && !failed.has(u) && !dmOpen.has(u));
  const retries = [...failed].filter((u) => !done.has(u) && !dmOpen.has(u));
  const mixed = [...retries];
  for (const u of pending) {
    const pos = Math.floor(Math.random() * (mixed.length + 1));
    mixed.splice(pos, 0, u);
  }
  return mixed;
}

function adspowerGet(endpoint) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1', port: 50325,
      path: endpoint, method: 'GET',
      headers: { 'Authorization': API_KEY },
      timeout: 10000,
    }, (res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error('Ответ: ' + body.slice(0, 120))); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Таймаут AdsPower')); });
    req.end();
  });
}

async function startProfile(userId) {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await adspowerGet('/api/v1/browser/start?user_id=' + userId + '&open_tabs=1&api_key=' + API_KEY);
      if (res.code === 0 && res.data && res.data.ws && res.data.ws.puppeteer) return res.data.ws.puppeteer;
      throw new Error(res.msg || 'неизвестная ошибка');
    } catch (e) {
      log('Старт попытка ' + attempt + '/3: ' + e.message, 'WARN', userId);
      if (attempt < 3) await wait([4000, 6000]);
      else throw new Error('Не удалось запустить профиль после 3 попыток');
    }
  }
}

async function stopProfile(userId) {
  try { await adspowerGet('/api/v1/browser/stop?user_id=' + userId + '&api_key=' + API_KEY); } catch (e) {}
}

async function dismissCookies(page) {
  const closed = await safeEval(page, () => {
    const btns = Array.from(document.querySelectorAll('button'));
    const r = btns.find((b) => b.textContent.includes('Refuse') || b.textContent.includes('Decline'));
    if (r) { r.click(); return true; }
    const a = btns.find((b) => b.textContent.includes('Accept all'));
    if (a) { a.click(); return true; }
    return false;
  }, 5000);
  if (closed) await wait([400, 700]);
}

async function followUser(page, id) {
  const status = await safeEval(page, () => {
    const btns = Array.from(document.querySelectorAll('[role="button"]'));
    const already = btns.find((b) => { const t = (b.textContent || '').trim(); return t === 'Following' || t === 'Unfollow'; });
    if (already) return 'already';
    const follow = btns.find((b) => (b.textContent || '').trim() === 'Follow' && !(b.getAttribute('data-testid') || '').includes('unfollow'));
    if (follow) return 'can';
    return null;
  });

  if (status === 'already') { log('Уже подписан', 'INFO', id); return 'already'; }
  if (!status) { log('Кнопка подписки не найдена', 'WARN', id); return 'not_found'; }

  const allBtns = await safeQueryAll(page, '[role="button"]');
  let targetBtn = null;
  for (const btn of allBtns) {
    const isFollow = await withTimeout(btn.evaluate((el) => {
      const t = (el.textContent || '').trim();
      return t === 'Follow' && !(el.getAttribute('data-testid') || '').includes('unfollow');
    }), 3000, 'findFollow').catch(() => false);
    if (isFollow) { targetBtn = btn; break; }
  }

  if (targetBtn) {
    await human.click(page, targetBtn); // мышь + клик как человек
  } else {
    await safeEval(page, () => {
      const btns = Array.from(document.querySelectorAll('[role="button"]'));
      const f = btns.find((b) => (b.textContent || '').trim() === 'Follow');
      if (f) f.click();
    });
  }

  await wait([1000, 1500]);

  const state = await safeEval(page, () => {
    const btns = Array.from(document.querySelectorAll('[role="button"]'));
    const following = btns.find((b) => { const t = (b.textContent || '').trim(); return t === 'Following' || t === 'Unfollow'; });
    if (following) return 'following';
    const follow = btns.find((b) => (b.textContent || '').trim() === 'Follow');
    if (follow) return 'unfollowed';
    return 'unknown';
  });

  if (state === 'following') { log('Подписка ✓', 'INFO', id); return 'ok'; }
  if (state === 'unfollowed') { log('X отменил подписку — лимит', 'WARN', id); return 'ratelimit'; }
  if (!state) { log('Таймаут проверки подписки', 'WARN', id); return 'error'; }
  return 'ok';
}

async function checkDMOpen(page) {
  try {
    await withTimeout(page.waitForFunction(() => {
      const btns = Array.from(document.querySelectorAll('[role="button"]'));
      return btns.some((b) => {
        const t = (b.textContent || '').trim();
        return t === 'Follow' || t === 'Following' || t === 'Unfollow';
      });
    }, { timeout: 8000 }), 12000, 'waitForFollow');
  } catch (e) {
  }

  const result = await safeEval(page, () => {
    const dmBtn = document.querySelector('[data-testid="sendDMFromProfile"]');
    if (dmBtn) return true;
    const profileActions = document.querySelector('[data-testid="userActions"]');
    if (profileActions) {
      const mailIcon = profileActions.querySelector('[data-testid*="message"], [data-testid*="dm"], [data-testid*="DM"]');
      if (mailIcon) return true;
    }
    return false;
  });
  return !!result;
}

async function likePost(page, index) {
  try {
    await withTimeout(page.waitForSelector('article[data-testid="tweet"]', { timeout: 10000 }), 15000, 'waitTweets');
  } catch (e) { return { ok: false, why: 'no_tweets' }; }

  let articles = await safeQueryAll(page, 'article[data-testid="tweet"]');
  for (let s = 0; articles.length < index && s < 8; s++) {
    await human.scroll(page); // человеческий скролл с рандомным расстоянием
    articles = await safeQueryAll(page, 'article[data-testid="tweet"]');
  }
  if (articles.length < index) return { ok: false, why: 'only_' + articles.length };

  const article = articles[index - 1];
  try {
    await withTimeout(article.evaluate((el) => { el.scrollIntoView({ block: 'center', behavior: 'smooth' }); }), 5000, 'scrollIntoView');
  } catch (e) { return { ok: false, why: 'scroll_timeout' }; }
  await wait([300, 600]);
  await dismissCookies(page);

  let state;
  try {
    state = await withTimeout(article.evaluate((el) => {
      if (el.querySelector('[data-testid="unlike"]')) return 'already';
      if (el.querySelector('[data-testid="like"]')) return 'can';
      return 'none';
    }), 8000, 'checkLikeState');
  } catch (e) { return { ok: false, why: 'state_timeout' }; }

  if (state === 'already') return { ok: true, why: 'already' };
  if (state === 'none') return { ok: false, why: 'no_button' };

  try {
    const likeBtn = await withTimeout(article.$('[data-testid="like"]'), 3000, 'findLikeBtn');
    if (likeBtn) {
      await human.click(page, likeBtn);
    } else {
      await withTimeout(article.evaluate((el) => { el.querySelector('[data-testid="like"]').click(); }), 5000, 'clickLike');
    }
  } catch (e) { return { ok: false, why: 'click_timeout' }; }
  await wait(CONFIG.DELAY.afterLike);

  let ok;
  try {
    ok = await withTimeout(article.evaluate((el) => !!el.querySelector('[data-testid="unlike"]')), 5000, 'confirmLike');
  } catch (e) { return { ok: false, why: 'confirm_timeout' }; }
  return ok ? { ok: true, why: 'liked' } : { ok: false, why: 'not_confirmed' };
}

async function likeWithRetry(page, index, id) {
  for (let i = 1; i <= CONFIG.RETRY; i++) {
    const r = await likePost(page, index);
    if (r.ok) {
      if (r.why === 'already') return 'already';
      log('#' + index + ' лайк', 'INFO', id);
      return 'liked';
    }
    if (i < CONFIG.RETRY) await wait(CONFIG.DELAY.retry);
    else log('#' + index + ' не удалось', 'WARN', id);
  }
  return false;
}

async function checkSessionAlive(page, id) {
  try {
    const url = page.url();
    if (url.includes('/login') || url.includes('/i/flow/login')) {
      log('Сессия слетела — редирект на логин!', 'ERROR', id);
      return false;
    }
    return true;
  } catch (e) { return false; }
}

async function processUser(page, username, num, total, id, followsLeft) {
  const stats = ' [DM✓' + statDmOpen + ' DM✗' + statDmClosed + (MODE !== 'check' ? ' | ' + rateLimiter.status() : '') + ']';
  const followInfo = MODE === 'check' ? '' : ' (подписок осталось: ' + followsLeft + ')';
  log('[' + num + '/' + total + '] @' + username + followInfo + stats, 'INFO', id);

  let loaded = false;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await withTimeout(page.goto('https://x.com/' + username, { waitUntil: 'domcontentloaded', timeout: 25000 }), 35000, 'goto @' + username);
      loaded = true;
      break;
    } catch (e) {
      log('Загрузка попытка ' + attempt + '/3', 'WARN', id);
      if (attempt < 3) await wait([2000, 3000]);
    }
  }
  if (!loaded) { log('Не загрузилась', 'ERROR', id); return 'error'; }

  await wait(CONFIG.DELAY.afterOpen);
  await dismissCookies(page);

  if (MODE !== 'check') {
    await human.idleMove(page);
    const humanPause = human.shouldPause(num);
    if (humanPause) {
      log('Пауза (имитация)', 'INFO', id);
      await wait(humanPause);
    }
  }

  if (!await checkSessionAlive(page, id)) return 'session_dead';

  const restriction = MODE !== 'check' ? await checkAccountRestriction(page, id) : null;
  if (restriction) {
    if (restriction.type === 'suspended' || restriction.type === 'locked') {
      burnedProfiles.add(id);
      return 'session_dead';
    }
    if (restriction.type === 'captcha') {
      burnedProfiles.add(id);
      tg('🚫 <b>CAPTCHA</b>\nПрофиль: ' + id.slice(-4) + '\nТребуется ручное решение — профиль остановлен');
      return 'session_dead';
    }
    if (restriction.type === 'restricted' || restriction.type === 'ratelimit_page') {
      const apiWait = xDetector.getWaitMs();
      if (apiWait > 0) {
        log('Ждём сброс лимита X API: ' + xDetector.formatWait(), 'WARN', id);
        tg('⏳ Ждём сброс лимита X API\nПрофиль: ' + id.slice(-4) + '\nОсталось: ' + xDetector.formatWait());
      }
      return 'ratelimit';
    }
    if (restriction.type === 'error_page') {
      log('Ошибка страницы X — пропускаю юзера', 'WARN', id);
      return 'error';
    }
  }

  const empty = await safeQuery(page, '[data-testid="empty_state_header_text"]');
  if (empty) { log('Не найден', 'WARN', id); statSkipped++; return 'skip'; }

  const suspended = await safeEval(page, () => {
    const h = document.querySelector('h2');
    if (h && (h.textContent.includes('suspended') || h.textContent.includes('Account suspended'))) return true;
    const s = document.querySelector('[data-testid="empty_state_header_text"]');
    if (s && s.textContent.includes('doesn')) return true;
    return false;
  });
  if (suspended) { log('Забанен/suspended', 'WARN', id); statSkipped++; return 'skip'; }

  const locked = await safeQuery(page, '[data-testid="shieldIcon"]');
  if (locked) { log('Приватный', 'WARN', id); statSkipped++; return 'skip'; }

  const dmOpen = await checkDMOpen(page);
  await wait(CONFIG.DELAY.afterDMCheck);

  if (dmOpen) {
    log('✉ DM ОТКРЫТЫ — сохраняю', 'INFO', id);
    markOpenDM(username);
    statDmOpen++;
    return 'ok';
  }

  statDmClosed++;

  if (MODE === 'check') {
    log('✗ DM закрыты — записал в closed_dm.txt', 'INFO', id);
    markClosedDM(username);
    return 'ok';
  }

  log('✗ DM закрыты — подписка + лайки', 'INFO', id);

  if (followsLeft > 0 && rateLimiter.canFollow()) {
    let followResult = 'error';
    for (let attempt = 1; attempt <= 2; attempt++) {
      followResult = await followUser(page, id);
      if (followResult === 'ratelimit') {
        rateLimiter.followRL++;
        rateLimiter.recordRL('follow_rl', id);
        profileRLCount[id] = (profileRLCount[id] || 0) + 1;
        const cd = rateLimiter.getFollowCooldown();
        const rlNum = profileRLCount[id];
        log('RL подписок #' + rateLimiter.followRL + ' (профиль: ' + rlNum + '/' + MAX_RL_PER_PROFILE + ') — СТОП', 'WARN', id);
        tg('⚠️ Рейтлимит подписок (x' + rateLimiter.followRL + ')\nПрофиль: ' + id.slice(-4) + ' (' + rlNum + '/' + MAX_RL_PER_PROFILE + ' RL)\nПауза: ' + rateLimiter.formatCooldown(cd) + '\n' + rateLimiter.status());
        if (rlNum >= MAX_RL_PER_PROFILE) {
          burnedProfiles.add(id);
          log('🚫 ПРОФИЛЬ ЗАБЛОКИРОВАН до конца прогона (' + rlNum + ' RL)', 'ERROR', id);
          tg('🚫 <b>ПРОФИЛЬ ЗАБЛОКИРОВАН</b>\n' + id.slice(-4) + ' — ' + rlNum + ' рейтлимитов, пропускаю до конца прогона');
        }
        return 'ratelimit';
      }
      if (followResult === 'ok') { statFollows++; rateLimiter.followsToday++; break; }
      if (followResult === 'already') break;
      if (attempt < 2) await wait([1000, 2000]);
    }
    await wait([500, 800]);
  } else if (followsLeft <= 0) {
    log('Лимит подписок профиля — только лайки', 'INFO', id);
  } else {
    log('Дневной лимит подписок (' + rateLimiter.followsToday + '/' + rateLimiter.safeFollowsPerDay + ') — только лайки', 'WARN', id);
  }

  if (!rateLimiter.canLike()) {
    log('Дневной лимит лайков (' + rateLimiter.likesToday + '/' + rateLimiter.safeLikesPerDay + ') — пропускаю', 'WARN', id);
    return 'ok';
  }

  const hasPosts = await safeQuery(page, 'article[data-testid="tweet"]');
  if (!hasPosts) { log('Нет постов', 'WARN', id); return 'ok'; }

  const likeOrder = human.shuffleLikeOrder(CONFIG.LIKE_POSTS);

  let likeFails = 0;
  for (const idx of likeOrder) {
    if (!rateLimiter.canLike()) {
      log('Дневной лимит лайков достигнут — стоп', 'WARN', id);
      break;
    }
    const likeResult = await likeWithRetry(page, idx, id);
    if (likeResult === 'liked') {
      likeFails = 0;
      rateLimiter.likesToday++; // только НОВЫЕ лайки считаем в дневной лимит
    } else if (likeResult === 'already') {
      likeFails = 0; // already не считается фейлом И не считается новым лайком
    } else {
      likeFails++;
    }
    if (likeFails >= 3) {
      rateLimiter.likeRL++;
      rateLimiter.recordRL('like_rl', id);
      profileRLCount[id] = (profileRLCount[id] || 0) + 1;
      const cd = rateLimiter.getLikeCooldown();
      const rlNum = profileRLCount[id];
      log('RL лайков #' + rateLimiter.likeRL + ' (профиль: ' + rlNum + '/' + MAX_RL_PER_PROFILE + ') — СТОП ПРОФИЛЯ, пауза ' + rateLimiter.formatCooldown(cd), 'WARN', id);
      tg('⚠️ Рейтлимит лайков (x' + rateLimiter.likeRL + ')\nПрофиль: ' + id.slice(-4) + ' (' + rlNum + '/' + MAX_RL_PER_PROFILE + ' RL)\nПауза: ' + rateLimiter.formatCooldown(cd) + '\n' + rateLimiter.status());
      if (rlNum >= MAX_RL_PER_PROFILE) {
        burnedProfiles.add(id);
        log('🚫 ПРОФИЛЬ ЗАБЛОКИРОВАН до конца прогона (' + rlNum + ' RL)', 'ERROR', id);
        tg('🚫 <b>ПРОФИЛЬ ЗАБЛОКИРОВАН</b>\n' + id.slice(-4) + ' — ' + rlNum + ' рейтлимитов, пропускаю до конца прогона');
      }
      return 'ratelimit';
    }
    await wait(CONFIG.DELAY.betweenLikes);
  }

  return 'ok';
}

function createQueue(users) {
  let index = 0;
  let locked = false;
  const next = (count) => new Promise((resolve) => {
    const tryGet = () => {
      if (locked) { setTimeout(tryGet, 50); return; }
      locked = true;
      const slice = users.slice(index, index + count);
      index += slice.length;
      locked = false;
      resolve(slice);
    };
    tryGet();
  });
  return {
    next,
    remaining: () => users.length - index,
    currentIndex: () => index,
  };
}

async function runProfileBatch(userId, users, totalUsers, profileFollows) {
  let browser;
  try {
    const ws = await startProfile(userId);
    await wait([2000, 3000]);
    browser = await withTimeout(puppeteer.connect({ browserWSEndpoint: ws, defaultViewport: null }), 20000, 'puppeteer.connect');
    const pages = await withTimeout(browser.pages(), 10000, 'browser.pages');
    const page = pages[pages.length - 1];

    xDetector.reset();
    attachNetworkDetector(page, userId);

    await withTimeout(page.goto('https://x.com/home', { waitUntil: 'networkidle2', timeout: 40000 }), 50000, 'goto home');
    await wait([2000, 3000]);
    await dismissCookies(page);

    const homeRestriction = await checkAccountRestriction(page, userId);
    if (homeRestriction) {
      if (homeRestriction.type === 'suspended' || homeRestriction.type === 'locked') {
        burnedProfiles.add(userId);
        tg('🚫 <b>ПРОФИЛЬ ЗАБАНЕН</b>\n' + userId.slice(-4) + ' — ' + homeRestriction.msg + '\nПрофиль исключён из прогона');
        return { status: 'session_dead', follows: profileFollows };
      }
    }

    let auth = null;
    for (let t = 0; t < 3; t++) {
      auth = await safeQuery(page, '[data-testid="SideNav_AccountSwitcher_Button"]');
      if (auth) break;
      log('Жду авторизацию ' + (t+1) + '/3', 'WARN', userId);
      await wait([3000, 4000]);
    }
    if (!auth) { log('Не авторизован', 'WARN', userId); return { status: 'ok', follows: profileFollows }; }
    log('Авторизован', 'INFO', userId);

    for (let i = 0; i < users.length; i++) {
      if (stopping) { log('Остановка по Ctrl+C', 'WARN', userId); return { status: 'stopped', follows: profileFollows }; }

      try {
        await withTimeout(page.evaluate(() => true), 5000, 'pageAlive');
      } catch (e) {
        log('Страница не отвечает — закрываю профиль', 'ERROR', userId);
        for (let j = i; j < users.length; j++) markFailed(users[j].username);
        return { status: 'session_dead', follows: profileFollows };
      }

      if (xDetector.count429 >= 5 && (Date.now() - xDetector.last429) < 120000) {
        log('🔴 429 шторм (' + xDetector.count429 + ' за сессию) — стоп профиля', 'ERROR', userId);
        profileRLCount[userId] = (profileRLCount[userId] || 0) + 2; // +2 сразу за шторм
        if (profileRLCount[userId] >= MAX_RL_PER_PROFILE) burnedProfiles.add(userId);
        tg('🔴 <b>429 ШТОРМ</b>\nПрофиль: ' + userId.slice(-4) + '\n' + xDetector.count429 + ' ответов 429 — стоп');
        for (let j = i; j < users.length; j++) markFailed(users[j].username);
        return { status: 'ratelimit', follows: profileFollows };
      }

      if (processed > 0 && processed - lastTgStatus >= TG_STATUS_INTERVAL) {
        lastTgStatus = processed;
        const doneNow = loadDone().size;
        const openDMNow = readLines(OPEN_DM_FILE).length;
        const remaining = queue.remaining() + (users.length - i - 1);
        tg('📊 <b>Прогресс</b>\n\nОбработано: ' + doneNow + '/' + totalUsers + ' (' + Math.round(doneNow / totalUsers * 100) + '%)\nDM открыты: ' + openDMNow + '\nDM закрыты: ' + statDmClosed + '\nОсталось: ~' + remaining + '\nETA: ' + eta(remaining) + '\nВремя: ' + elapsed() + '\n\n' + rateLimiter.status());
      }

      const username = users[i].username;
      const followsLeft = CONFIG.MAX_FOLLOWS_PER_PROFILE - profileFollows;
      const followsBefore = statFollows;
      const result = await processUser(page, username, users[i].num, totalUsers, userId, followsLeft);

      if (statFollows > followsBefore) {
        profileFollows += (statFollows - followsBefore);
      }

      if (result === 'session_dead') {
        log('Сессия мертва — закрываю профиль', 'ERROR', userId);
        rateLimiter.recordRL('session_dead', userId);
        tg('<b>Сессия слетела</b>\nПрофиль: ' + userId.slice(-4) + '\n\n' + rateLimiter.status());
        for (let j = i; j < users.length; j++) markFailed(users[j].username);
        return { status: 'session_dead', follows: profileFollows };
      }

      if (result === 'ratelimit') {
        markFailed(username);
        return { status: 'ratelimit', follows: profileFollows };
      }

      if (result === 'ok' || result === 'skip') {
        markDone(username);
        processed++;
      } else {
        markFailed(username);
      }

      if (i < users.length - 1) await wait(rateLimiter.getUserDelay());
    }
    return { status: 'ok', follows: profileFollows };
  } catch (e) {
    log('Ошибка: ' + e.message, 'ERROR', userId);
    const d = loadDone(); // читаем ОДИН раз, не N раз
    for (const u of users) {
      if (!d.has(u.username)) markFailed(u.username);
    }
    return { status: 'ok', follows: profileFollows };
  } finally {
    if (browser) try { await browser.disconnect(); } catch(e) {}
    await stopProfile(userId);
    log('Завершён (подписок: ' + profileFollows + '/' + CONFIG.MAX_FOLLOWS_PER_PROFILE + ')', 'INFO', userId);
  }
}

async function runProfile(userId, queue, totalUsers) {
  let profileFollows = 0;

  while (queue.remaining() > 0 && !stopping) {
    const count = CONFIG.BATCH();
    const users = await queue.next(count);
    if (!users.length) break;
    const batch = users.map((u, i) => ({ username: u, num: queue.currentIndex() - users.length + i + 1 }));
    const { status, follows } = await runProfileBatch(userId, batch, totalUsers, profileFollows);
    profileFollows = follows;

    if (status === 'stopped' || status === 'session_dead') break;
    if (status === 'ratelimit') {
      const apiWait = xDetector.getWaitMs();
      const ourCd = rateLimiter.getLikeCooldown();
      const cd = apiWait > 0 ? Math.max(apiWait + 30000, ourCd) : ourCd; // +30с запас к API времени
      const source = apiWait > 0 ? 'X API reset' : 'расчётный';
      log('Кулдаун ' + rateLimiter.formatCooldown(cd) + ' (' + source + ')...', 'WARN', userId);
      tg('⏳ Кулдаун: ' + rateLimiter.formatCooldown(cd) + ' (' + source + ')\nПрофиль: ' + userId.slice(-4) + '\nСброс: ' + (apiWait > 0 ? new Date(xDetector.lastReset * 1000).toLocaleTimeString() : '—'));
      await wait([cd, cd]);
      log('Кулдаун завершён', 'INFO', userId);
      break;
    }
    if (queue.remaining() > 0) await wait(CONFIG.DELAY.betweenStart);
  }

  log('Итого подписок за профиль ' + userId.slice(-4) + ': ' + profileFollows + '/' + CONFIG.MAX_FOLLOWS_PER_PROFILE, 'INFO', userId);
}

(async () => {
  console.log('\nx.com DM Checker\n');

  let allUsers;
  try { allUsers = loadUsers(); }
  catch (e) { log(e.message, 'ERROR'); process.exit(1); }

  const doneBefore = loadDone();
  const dmOpenPrev = readLines(OPEN_DM_FILE).length;
  const dmClosedPrev = readLines(CLOSED_DM_FILE).length;
  const checkQueue = createQueue(buildQueue(allUsers));

  if (checkQueue.remaining() > 0) {
    banner('ФАЗА 1: ПРОВЕРКА DM (' + checkQueue.remaining() + ' юзеров, ' + PROFILES.length + ' профилей параллельно)');
    tg('<b>ФАЗА 1: Проверка DM</b>\n\nВсего: ' + allUsers.length + '\nУже проверено: ' + doneBefore.size + '\nDM открыты: ' + dmOpenPrev + '\nDM закрыты: ' + dmClosedPrev + '\nВ очереди: ' + checkQueue.remaining() + '\nПрофилей: ' + PROFILES.length + ' (параллельно)');

    const checkPromises = PROFILES.map((userId, i) => {
      const profileNum = i + 1;
      log('Запуск профиля ' + profileNum + ' для проверки DM', 'INFO', userId);
      return runProfile(userId, checkQueue, allUsers.length).catch((e) => {
        log('Профиль ' + profileNum + ' ошибка: ' + e.message, 'ERROR', userId);
      });
    });

    await Promise.all(checkPromises);

    const openDMAfterCheck = readLines(OPEN_DM_FILE).length;
    const closedDMAfterCheck = readLines(CLOSED_DM_FILE).length;
    banner('ФАЗА 1 ЗАВЕРШЕНА | DM открыты: ' + openDMAfterCheck + ' | DM закрыты: ' + closedDMAfterCheck);
    tg('<b>ФАЗА 1 завершена</b>\n\nDM открыты: ' + openDMAfterCheck + '\nDM закрыты: ' + closedDMAfterCheck + '\nВремя: ' + elapsed() + '\n\nПереход к фазе 2 — лайки');
  } else {
    log('Все DM уже проверены — переход к лайкам');
  }

  if (stopping) { logStream.end(); return; }

  MODE = 'like';

  const likeQueue = createQueue(buildQueue(allUsers));

  if (likeQueue.remaining() > 0 && !stopping) {
    burnedProfiles.clear();
    for (const k of Object.keys(profileRLCount)) delete profileRLCount[k];

    banner('ФАЗА 2: ЛАЙКИ + ПОДПИСКИ (' + likeQueue.remaining() + ' юзеров с закрытыми DM)');
    tg('<b>ФАЗА 2: Лайки + подписки</b>\n\nЮзеров: ' + likeQueue.remaining() + '\n\n<b>Лимиты:</b>\nЛайки: ' + rateLimiter.safeLikesPerDay + '/день\nПодписки: ' + rateLimiter.safeFollowsPerDay + '/день');

    let profileIdx = 0;

    while (likeQueue.remaining() > 0 && !stopping) {
      const userId = PROFILES[profileIdx % PROFILES.length];
      const profileNum = (profileIdx % PROFILES.length) + 1;

      if (burnedProfiles.size >= PROFILES.length) {
        log('ВСЕ ПРОФИЛИ ЗАБЛОКИРОВАНЫ — завершаю', 'ERROR');
        tg('🚫 <b>ВСЕ ПРОФИЛИ ЗАБЛОКИРОВАНЫ</b>\nОсталось: ' + likeQueue.remaining());
        break;
      }

      if (burnedProfiles.has(userId)) {
        profileIdx++;
        continue;
      }

      banner('ПРОФИЛЬ ' + profileNum + ' (' + userId.slice(-4) + ') — лайки');
      await runProfile(userId, likeQueue, allUsers.length);

      const doneSize = loadDone().size;
      tg('<b>Профиль ' + profileNum + '</b>\n\nСделано: ' + doneSize + '/' + allUsers.length + '\nОсталось: ' + likeQueue.remaining() + '\nETA: ' + eta(likeQueue.remaining()) + '\n\n' + rateLimiter.status());

      profileIdx++;
      if (likeQueue.remaining() > 0 && !stopping) await wait(CONFIG.DELAY.betweenStart);
    }
  } else {
    log('Нет юзеров с закрытыми DM для лайков');
  }

  const doneNow = loadDone();
  const failedNow = loadFailed();
  const openDMNow = readLines(OPEN_DM_FILE).length;
  const closedDMNow = readLines(CLOSED_DM_FILE).length;
  const status = stopping ? 'ОСТАНОВЛЕНО' : 'ВСЁ ГОТОВО';
  banner(status + ' | DM открыты: ' + openDMNow + ' | DM закрыты: ' + closedDMNow + ' | подписок: ' + statFollows + ' | ошибки: ' + failedNow.size);
  tg('<b>DM Checker ' + (stopping ? 'СТОП' : 'ГОТОВО') + '</b>\n\nDM открыты: ' + openDMNow + '\nDM закрыты (лайкнуты): ' + doneNow.size + '\nПодписок: ' + statFollows + '\nОшибки: ' + failedNow.size + '\nВремя: ' + elapsed() + '\n\n' + rateLimiter.status());
  logStream.end();
})();