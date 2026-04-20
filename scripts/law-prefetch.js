#!/usr/bin/env node
const https = require('https');

const OC = process.env.LAW_OC || 'STEP-OPENAPI';
const WORKER_URL = process.env.WORKER_URL || 'https://band-archive-api.cm99i.workers.dev';
const TOKEN = process.env.LAW_CACHE_TOKEN;

const DEFAULTS = [
  { name: '근로기준법', mst: '226223' },
  { name: '최저임금법', mst: '226179' },
  { name: '남녀고용평등법', mst: '226228' },
  { name: '근로자퇴직급여 보장법', mst: '226218' },
  { name: '산업안전보건법', mst: '226183' },
  { name: '산업재해보상보험법', mst: '226181' },
  { name: '고용보험법', mst: '226166' },
  { name: '기간제 및 단시간근로자 보호 등에 관한 법률', mst: '226222' },
  { name: '파견근로자 보호 등에 관한 법률', mst: '226186' },
  { name: '직업안정법', mst: '226191' },
  { name: '고용상 연령차별금지 및 고령자고용촉진에 관한 법률', mst: '226164' },
  { name: '장애인고용촉진 및 직업재활법', mst: '226220' },
];

const LAW_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'application/json, text/plain, */*',
  'Referer': 'https://www.law.go.kr/',
  'Accept-Language': 'ko-KR,ko;q=0.9',
};

function get(url, headers = {}) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { ...LAW_HEADERS, ...headers } }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    }).on('error', reject);
  });
}

function post(url, body, token) {
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname, path: u.pathname, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Cache-Token': token, 'Content-Length': Buffer.byteLength(payload) },
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

function fmtDate(d) {
  const s = String(d || '');
  return s.length === 8 ? `${s.slice(0,4)}.${s.slice(4,6)}.${s.slice(6,8)}` : s;
}

function stripCdata(s) { return s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1'); }
function strip(s) {
  return stripCdata(s).replace(/<[^>]+>/g, '').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&nbsp;/g,' ').trim();
}
function xtag(xml, t) { const m = xml.match(new RegExp(`<${t}[^>]*>([\\s\\S]*?)<\\/${t}>`, 'i')); return m ? m[1] : ''; }

function xmlToHtml(xml) {
  const units = [...xml.matchAll(/<조문단위[^>]*>([\s\S]*?)<\/조문단위>/gi)];
  if (units.length) {
    return units.map(u => {
      const num   = strip(xtag(u[1], '조문번호'));
      const title = strip(xtag(u[1], '조문제목'));
      const body  = strip(xtag(u[1], '조문내용'));
      const paras = [...u[1].matchAll(/<항[^>]*>([\s\S]*?)<\/항>/gi)].map(a => {
        const pn = strip(xtag(a[1], '항번호'));
        const pc = strip(xtag(a[1], '항내용'));
        return pn||pc ? `<div style="margin:4px 0 0 14px;line-height:1.7;color:#333">${pn} ${pc}</div>` : '';
      }).join('');
      const hasBody = body || paras;
      const bodyHtml = hasBody ? `<div class="la-body" style="display:none;padding:10px 0 4px;border-top:1px solid #e8e8e8;margin-top:8px">${body?`<div style="line-height:1.8;font-size:14px;color:#222">${body}</div>`:''}${paras}</div>` : '';
      return `<div class="la-item" onclick="toggleLawArticle(this)" style="padding:11px 14px;border-left:3px solid #37c272;margin:6px 0;background:#f9fafb;border-radius:0 6px 6px 0;cursor:${hasBody?'pointer':'default'}">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <strong style="font-size:14px;color:#111">제${num}조${title?` <span style="font-weight:400;color:#555">(${title})</span>`:''}</strong>
          ${hasBody?`<span class="la-arr" style="font-size:11px;color:#aaa;transition:transform .2s">▼</span>`:''}
        </div>${bodyHtml}
      </div>`;
    }).join('');
  }
  // 조문단위 없으면 텍스트 블록으로 폴백
  const chunks = [...xml.matchAll(/<조문내용[^>]*>([\s\S]*?)<\/조문내용>/gi)];
  if (chunks.length) {
    return chunks.map(c => `<div style="padding:10px 14px;border-bottom:1px solid #eee;font-size:14px;line-height:1.8">${strip(c[1])}</div>`).join('');
  }
  return '';
}

async function main() {
  if (!TOKEN) { console.error('LAW_CACHE_TOKEN 미설정'); process.exit(1); }
  const items = [];

  for (const law of DEFAULTS) {
    // 1. 검색 결과 캐시
    try {
      const enc = encodeURIComponent(law.name);
      const r = await get(`https://www.law.go.kr/DRF/lawSearch.do?OC=${OC}&target=law&type=JSON&query=${enc}&display=10&sort=efYd`);
      if (r.status === 200) {
        const d = JSON.parse(r.body);
        const root = d?.LawSearch || {};
        const arr = root.law || [];
        const laws = (Array.isArray(arr) ? arr : arr ? [arr] : []).map(l => {
          const link = l['법령상세링크'] || '';
          const mstFromLink = (link.match(/MST=(\d+)/) || [])[1] || '';
          return { name: l['법령명한글']||l['법령명']||'', dept: l['소관부처명']||'', date: fmtDate(l['시행일자']||l['공포일자']||''), id: l['법령ID']||l['법령일련번호']||'', mst: mstFromLink||l['법령일련번호']||'' };
        }).filter(l => l.name);
        if (laws.length) {
          const key = `law3_${OC}_law_${law.name.slice(0,30)}`;
          items.push({ key, data: { laws, precs: [], expcs: [], query: law.name, oc: OC, debug: [] } });
          console.log(`✅ 검색캐시: ${law.name} (${laws.length}건)`);

          // 검색 결과의 실제 MST로 본문 캐시 (상위 2개)
          for (const found of laws.slice(0, 2)) {
            const realMst = found.mst;
            if (!realMst) continue;
            try {
              await new Promise(r => setTimeout(r, 300));
              const cr = await get(`https://www.law.go.kr/DRF/lawService.do?OC=${OC}&target=law&MST=${realMst}&type=XML`, { Accept: 'application/xml,text/xml,*/*' });
              if (cr.status === 200 && cr.body.length > 200) {
                const html = xmlToHtml(cr.body);
                if (html.length > 50) {
                  items.push({ key: `lawxml1_law_${realMst}`, data: { html, name: found.name, lawtype: 'law' } });
                  // MST로도 캐시 (id 기반 접근 대비)
                  if (found.id) items.push({ key: `lawxml1_law_${found.id}`, data: { html, name: found.name, lawtype: 'law' } });
                  console.log(`  ✅ 본문캐시: ${found.name} (MST:${realMst})`);
                }
              }
            } catch(e) { console.warn(`  ⚠️  본문 실패: ${found.name}`, e.message); }
          }
        }
      }
    } catch(e) { console.warn(`⚠️  검색 실패: ${law.name}`, e.message); }

    await new Promise(r => setTimeout(r, 500)); // rate limit 방지
  }

  if (!items.length) { console.error('캐시할 데이터 없음'); process.exit(1); }

  const res = await post(`${WORKER_URL}/api/admin/law-cache`, items, TOKEN);
  console.log(`📦 Worker 저장 완료: ${res.status}`, res.body);
}

main().catch(e => { console.error(e); process.exit(1); });
