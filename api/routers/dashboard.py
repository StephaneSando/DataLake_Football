"""
Endpoint /dashboard - Tableau de bord de visualisation du data lake Football.
Accessible sur http://localhost:8000/dashboard
"""
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Football Data Lake . Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0c12;
    --surface: #12151f;
    --surface-2: #191d2b;
    --surface-3: #1f2434;
    --border: #262b3d;
    --border-soft: #1c202e;
    --text: #eef0f6;
    --text-dim: #9095ab;
    --text-faint: #565c76;
    --accent: #8b7bf7;
    --accent-soft: #8b7bf722;
    --green: #3ddc97;
    --green-soft: #3ddc9722;
    --amber: #fbbf5b;
    --amber-soft: #fbbf5b22;
    --red: #f7768e;
    --red-soft: #f7768e22;
    --radius-lg: 16px;
    --radius-md: 10px;
    --radius-sm: 7px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Inter', -apple-system, sans-serif;
    background:
      radial-gradient(900px 500px at 15% -10%, #8b7bf714, transparent),
      radial-gradient(700px 400px at 100% 0%, #3ddc9710, transparent),
      var(--bg);
    color: var(--text);
    min-height: 100vh;
    -webkit-font-smoothing: antialiased;
  }

  header {
    position: sticky; top: 0; z-index: 10;
    background: rgba(10,12,18,0.85);
    backdrop-filter: blur(14px);
    border-bottom: 1px solid var(--border-soft);
    padding: 16px 36px;
    display: flex; align-items: center; gap: 14px;
  }
  .logo {
    width: 36px; height: 36px; border-radius: 10px;
    background: linear-gradient(135deg, var(--accent), #6a5cf5);
    display: flex; align-items: center; justify-content: center;
    font-size: 17px;
    box-shadow: 0 4px 14px -4px #8b7bf760;
  }
  header h1 { font-size: 16px; font-weight: 700; letter-spacing: -0.2px; }
  header .subtitle { font-size: 12px; color: var(--text-faint); margin-top: 1px; }
  header .titles { flex: 1; }

  .status-pill {
    display: flex; align-items: center; gap: 7px;
    background: var(--surface-2); border: 1px solid var(--border);
    padding: 6px 13px; border-radius: 20px; font-size: 12px; font-weight: 500;
    color: var(--text-dim);
  }
  .status-pill .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--text-faint); }
  .status-pill.ok .dot { background: var(--green); box-shadow: 0 0 8px 1px #3ddc9770; animation: pulse 2s ease-in-out infinite; }
  .status-pill.ok { color: var(--green); border-color: #3ddc9740; }
  .status-pill.err .dot { background: var(--red); }
  .status-pill.err { color: var(--red); border-color: #f7768e40; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

  .container { max-width: 1320px; margin: 0 auto; padding: 32px 36px 60px; }

  .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }
  .stat-card {
    position: relative;
    background: var(--surface); border: 1px solid var(--border-soft);
    border-radius: var(--radius-lg); padding: 20px 22px;
    overflow: hidden; transition: border-color .2s, transform .2s;
  }
  .stat-card:hover { border-color: var(--border); transform: translateY(-1px); }
  .stat-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--accent-bar, var(--accent));
  }
  .stat-card.c-green  { --accent-bar: var(--green); }
  .stat-card.c-amber  { --accent-bar: var(--amber); }
  .stat-card .top-row { display: flex; align-items: center; gap: 9px; margin-bottom: 14px; }
  .stat-card .icon-badge {
    width: 30px; height: 30px; border-radius: 9px;
    display: flex; align-items: center; justify-content: center; font-size: 14px;
    background: var(--surface-3);
  }
  .stat-card .label { font-size: 12px; color: var(--text-dim); font-weight: 500; }
  .stat-card .value { font-size: 30px; font-weight: 800; letter-spacing: -0.5px; line-height: 1; }
  .stat-card .sub { font-size: 12px; color: var(--text-faint); margin-top: 8px; }

  .tabs {
    display: inline-flex; gap: 3px; padding: 4px;
    background: var(--surface); border: 1px solid var(--border-soft);
    border-radius: 12px; margin-bottom: 20px;
  }
  .tab {
    padding: 9px 18px; cursor: pointer; color: var(--text-dim);
    font-size: 13px; font-weight: 500; border-radius: 9px;
    transition: all .15s; user-select: none;
  }
  .tab:hover { color: var(--text); }
  .tab.active { color: #fff; background: var(--accent); box-shadow: 0 2px 10px -2px #8b7bf760; }

  .panel { display: none; }
  .panel.active { display: block; animation: fadein .25s ease; }
  @keyframes fadein { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }

  .toolbar { display: flex; align-items: center; gap: 12px; margin-bottom: 14px; }
  .toolbar input {
    background: var(--surface); border: 1px solid var(--border-soft);
    color: var(--text); padding: 9px 14px; border-radius: var(--radius-md);
    width: 260px; font-size: 13px; outline: none; font-family: inherit;
    transition: border-color .15s;
  }
  .toolbar input:focus { border-color: var(--accent); }
  .toolbar input::placeholder { color: var(--text-faint); }
  .toolbar .count { color: var(--text-faint); font-size: 12px; margin-left: auto; }

  .table-wrap {
    border: 1px solid var(--border-soft); border-radius: var(--radius-lg);
    overflow: hidden; background: var(--surface);
  }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead tr { background: var(--surface-2); }
  th {
    padding: 12px 16px; text-align: left; color: var(--text-faint);
    font-weight: 600; font-size: 10.5px; text-transform: uppercase;
    letter-spacing: .6px; white-space: nowrap;
  }
  td { padding: 11px 16px; border-top: 1px solid var(--border-soft); color: #cfd3e2; }
  tbody tr { transition: background .12s; }
  tbody tr:hover td { background: var(--surface-2); }

  .badge { padding: 3px 10px; border-radius: 6px; font-size: 11px; font-weight: 700; letter-spacing: .3px; }
  .badge-H { background: var(--green-soft); color: var(--green); }
  .badge-D { background: var(--amber-soft); color: var(--amber); }
  .badge-A { background: var(--accent-soft); color: #b3a8fb; }
  .verdict { font-size: 15px; }
  .correct { color: var(--green); }
  .wrong   { color: var(--red); }

  .prob-wrap { display: flex; flex-direction: column; gap: 4px; }
  .prob-bar { display: flex; height: 5px; border-radius: 3px; overflow: hidden; width: 120px; gap: 2px; background: var(--surface-3); }
  .prob-bar .ph { background: var(--green); }
  .prob-bar .pd { background: var(--amber); }
  .prob-bar .pa { background: var(--accent); }
  .prob-labels { font-size: 10.5px; color: var(--text-faint); display: flex; gap: 8px; }

  .file-row { display: flex; align-items: center; padding: 11px 16px; border-top: 1px solid var(--border-soft); gap: 13px; }
  .file-row:first-child { border-top: none; }
  .file-row:hover { background: var(--surface-2); }
  .file-icon {
    width: 26px; height: 26px; border-radius: 7px; background: var(--surface-3);
    display: flex; align-items: center; justify-content: center; font-size: 12px; flex-shrink: 0;
  }
  .file-key { color: var(--text-dim); font-size: 12.5px; font-family: 'SF Mono', monospace; flex: 1; }
  .file-meta { color: var(--text-faint); font-size: 11.5px; white-space: nowrap; }

  .pagination { display: flex; align-items: center; gap: 10px; margin-top: 16px; justify-content: flex-end; }
  .pagination button {
    background: var(--surface); border: 1px solid var(--border-soft); color: var(--text-dim);
    width: 32px; height: 32px; border-radius: var(--radius-sm); cursor: pointer;
    font-size: 14px; transition: all .15s; display: flex; align-items: center; justify-content: center;
  }
  .pagination button:hover:not(:disabled) { border-color: var(--accent); color: var(--accent); }
  .pagination button:disabled { opacity: .3; cursor: not-allowed; }
  .pagination .page-info { color: var(--text-faint); font-size: 12px; min-width: 70px; text-align: center; }

  .empty { text-align: center; padding: 48px; color: var(--text-faint); font-size: 13px; }
</style>
</head>
<body>

<header>
  <div class="logo">\U000026BD</div>
  <div class="titles">
    <h1>Football Data Lake</h1>
    <div class="subtitle">Premier League . Raw / Staging / Curated</div>
  </div>
  <span id="health-badge" class="status-pill"><span class="dot"></span>Connexion...</span>
</header>

<div class="container">

  <div class="stats-grid">
    <div class="stat-card">
      <div class="top-row"><div class="icon-badge">\U0001F5C4\U0000FE0F</div><div class="label">Raw . Fichiers MinIO</div></div>
      <div class="value" id="s-files">-</div>
      <div class="sub" id="s-size">-</div>
    </div>
    <div class="stat-card">
      <div class="top-row"><div class="icon-badge">\U0001F4CA</div><div class="label">Staging . Matchs</div></div>
      <div class="value" id="s-matches">-</div>
      <div class="sub" id="s-seasons">-</div>
    </div>
    <div class="stat-card">
      <div class="top-row"><div class="icon-badge">\U0001F916</div><div class="label">Curated . Predictions</div></div>
      <div class="value" id="s-preds">-</div>
      <div class="sub" id="s-model">-</div>
    </div>
    <div class="stat-card c-green">
      <div class="top-row"><div class="icon-badge">\U0001F3AF</div><div class="label">Accuracy XGBoost</div></div>
      <div class="value" id="s-acc">-</div>
      <div class="sub">Classification H / D / A</div>
    </div>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('staging')">Staging</div>
    <div class="tab" onclick="switchTab('curated')">Curated</div>
    <div class="tab" onclick="switchTab('raw')">Raw</div>
  </div>

  <div class="panel active" id="panel-staging">
    <div class="toolbar">
      <input type="text" id="staging-search" placeholder="Filtrer par equipe..." oninput="loadStaging(0)">
      <span class="count" id="staging-count"></span>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Date</th><th>Domicile</th><th>Exterieur</th><th>Score</th>
          <th>Resultat</th><th>Forme dom.</th><th>Forme ext.</th>
          <th>Moy. buts D</th><th>Moy. buts E</th><th>Saison</th>
        </tr></thead>
        <tbody id="staging-body"><tr><td colspan="10" class="empty">Chargement...</td></tr></tbody>
      </table>
    </div>
    <div class="pagination">
      <button onclick="stagingPage(-1)" id="btn-sp">&#8249;</button>
      <span class="page-info" id="staging-pi">-</span>
      <button onclick="stagingPage(1)" id="btn-sn">&#8250;</button>
    </div>
  </div>

  <div class="panel" id="panel-curated">
    <div class="toolbar">
      <input type="text" id="curated-search" placeholder="Filtrer par equipe..." oninput="loadCurated(0)">
      <span class="count" id="curated-count"></span>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Date</th><th>Domicile</th><th>Exterieur</th>
          <th>Reel</th><th>Predit</th><th></th>
          <th>Probabilites (H / D / A)</th><th>Modele</th>
        </tr></thead>
        <tbody id="curated-body"><tr><td colspan="8" class="empty">Chargement...</td></tr></tbody>
      </table>
    </div>
    <div class="pagination">
      <button onclick="curatedPage(-1)" id="btn-cp">&#8249;</button>
      <span class="page-info" id="curated-pi">-</span>
      <button onclick="curatedPage(1)" id="btn-cn">&#8250;</button>
    </div>
  </div>

  <div class="panel" id="panel-raw">
    <div class="toolbar">
      <span style="font-size:13px;color:var(--text-faint)">Fichiers stockes dans le bucket raw-football</span>
      <span class="count" id="raw-count"></span>
    </div>
    <div class="table-wrap" id="raw-list">
      <div class="empty">Chargement...</div>
    </div>
  </div>

</div>

<script>
const PAGE = 50;
let stOff = 0, cuOff = 0;

async function api(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
  return r.json();
}

function badge(r) {
  return r ? `<span class="badge badge-${r}">${r}</span>` : '-';
}

function probBar(ph, pd, pa) {
  const h = Math.round(ph * 100), d = Math.round(pd * 100), a = Math.round(pa * 100);
  return `<div class="prob-wrap">
    <div class="prob-bar">
      <div class="ph" style="width:${h}%"></div>
      <div class="pd" style="width:${d}%"></div>
      <div class="pa" style="width:${a}%"></div>
    </div>
    <span class="prob-labels"><span>${h}%</span><span>${d}%</span><span>${a}%</span></span>
  </div>`;
}

function fmt(v, dec=1) { return v != null ? (+v).toFixed(dec) : '-'; }

async function loadHealth() {
  const el = document.getElementById('health-badge');
  try {
    const d = await api('/health');
    const ok = d.overall === 'healthy';
    el.innerHTML = '<span class="dot"></span>' + (ok ? 'Services actifs' : 'Service degrade');
    el.className = 'status-pill ' + (ok ? 'ok' : 'err');
  } catch {
    el.innerHTML = '<span class="dot"></span>API inaccessible';
    el.className = 'status-pill err';
  }
}

async function loadStats() {
  try {
    const d = await api('/stats');
    const z = d.zones;
    document.getElementById('s-files').textContent   = z.raw?.nb_files ?? '-';
    document.getElementById('s-size').textContent    = (z.raw?.total_size_mb ?? '-') + ' MB dans MinIO';
    document.getElementById('s-matches').textContent = z.staging?.nb_matches ?? '-';
    document.getElementById('s-seasons').textContent =
      (z.staging?.nb_seasons ?? '-') + ' saisons . ' + (z.staging?.nb_teams ?? '-') + ' equipes';
    document.getElementById('s-preds').textContent   = z.curated?.nb_predictions ?? '-';
    document.getElementById('s-model').textContent   = 'Modele ' + (z.curated?.latest_model_version ?? '-');
    const acc = z.curated?.model_accuracy;
    document.getElementById('s-acc').textContent     = acc ? (acc * 100).toFixed(1) + '%' : '-';
  } catch(e) { console.warn('Stats:', e); }
}

async function loadStaging(offset) {
  if (offset !== undefined) stOff = offset;
  const team = document.getElementById('staging-search').value.trim();
  let url = `/staging/?limit=${PAGE}&offset=${stOff}`;
  if (team) url += `&team=${encodeURIComponent(team)}`;
  const tbody = document.getElementById('staging-body');
  try {
    const d = await api(url);
    const rows = d.matches;
    document.getElementById('staging-count').textContent = rows.length + ' resultats affiches';
    document.getElementById('staging-pi').textContent = (stOff + 1) + ' a ' + (stOff + rows.length);
    document.getElementById('btn-sp').disabled = stOff === 0;
    document.getElementById('btn-sn').disabled = rows.length < PAGE;

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="empty">Aucun resultat</td></tr>'; return;
    }
    tbody.innerHTML = rows.map(m => `<tr>
      <td>${m.date}</td>
      <td>${m.home_team}</td>
      <td>${m.away_team}</td>
      <td style="font-weight:700">${m.home_goals} - ${m.away_goals}</td>
      <td>${badge(m.result)}</td>
      <td style="color:var(--text-dim)">${fmt(m.home_form, 0)} pts</td>
      <td style="color:var(--text-dim)">${fmt(m.away_form, 0)} pts</td>
      <td style="color:var(--text-faint)">${fmt(m.home_avg_goals_scored)} m / ${fmt(m.home_avg_goals_conceded)} e</td>
      <td style="color:var(--text-faint)">${fmt(m.away_avg_goals_scored)} m / ${fmt(m.away_avg_goals_conceded)} e</td>
      <td style="color:var(--text-faint)">${m.season}</td>
    </tr>`).join('');
  } catch(e) {
    tbody.innerHTML = `<tr><td colspan="10" class="empty">Erreur : ${e.message}</td></tr>`;
  }
}

function stagingPage(dir) { loadStaging(stOff + dir * PAGE); }

async function loadCurated(offset) {
  if (offset !== undefined) cuOff = offset;
  const team = document.getElementById('curated-search').value.trim();
  let url = `/curated/?limit=${PAGE}&offset=${cuOff}`;
  if (team) url += `&home_team=${encodeURIComponent(team)}`;
  const tbody = document.getElementById('curated-body');
  try {
    const d = await api(url);
    const rows = d.predictions;
    document.getElementById('curated-count').textContent =
      rows.length + ' resultats . Accuracy globale ' +
      (d.global_accuracy != null ? (d.global_accuracy * 100).toFixed(1) + '%' : '-');
    document.getElementById('curated-pi').textContent = (cuOff + 1) + ' a ' + (cuOff + rows.length);
    document.getElementById('btn-cp').disabled = cuOff === 0;
    document.getElementById('btn-cn').disabled = rows.length < PAGE;

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="empty">Aucun resultat</td></tr>'; return;
    }
    tbody.innerHTML = rows.map(p => {
      const ok = p.actual_result === p.predicted_result;
      return `<tr>
        <td>${p.date}</td>
        <td>${p.home_team}</td>
        <td>${p.away_team}</td>
        <td>${badge(p.actual_result)}</td>
        <td>${badge(p.predicted_result)}</td>
        <td class="verdict ${ok ? 'correct' : 'wrong'}">${ok ? '&#10003;' : '&#10007;'}</td>
        <td>${probBar(p.prob_home, p.prob_draw, p.prob_away)}</td>
        <td style="color:var(--text-faint);font-size:11px">${p.model_version ?? '-'}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    tbody.innerHTML = `<tr><td colspan="8" class="empty">Erreur : ${e.message}</td></tr>`;
  }
}

function curatedPage(dir) { loadCurated(cuOff + dir * PAGE); }

async function loadRaw() {
  const container = document.getElementById('raw-list');
  try {
    const d = await api('/raw/?limit=500');
    document.getElementById('raw-count').textContent = d.count + ' fichiers';
    if (!d.files.length) { container.innerHTML = '<div class="empty">Aucun fichier dans MinIO</div>'; return; }

    const icons = { csv: '\U0001F4C4', json: '\U0001F4CB', pkl: '\U0001F916' };
    container.innerHTML = d.files.map(f => {
      const ext = f.key.split('.').pop();
      const icon = icons[ext] || '\U0001F4C1';
      return `<div class="file-row">
        <span class="file-icon">${icon}</span>
        <span class="file-key">${f.key}</span>
        <span class="file-meta">${f.size_kb} KB . ${f.last_modified.slice(0,10)}</span>
      </div>`;
    }).join('');
  } catch(e) {
    container.innerHTML = `<div class="empty">Erreur : ${e.message}</div>`;
  }
}

function switchTab(name) {
  const order = ['staging', 'curated', 'raw'];
  document.querySelectorAll('.tab').forEach((t, i) =>
    t.classList.toggle('active', order[i] === name));
  document.querySelectorAll('.panel').forEach(p =>
    p.classList.toggle('active', p.id === 'panel-' + name));
  if (name === 'raw'     && !document.getElementById('raw-list').querySelector('.file-row')) loadRaw();
  if (name === 'curated' && cuOff === 0) loadCurated(0);
}

loadHealth();
loadStats();
loadStaging(0);
setInterval(() => { loadHealth(); loadStats(); }, 30000);
</script>
</body>
</html>"""

@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    """Tableau de bord de visualisation du data lake Football."""
    return DASHBOARD_HTML