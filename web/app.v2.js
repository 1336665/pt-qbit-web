const api = async (path, options = {}) => {
  const response = await fetch(`/api${path}`, {
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {})
    },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || payload.message || `HTTP ${response.status}`);
  }
  if (response.status === 204) return null;
  return response.json();
};

const state = {
  page: 'dashboard',
  instances: [],
  sites: []
};

const pages = [
  { key: 'dashboard', label: '仪表盘' },
  { key: 'instances', label: 'qB实例' },
  { key: 'torrents', label: '种子管理' },
  { key: 'sites', label: 'PT站点' },
  { key: 'speed-rules', label: '限速规则' },
  { key: 'remove-rules', label: '删种规则' },
  { key: 'logs', label: '运行日志' },
  { key: 'settings', label: '系统设置' }
];

const elements = {
  sidebar: document.getElementById('sidebar'),
  content: document.getElementById('content'),
  title: document.getElementById('page-title'),
  status: document.getElementById('status-message')
};

const setStatus = (message, type = 'info') => {
  if (!elements.status) return;
  elements.status.textContent = message;
  elements.status.className = `status-message ${type}`;
};

const clearStatus = () => {
  if (!elements.status) return;
  elements.status.textContent = '';
  elements.status.className = 'status-message';
};

const formatBytes = (value = 0) => {
  if (value === 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let idx = 0;
  let num = value;
  while (num >= 1024 && idx < units.length - 1) {
    num /= 1024;
    idx += 1;
  }
  return `${num.toFixed(2)} ${units[idx]}`;
};

const formatSpeed = (value = 0) => `${formatBytes(value)}/s`;

const createButton = (label, onClick, variant = 'primary') => {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = `btn ${variant}`;
  button.textContent = label;
  button.addEventListener('click', onClick);
  return button;
};

const createSection = (title) => {
  const section = document.createElement('section');
  section.className = 'panel';
  const header = document.createElement('div');
  header.className = 'panel-header';
  const heading = document.createElement('h2');
  heading.textContent = title;
  header.appendChild(heading);
  section.appendChild(header);
  return { section, header };
};

const renderTable = (columns, rows) => {
  const table = document.createElement('table');
  table.className = 'data-table';
  const thead = document.createElement('thead');
  const tr = document.createElement('tr');
  columns.forEach((col) => {
    const th = document.createElement('th');
    th.textContent = col;
    tr.appendChild(th);
  });
  thead.appendChild(tr);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach((row) => tbody.appendChild(row));
  table.appendChild(tbody);

  return table;
};

const renderDashboard = async () => {
  const { section } = createSection('仪表盘');
  const grid = document.createElement('div');
  grid.className = 'stat-grid';

  const data = await api('/dashboard');

  const stats = [
    { label: '总上传速度', value: formatSpeed(data.total_up_speed || 0) },
    { label: '总下载速度', value: formatSpeed(data.total_dl_speed || 0) },
    { label: '活跃种子', value: data.total_torrents ?? 0 },
    { label: '累计上传', value: formatBytes(data.total_uploaded || 0) },
    { label: '限速中', value: data.limit_count ?? 0 },
    { label: '删种记录', value: data.removed_count ?? 0 }
  ];

  stats.forEach((stat) => {
    const card = document.createElement('div');
    card.className = 'stat-card';
    const label = document.createElement('div');
    label.className = 'stat-label';
    label.textContent = stat.label;
    const value = document.createElement('div');
    value.className = 'stat-value';
    value.textContent = stat.value;
    card.append(label, value);
    grid.appendChild(card);
  });

  section.appendChild(grid);
  return section;
};

const loadInstances = async () => {
  state.instances = await api('/qb/instances');
  return state.instances;
};

const renderInstances = async () => {
  const { section, header } = createSection('qB 实例');
  const refresh = createButton('刷新', () => navigate('instances'), 'ghost');
  header.appendChild(refresh);

  const instances = await loadInstances();
  const rows = instances.map((inst) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${inst.name || inst.id}</td>
      <td>${inst.host || '-'}</td>
      <td>${inst.connected ? '已连接' : '离线'}</td>
      <td>${inst.enabled ? '启用' : '停用'}</td>
      <td class="table-actions"></td>
    `;

    const actions = row.querySelector('.table-actions');
    const connectBtn = createButton(inst.connected ? '断开' : '连接', async () => {
      try {
        await api(`/qb/instances/${inst.id}/${inst.connected ? 'disconnect' : 'connect'}`, { method: 'POST' });
        setStatus('操作成功', 'success');
        navigate('instances');
      } catch (error) {
        setStatus(error.message, 'error');
      }
    }, inst.connected ? 'danger' : 'primary');

    actions.appendChild(connectBtn);
    return row;
  });

  section.appendChild(renderTable(['名称', '地址', '状态', '启用', '操作'], rows));
  return section;
};

const renderTorrents = async () => {
  const { section, header } = createSection('种子管理');

  const instances = state.instances.length ? state.instances : await loadInstances();
  const selector = document.createElement('select');
  selector.className = 'select';
  instances.forEach((inst) => {
    const option = document.createElement('option');
    option.value = inst.id;
    option.textContent = inst.name || `实例 ${inst.id}`;
    selector.appendChild(option);
  });

  const toolbar = document.createElement('div');
  toolbar.className = 'toolbar';
  toolbar.appendChild(selector);
  toolbar.appendChild(createButton('刷新', () => loadTorrents(Number(selector.value)), 'ghost'));
  header.appendChild(toolbar);

  const tableContainer = document.createElement('div');
  tableContainer.className = 'table-container';
  section.appendChild(tableContainer);

  const loadTorrents = async (instanceId) => {
    if (!instanceId) return;
    try {
      const torrents = await api(`/qb/instances/${instanceId}/torrents`);
      const rows = torrents.map((torrent) => {
        const row = document.createElement('tr');
        row.innerHTML = `
          <td>${torrent.name || torrent.hash}</td>
          <td>${formatBytes(torrent.size || 0)}</td>
          <td>${formatSpeed(torrent.upspeed || 0)}</td>
          <td>${formatSpeed(torrent.dlspeed || 0)}</td>
          <td>${torrent.state || '-'} </td>
          <td class="table-actions"></td>
        `;
        const actions = row.querySelector('.table-actions');
        actions.appendChild(createButton('暂停', () => controlTorrent(instanceId, torrent.hash, 'pause'), 'ghost'));
        actions.appendChild(createButton('继续', () => controlTorrent(instanceId, torrent.hash, 'resume'), 'primary'));
        actions.appendChild(createButton('删除', () => controlTorrent(instanceId, torrent.hash, 'delete'), 'danger'));
        return row;
      });
      tableContainer.innerHTML = '';
      tableContainer.appendChild(renderTable(['名称', '大小', '上传', '下载', '状态', '操作'], rows));
    } catch (error) {
      setStatus(error.message, 'error');
    }
  };

  const controlTorrent = async (instanceId, hash, action) => {
    try {
      if (action === 'delete') {
        await api(`/qb/instances/${instanceId}/torrents/${hash}`, { method: 'DELETE' });
      } else {
        await api(`/qb/instances/${instanceId}/torrents/${hash}/${action}`, { method: 'POST' });
      }
      setStatus('操作成功', 'success');
      loadTorrents(instanceId);
    } catch (error) {
      setStatus(error.message, 'error');
    }
  };

  if (instances.length) {
    await loadTorrents(instances[0].id);
  }

  selector.addEventListener('change', () => loadTorrents(Number(selector.value)));
  return section;
};

const renderSites = async () => {
  const { section, header } = createSection('PT 站点');
  header.appendChild(createButton('刷新', () => navigate('sites'), 'ghost'));

  state.sites = await api('/pt/sites');
  const rows = state.sites.map((site) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${site.name}</td>
      <td>${site.url || '-'}</td>
      <td>${site.tracker_keyword || '-'}</td>
      <td>${site.enabled ? '启用' : '停用'}</td>
    `;
    return row;
  });

  section.appendChild(renderTable(['名称', '地址', 'Tracker 关键字', '启用'], rows));
  return section;
};

const renderSpeedRules = async () => {
  const { section, header } = createSection('限速规则');
  header.appendChild(createButton('刷新', () => navigate('speed-rules'), 'ghost'));

  const rules = await api('/speed/rules');
  const rows = rules.map((rule) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${rule.name}</td>
      <td>${rule.site_name || '全局'}</td>
      <td>${rule.target_speed_kib} KiB/s</td>
      <td>${rule.safety_margin}</td>
      <td>${rule.enabled ? '启用' : '停用'}</td>
    `;
    return row;
  });

  section.appendChild(renderTable(['名称', '站点', '目标速度', '安全边际', '启用'], rows));
  return section;
};

const renderRemoveRules = async () => {
  const { section, header } = createSection('删种规则');
  header.appendChild(createButton('刷新', () => navigate('remove-rules'), 'ghost'));

  const rules = await api('/remove/rules');
  const rows = rules.map((rule) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${rule.name}</td>
      <td>${rule.description || '-'}</td>
      <td>${rule.enabled ? '启用' : '停用'}</td>
    `;
    return row;
  });

  section.appendChild(renderTable(['名称', '描述', '启用'], rows));
  return section;
};

const renderLogs = async () => {
  const { section, header } = createSection('运行日志');
  header.appendChild(createButton('刷新', () => navigate('logs'), 'ghost'));

  const logs = await api('/logs?limit=200');
  const list = document.createElement('div');
  list.className = 'log-list';
  logs.forEach((log) => {
    const item = document.createElement('div');
    item.className = `log-item ${log.level?.toLowerCase() || 'info'}`;
    item.innerHTML = `<span>${log.created_at}</span><span>${log.level}</span><span>${log.message}</span>`;
    list.appendChild(item);
  });

  section.appendChild(list);
  return section;
};

const renderSettings = async () => {
  const { section, header } = createSection('系统设置');
  header.appendChild(createButton('刷新', () => navigate('settings'), 'ghost'));

  const config = await api('/config');
  const block = document.createElement('pre');
  block.className = 'code-block';
  block.textContent = JSON.stringify(config, null, 2);
  section.appendChild(block);
  return section;
};

const pageRenderers = {
  dashboard: renderDashboard,
  instances: renderInstances,
  torrents: renderTorrents,
  sites: renderSites,
  'speed-rules': renderSpeedRules,
  'remove-rules': renderRemoveRules,
  logs: renderLogs,
  settings: renderSettings
};

const navigate = async (page) => {
  state.page = page;
  if (elements.title) {
    elements.title.textContent = pages.find((p) => p.key === page)?.label || '';
  }
  clearStatus();
  elements.content.innerHTML = '<div class="loading">加载中...</div>';
  try {
    const renderer = pageRenderers[page];
    const view = renderer ? await renderer() : document.createElement('div');
    elements.content.innerHTML = '';
    elements.content.appendChild(view);
  } catch (error) {
    elements.content.innerHTML = '';
    setStatus(error.message, 'error');
  }
  updateSidebar();
};

const updateSidebar = () => {
  if (!elements.sidebar) return;
  elements.sidebar.querySelectorAll('button').forEach((button) => {
    button.classList.toggle('active', button.dataset.page === state.page);
  });
};

const initSidebar = () => {
  if (!elements.sidebar) return;
  elements.sidebar.innerHTML = '';
  pages.forEach((page) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'nav-button';
    button.textContent = page.label;
    button.dataset.page = page.key;
    button.addEventListener('click', () => navigate(page.key));
    elements.sidebar.appendChild(button);
  });
};

const initLogout = () => {
  const logoutBtn = document.getElementById('logout-btn');
  if (!logoutBtn) return;
  logoutBtn.addEventListener('click', async () => {
    try {
      await api('/logout', { method: 'POST' });
      window.location.href = '/login';
    } catch (error) {
      setStatus(error.message, 'error');
    }
  });
};

const init = async () => {
  initSidebar();
  initLogout();
  await navigate(state.page);
};

document.addEventListener('DOMContentLoaded', init);
