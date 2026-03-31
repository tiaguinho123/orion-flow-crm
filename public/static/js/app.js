/* ═══════════════════════════════════════════════════════════
   App Module — Main application controller
   ═══════════════════════════════════════════════════════════ */

let allLeads = [];
let config = { statuses: [], channels: [] };
let searchTimeout = null;

// ─── Initialization ──────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
    await loadConfig();
    await loadMetrics();
    await loadLeads();
});

// ─── Data Loading ────────────────────────────────────────────

async function loadConfig() {
    try {
        const res = await fetch('/api/config');
        config = await res.json();
        populateFilterDropdowns();
        populateModalDropdowns();
    } catch (e) {
        console.error('Failed to load config:', e);
    }
}

async function loadLeads() {
    try {
        const params = new URLSearchParams();
        const status = document.getElementById('filterStatus').value;
        const canal = document.getElementById('filterCanal').value;
        const categoria = document.getElementById('filterCategoria').value;
        const search = document.getElementById('searchInput').value;
        
        if (status) params.set('status', status);
        if (canal) params.set('canal', canal);
        if (categoria) params.set('categoria', categoria);
        if (search) params.set('search', search);

        const res = await fetch(`/api/leads?${params.toString()}`);
        const data = await res.json();
        allLeads = data.leads;
        renderLeadsTable(allLeads);
    } catch (e) {
        console.error('Failed to load leads:', e);
    }
}

async function loadMetrics() {
    try {
        const res = await fetch('/api/metrics');
        const metrics = await res.json();
        updateMetricCards(metrics);
        renderFunnelChart(metrics.funnel);
        renderDailyChart(metrics.daily_contacts);
        renderChannelChart(metrics.channel_counts);
    } catch (e) {
        console.error('Failed to load metrics:', e);
    }
}

// ─── Metric Cards ────────────────────────────────────────────

function updateMetricCards(metrics) {
    animateValue('metricTotal', metrics.total);
    document.getElementById('metricOpenRate').textContent = `${metrics.open_rate}%`;
    document.getElementById('metricClickRate').textContent = `${metrics.click_rate}%`;
    document.getElementById('metricResponseRate').textContent = `${metrics.response_rate}%`;
    animateValue('metricNegotiation', metrics.in_negotiation);
    animateValue('metricClosed', metrics.closed);
}

function animateValue(elementId, target) {
    const el = document.getElementById(elementId);
    const current = parseInt(el.textContent) || 0;
    if (current === target) {
        el.textContent = target;
        return;
    }
    
    const duration = 600;
    const startTime = performance.now();
    
    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const eased = 1 - Math.pow(1 - progress, 3); // ease-out cubic
        const value = Math.round(current + (target - current) * eased);
        el.textContent = value;
        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }
    requestAnimationFrame(update);
}

// ─── Filters ─────────────────────────────────────────────────

function populateFilterDropdowns() {
    const statusSelect = document.getElementById('filterStatus');
    const canalSelect = document.getElementById('filterCanal');
    
    // Populate status filter
    config.statuses.forEach(status => {
        const opt = document.createElement('option');
        opt.value = status;
        opt.textContent = status;
        statusSelect.appendChild(opt);
    });

    // Populate channel filter
    config.channels.forEach(channel => {
        const opt = document.createElement('option');
        opt.value = channel;
        opt.textContent = channel;
        canalSelect.appendChild(opt);
    });

    // Populate category filter (populated after leads load)
    loadCategoriesFilter();
}

async function loadCategoriesFilter() {
    try {
        const res = await fetch('/api/leads');
        const data = await res.json();
        const categories = [...new Set(data.leads.map(l => l.categoria).filter(Boolean))];
        const select = document.getElementById('filterCategoria');
        categories.sort().forEach(cat => {
            const opt = document.createElement('option');
            opt.value = cat;
            opt.textContent = cat;
            select.appendChild(opt);
        });
    } catch (e) {
        console.error('Error loading categories:', e);
    }
}

function applyFilters() {
    loadLeads();
}

function debounceSearch() {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
        loadLeads();
    }, 300);
}

// ─── Reload Data ─────────────────────────────────────────────

async function reloadData() {
    const btn = document.getElementById('btnReload');
    btn.disabled = true;
    btn.innerHTML = `
        <svg class="loading" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21.5 2v6h-6M2.5 22v-6h6M2 11.5a10 10 0 0 1 18.8-4.2M22 12.5a10 10 0 0 1-18.8 4.2"/>
        </svg>
        Recarregando...
    `;

    try {
        const res = await fetch('/api/reload', { method: 'POST' });
        const data = await res.json();
        if (data.success) {
            showToast(data.message);
            await loadMetrics();
            await loadLeads();
            document.getElementById('lastUpdate').textContent = 
                `Atualizado ${new Date().toLocaleTimeString('pt-BR')}`;
        } else {
            showToast('Erro ao recarregar: ' + data.error);
        }
    } catch (e) {
        showToast('Erro de conexão ao recarregar dados');
    }

    btn.disabled = false;
    btn.innerHTML = `
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21.5 2v6h-6M2.5 22v-6h6M2 11.5a10 10 0 0 1 18.8-4.2M22 12.5a10 10 0 0 1-18.8 4.2"/>
        </svg>
        Recarregar Dados
    `;
}

// ─── Lead Modal ──────────────────────────────────────────────

function populateModalDropdowns() {
    const statusSelect = document.getElementById('modalStatus');
    const canalSelect = document.getElementById('modalCanal');

    statusSelect.innerHTML = '';
    config.statuses.forEach(status => {
        const opt = document.createElement('option');
        opt.value = status;
        opt.textContent = status;
        statusSelect.appendChild(opt);
    });

    canalSelect.innerHTML = '<option value="">Selecione...</option>';
    config.channels.forEach(channel => {
        const opt = document.createElement('option');
        opt.value = channel;
        opt.textContent = channel;
        canalSelect.appendChild(opt);
    });
}

async function openLeadModal(leadId) {
    const modal = document.getElementById('leadModal');
    
    try {
        const res = await fetch(`/api/leads/${leadId}`);
        const lead = await res.json();

        document.getElementById('modalLeadId').value = lead.id;
        document.getElementById('modalTitle').textContent = lead.nome_negocio || 'Lead';
        
        // Info panel (read-only)
        document.getElementById('modalCategoria').textContent = lead.categoria || '—';
        document.getElementById('modalEndereco').textContent = lead.endereco || '—';
        document.getElementById('modalRating').textContent = lead.avaliacao_google ? 
            `${lead.avaliacao_google} ★ (${lead.num_reviews || 0} reviews)` : '—';

        // Editable contact & social fields
        document.getElementById('modalEditTelefone').value = lead.telefone || '';
        document.getElementById('modalEditEmail').value = lead.email || '';
        document.getElementById('modalEditSite').value = lead.site || '';
        document.getElementById('modalEditInstagram').value = lead.instagram || '';
        document.getElementById('modalEditFacebook').value = lead.facebook || '';
        document.getElementById('modalEditTiktok').value = lead.tiktok || '';
        document.getElementById('modalEditYelp').value = lead.yelp || '';

        // Edit panel
        document.getElementById('modalStatus').value = lead.status_lead || 'Novo';
        document.getElementById('modalCanal').value = lead.canal_abordagem || '';
        document.getElementById('modalScore').value = lead.score_prioridade || '';
        document.getElementById('modalDataContato').value = lead.data_contato || '';
        document.getElementById('modalProximoPasso').value = lead.proximo_passo || '';
        document.getElementById('modalEmailAberto').checked = lead.email_aberto == 1;
        document.getElementById('modalEmailClicou').checked = lead.email_clicou == 1;
        document.getElementById('modalNotas').value = lead.notas || '';

        // Initialize notes + research checklist from DB
        initNotesEnhancements();
        await loadResearchChecklist(leadId);

        // Load history
        await loadHistory(leadId);

        // Load generated messages
        await loadLeadMessages(leadId);

        // Populate funnel action zone
        updateFunnelZone(lead);

        modal.classList.add('active');
    } catch (e) {
        console.error('Error loading lead:', e);
        showToast('Erro ao carregar detalhes do lead');
    }
}

function closeModal() {
    document.getElementById('leadModal').classList.remove('active');
}

// Close modal on overlay click
document.addEventListener('click', (e) => {
    if (e.target.id === 'leadModal') {
        closeModal();
    }
});

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});

async function saveLead() {
    const leadId = document.getElementById('modalLeadId').value;
    if (!leadId) return;

    const data = {
        status_lead: document.getElementById('modalStatus').value,
        canal_abordagem: document.getElementById('modalCanal').value,
        score_prioridade: document.getElementById('modalScore').value || null,
        data_contato: document.getElementById('modalDataContato').value,
        proximo_passo: document.getElementById('modalProximoPasso').value,
        email_aberto: document.getElementById('modalEmailAberto').checked ? 1 : 0,
        email_clicou: document.getElementById('modalEmailClicou').checked ? 1 : 0,
        notas: document.getElementById('modalNotas').value,
        // Editable contact & social fields
        telefone: document.getElementById('modalEditTelefone').value,
        email: document.getElementById('modalEditEmail').value,
        site: document.getElementById('modalEditSite').value,
        instagram: document.getElementById('modalEditInstagram').value,
        facebook: document.getElementById('modalEditFacebook').value,
        tiktok: document.getElementById('modalEditTiktok').value,
        yelp: document.getElementById('modalEditYelp').value,
    };

    try {
        const res = await fetch(`/api/leads/${leadId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });

        if (res.ok) {
            showToast('Lead atualizado com sucesso!');
            closeModal();
            await loadMetrics();
            await loadLeads();
        } else {
            showToast('Erro ao salvar lead');
        }
    } catch (e) {
        console.error('Error saving lead:', e);
        showToast('Erro de conexão ao salvar');
    }
}

async function loadHistory(leadId) {
    const container = document.getElementById('historyList');
    
    try {
        const res = await fetch(`/api/leads/${leadId}/history`);
        const data = await res.json();

        if (!data.history || data.history.length === 0) {
            container.innerHTML = '<p class="history-empty">Nenhuma interação registrada.</p>';
            return;
        }

        const fieldLabels = {
            'status_lead': 'Status',
            'canal_abordagem': 'Canal',
            'notas': 'Notas',
            'data_contato': 'Data de Contato',
            'data_resposta': 'Data de Resposta',
            'proximo_passo': 'Próximo Passo',
            'email_aberto': 'Email Aberto',
            'email_clicou': 'Email Clicou',
            'score_prioridade': 'Score',
        };

        let html = '';
        for (const h of data.history) {
            const fieldLabel = fieldLabels[h.field_changed] || h.field_changed;
            const oldVal = h.old_value || '(vazio)';
            const newVal = h.new_value || '(vazio)';
            html += `
                <div class="history-item">
                    <div class="history-date">${h.changed_at}</div>
                    <div class="history-change">
                        <b>${fieldLabel}</b>: ${escapeHtml(oldVal)} → ${escapeHtml(newVal)}
                    </div>
                </div>
            `;
        }
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = '<p class="history-empty">Erro ao carregar histórico.</p>';
    }
}

// ─── Toast Notification ──────────────────────────────────────

function showToast(message) {
    const toast = document.getElementById('toast');
    document.getElementById('toastMessage').textContent = message;
    toast.classList.add('show');
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// ─── Lead Generation ─────────────────────────────────────────

let generatePollInterval = null;

async function generateLeads() {
    try {
        const res = await fetch('/api/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}), // uses saved settings
        });
        const data = await res.json();
        
        if (!res.ok) {
            if (res.status === 400) {
                showToast(data.error || 'Configure as API Keys primeiro');
                window.location.href = '/settings';
                return;
            }
            showToast(data.error || 'Erro ao iniciar geração');
            return;
        }
        
        // Open progress modal
        openGenerateModal();
        
        // Start polling
        pollGenerateStatus();
        generatePollInterval = setInterval(pollGenerateStatus, 1000);
        
    } catch (e) {
        console.error('Error starting generation:', e);
        showToast('Erro de conexão ao iniciar geração de leads');
    }
}

function openGenerateModal() {
    const modal = document.getElementById('generateModal');
    document.getElementById('genPhase').textContent = 'Iniciando...';
    document.getElementById('genProgressFill').style.width = '0%';
    document.getElementById('genCounter').textContent = '0 / 0';
    document.getElementById('genLog').innerHTML = '<div>🚀 Iniciando geração de leads...</div>';
    modal.classList.add('active');
}

function closeGenerateModal() {
    const modal = document.getElementById('generateModal');
    modal.classList.remove('active');
    if (generatePollInterval) {
        clearInterval(generatePollInterval);
        generatePollInterval = null;
    }
    // Refresh dashboard data
    loadMetrics();
    loadLeads();
}

let lastLogCount = 0;

async function pollGenerateStatus() {
    try {
        const res = await fetch('/api/generate/status');
        const status = await res.json();
        
        // Update phase text
        const phaseLabels = {
            'starting': '🔄 Preparando...',
            'apify': '📡 Buscando no Google Maps...',
            'qualifying': '🤖 Qualificando com IA...',
            'saving': '💾 Salvando no banco de dados...',
            'done': '✅ Concluído!',
            'error': '❌ Erro',
        };
        document.getElementById('genPhase').textContent = 
            phaseLabels[status.phase] || status.phase || 'Aguardando...';
        
        // Update progress bar
        const pct = status.total > 0 ? Math.round((status.current / status.total) * 100) : 0;
        document.getElementById('genProgressFill').style.width = 
            status.phase === 'apify' ? '15%' : `${pct}%`;
        
        // Update counter
        document.getElementById('genCounter').textContent = 
            `${status.current} / ${status.total}`;
        
        // Update log (only new entries)
        if (status.log && status.log.length > lastLogCount) {
            const logEl = document.getElementById('genLog');
            const newEntries = status.log.slice(lastLogCount);
            for (const entry of newEntries) {
                const div = document.createElement('div');
                div.textContent = entry;
                logEl.appendChild(div);
            }
            lastLogCount = status.log.length;
            logEl.scrollTop = logEl.scrollHeight;
        }
        
        // Stop polling when done
        if (!status.running && (status.phase === 'done' || status.phase === 'error')) {
            if (generatePollInterval) {
                clearInterval(generatePollInterval);
                generatePollInterval = null;
            }
            
            // Set progress to 100% on done
            if (status.phase === 'done') {
                document.getElementById('genProgressFill').style.width = '100%';
            }
            
            // Show result toast
            if (status.result) {
                const r = status.result;
                showToast(`${r.imported} leads adicionados, ${r.skipped} já existiam, ${r.errors || 0} erros.`);
            }
            
            // Refresh data
            loadMetrics();
            loadLeads();
        }
    } catch (e) {
        console.error('Error polling status:', e);
    }
}

// Also handle generate modal overlay click
document.addEventListener('click', (e) => {
    if (e.target.id === 'generateModal') {
        closeGenerateModal();
    }
});

// ─── Funnel Message System ───────────────────────────────────

let _currentModalLead = null;

const FUNNEL_CONFIG = {
    'New':            { color: '#3b82f6', icon: '🔵', stage: 'first_contact',  action: 'Generate First Contact Message',  next: 'Generate a first-contact message to open the conversation.' },
    'Contacted':      { color: '#8b5cf6', icon: '🟣', stage: null,             action: null,           next: 'Waiting for response. Update status when lead replies.' },
    'Thinking':       { color: '#eab308', icon: '🟡', stage: 'follow_up',      action: 'Generate Follow-up Message',      next: 'Lead is considering. Send a follow-up to build trust.' },
    'Interested':     { color: '#22c55e', icon: '🟢', stage: 'closing',        action: 'Generate Closing Message',        next: 'Lead is interested! Send a closing message to book a call.' },
    'Not Interested': { color: '#ef4444', icon: '🔴', stage: 'recovery',       action: 'Generate Recovery Message',       next: 'Try a recovery message — acknowledge, ask, leave door open.' },
    'Closed':         { color: '#6b7280', icon: '⚫', stage: 'review_request', action: 'Generate Review Request',         next: 'Deal closed! Ask for a Google review and referrals.' },
    'Lost':           { color: '#374151', icon: '💀', stage: null,             action: null,           next: 'Lead archived. No more messages.' },
};

function updateFunnelZone(lead) {
    _currentModalLead = lead;
    const status = lead.status_lead || 'New';
    const config = FUNNEL_CONFIG[status] || FUNNEL_CONFIG['New'];

    // Status badge
    const badge = document.getElementById('funnelStatusBadge');
    badge.innerHTML = `
        <span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:${config.color};"></span>
        <span style="font-size:0.9rem;font-weight:600;color:var(--text-primary);">${config.icon} ${status}</span>
    `;

    // Next step text
    document.getElementById('funnelNextStep').textContent = config.next;

    // 3-day alert for Contacted status
    const alertZone = document.getElementById('funnelAlertZone');
    if (status === 'Contacted' && lead.data_contato) {
        const contactDate = new Date(lead.data_contato);
        const now = new Date();
        const daysDiff = Math.floor((now - contactDate) / (1000 * 60 * 60 * 24));
        if (daysDiff >= 3) {
            alertZone.style.display = 'block';
            alertZone.innerHTML = `
                <div style="background:rgba(234,179,8,0.1);border:1px solid rgba(234,179,8,0.3);border-radius:var(--radius-sm);padding:0.6rem 0.8rem;font-size:0.8rem;color:#eab308;">
                    ⚠️ No response in ${daysDiff} days — consider updating status to "Thinking" and sending a follow-up.
                </div>
            `;
        } else {
            alertZone.style.display = 'none';
        }
    } else {
        alertZone.style.display = 'none';
    }

    // Action button — gated by research progress
    const btnZone = document.getElementById('funnelActionBtn');
    const researchNeeded = _currentResearchProgress < 8;

    if (status === 'Closed') {
        btnZone.innerHTML = `
            <button class="btn btn-primary btn-full" onclick="generateFunnelMessage(${lead.id}, 'review_request')" style="background:linear-gradient(135deg,#6b7280,#8b5cf6);margin-bottom:0.5rem;">
                ⭐ Generate Review Request
            </button>
            <button class="btn btn-primary btn-full" onclick="generateFunnelMessage(${lead.id}, 'referral_request')" style="background:linear-gradient(135deg,#6b7280,#06b6d4);">
                🤝 Generate Referral Request
            </button>
        `;
    } else if (config.action && config.stage) {
        if (researchNeeded) {
            btnZone.innerHTML = `
                <button class="btn btn-primary btn-full" disabled style="opacity:0.5;cursor:not-allowed;background:linear-gradient(135deg,${config.color},#8b5cf6);">
                    🔒 Complete at least 8/16 research items first
                </button>
                <div style="text-align:center;font-size:0.72rem;color:var(--text-muted);margin-top:0.4rem;">${_currentResearchProgress}/16 items researched</div>
            `;
        } else {
            btnZone.innerHTML = `
                <button class="btn btn-primary btn-full" onclick="generateFunnelMessage(${lead.id}, '${config.stage}')" style="background:linear-gradient(135deg,${config.color},#8b5cf6);">
                    ✍️ ${config.action}
                </button>
            `;
        }
    } else if (status === 'Lost') {
        btnZone.innerHTML = `
            <div style="text-align:center;font-size:0.8rem;color:var(--text-muted);padding:0.5rem;">💀 Lead archived — no more messages</div>
        `;
    } else {
        btnZone.innerHTML = `
            <div style="text-align:center;font-size:0.8rem;color:var(--text-muted);padding:0.5rem;">Update the status above when the lead responds.</div>
        `;
    }
}

async function generateFunnelMessage(leadId, stage) {
    const btnZone = document.getElementById('funnelActionBtn');
    const loading = document.getElementById('funnelLoading');

    // Show loading, hide button
    btnZone.style.display = 'none';
    loading.style.display = 'block';

    try {
        const res = await fetch(`/api/leads/${leadId}/generate-message`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ stage }),
        });
        const data = await res.json();

        if (!res.ok) {
            if (res.status === 400 && data.error && data.error.includes('API Key')) {
                showToast(data.error);
                window.location.href = '/settings';
                return;
            }
            if (res.status === 409 && data.already_exists) {
                showToast('Messages already generated for this stage. Check history below.');
            } else {
                showToast(data.error || 'Error generating messages');
            }
            loading.style.display = 'none';
            btnZone.style.display = 'block';
            return;
        }

        showToast(`✅ ${data.messages.length} messages generated!`);

        // Reload messages
        await loadLeadMessages(leadId);

        // Replace button with success message
        loading.style.display = 'none';
        btnZone.style.display = 'block';
        btnZone.innerHTML = `
            <div style="text-align:center;padding:0.5rem;font-size:0.85rem;color:#22c55e;">✅ Messages generated! See history below.</div>
        `;

    } catch (e) {
        console.error('Error generating funnel message:', e);
        showToast('Connection error');
        loading.style.display = 'none';
        btnZone.style.display = 'block';
    }
}

// ─── Lead Messages Display (Collapsible History) ──────────────

let _messagesExpanded = true;

function toggleMessagesHistory() {
    const list = document.getElementById('messagesList');
    const icon = document.getElementById('msgToggleIcon');
    _messagesExpanded = !_messagesExpanded;
    list.style.display = _messagesExpanded ? 'block' : 'none';
    icon.textContent = _messagesExpanded ? '▼' : '▶';
}

async function loadLeadMessages(leadId) {
    const section = document.getElementById('messagesSection');
    const list = document.getElementById('messagesList');

    try {
        const res = await fetch(`/api/leads/${leadId}/messages`);
        const data = await res.json();

        if (!data.messages || data.messages.length === 0) {
            section.style.display = 'none';
            return;
        }

        section.style.display = 'block';
        _messagesExpanded = true;
        document.getElementById('msgToggleIcon').textContent = '▼';
        list.style.display = 'block';

        // Group messages by funnel_stage
        const stages = {};
        for (const msg of data.messages) {
            const key = msg.funnel_stage || 'first_contact';
            if (!stages[key]) stages[key] = [];
            stages[key].push(msg);
        }

        const stageLabels = {
            'first_contact': '🔵 First Contact',
            'follow_up': '🟡 Follow-up',
            'closing': '🟢 Closing',
            'recovery': '🔴 Recovery',
            'review_request': '⚫ Review Request',
        };

        let html = '';
        for (const [stage, msgs] of Object.entries(stages)) {
            const label = stageLabels[stage] || stage;
            const date = msgs[0].created_at ? new Date(msgs[0].created_at).toLocaleDateString() : '';
            html += `<div style="font-size:0.75rem;font-weight:600;color:var(--text-primary);margin:0.75rem 0 0.4rem;border-bottom:1px solid var(--border-color);padding-bottom:0.3rem;">${label} <span style="font-weight:400;color:var(--text-muted);">${date}</span></div>`;

            for (const msg of msgs) {
                const channelIcon = { 'Instagram DM': '📸', 'Facebook DM': '📘', 'Email': '✉️' }[msg.channel] || '💬';
                const subjectLine = msg.subject ? `<div style="font-size:0.72rem;color:var(--accent-cyan);margin-bottom:0.2rem;">Subject: ${escapeHtml(msg.subject)}</div>` : '';
                const copiedBadge = msg.copied ? '<span style="font-size:0.65rem;color:#22c55e;margin-left:0.5rem;">✓ copied</span>' : '';
                const msgText = escapeHtml(msg.message || '').replace(/\n/g, '<br>');

                html += `
                    <div style="background:var(--bg-card);border:1px solid var(--border-color);border-radius:var(--radius-sm);padding:0.6rem;margin-bottom:0.5rem;">
                        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem;">
                            <span style="font-size:0.78rem;font-weight:600;color:var(--text-primary);">${channelIcon} ${escapeHtml(msg.channel)}${copiedBadge}</span>
                            <button class="btn btn-secondary" style="padding:0.15rem 0.5rem;font-size:0.68rem;" onclick="copyMessage(this, ${msg.id}, ${leadId})">📋 Copy</button>
                        </div>
                        ${subjectLine}
                        <div style="font-size:0.78rem;color:var(--text-secondary);line-height:1.45;">${msgText}</div>
                    </div>
                `;
            }
        }

        list.innerHTML = html;
    } catch (e) {
        section.style.display = 'none';
        console.error('Error loading messages:', e);
    }
}

async function copyMessage(btn, msgId, leadId) {
    // Get the message text from the sibling div
    const card = btn.closest('div[style*="background:var(--bg-card)"]');
    const textDiv = card.querySelector('div:last-child');
    const text = textDiv ? textDiv.textContent : '';

    try {
        await navigator.clipboard.writeText(text);
        btn.textContent = '✅ Copied!';
        setTimeout(() => { btn.textContent = '📋 Copy'; }, 2000);

        // Mark as copied in backend
        fetch(`/api/leads/${leadId}/messages/${msgId}/copied`, { method: 'POST' });
    } catch (e) {
        console.error('Copy failed:', e);
    }
}

// ─── Notes Auto-Save ─────────────────────────────────────────

let _notesAutoSaveTimeout = null;

function initNotesEnhancements() {
    const textarea = document.getElementById('modalNotas');
    if (!textarea) return;

    updateCharCounter();

    textarea.removeEventListener('input', onNotesInput);
    textarea.addEventListener('input', onNotesInput);
}

function onNotesInput() {
    updateCharCounter();
    scheduleNotesAutoSave();
}

function updateCharCounter() {
    const textarea = document.getElementById('modalNotas');
    const counter = document.getElementById('charCounter');
    if (textarea && counter) {
        const len = (textarea.value || '').length;
        counter.textContent = `${len} chars`;
    }
}

function scheduleNotesAutoSave() {
    clearTimeout(_notesAutoSaveTimeout);
    const indicator = document.getElementById('autosaveIndicator');
    if (indicator) {
        indicator.style.display = 'inline';
        indicator.textContent = '⏳ Saving...';
        indicator.style.color = 'var(--text-muted)';
    }

    _notesAutoSaveTimeout = setTimeout(async () => {
        const leadId = document.getElementById('modalLeadId').value;
        if (!leadId) return;

        const notas = document.getElementById('modalNotas').value;
        try {
            const res = await fetch(`/api/leads/${leadId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ notas }),
            });
            if (res.ok && indicator) {
                indicator.textContent = '✓ Saved';
                indicator.style.color = '#22c55e';
                setTimeout(() => {
                    indicator.style.display = 'none';
                }, 2000);
            } else if (indicator) {
                indicator.textContent = '✗ Error';
                indicator.style.color = '#ef4444';
            }
        } catch (e) {
            console.error('Auto-save failed:', e);
            if (indicator) {
                indicator.textContent = '✗ Error';
                indicator.style.color = '#ef4444';
            }
        }
    }, 800);
}

// ─── Persistent Research Checklist ───────────────────────────

let _researchSaveTimeouts = {};
let _currentResearchProgress = 0;

async function loadResearchChecklist(leadId) {
    // Reset all checkboxes and notes first
    document.querySelectorAll('.ri-check').forEach(cb => { cb.checked = false; });
    document.querySelectorAll('.ri-note').forEach(inp => { inp.value = ''; });

    try {
        const res = await fetch(`/api/leads/${leadId}/research`);
        const data = await res.json();

        // Populate checkboxes and notes from DB
        if (data.items) {
            for (const [key, item] of Object.entries(data.items)) {
                const cb = document.querySelector(`.ri-check[data-key="${key}"]`);
                const note = document.querySelector(`.ri-note[data-key="${key}"]`);
                if (cb) cb.checked = item.checked;
                if (note) note.value = item.note || '';
            }
        }

        // Update progress
        if (data.progress) {
            _currentResearchProgress = data.progress.checked;
            updateResearchProgress(data.progress.checked, data.progress.total);
        }
    } catch (e) {
        console.error('Failed to load research:', e);
    }

    // Attach event listeners
    document.querySelectorAll('.ri-check').forEach(cb => {
        cb.removeEventListener('change', onResearchCheckChange);
        cb.addEventListener('change', onResearchCheckChange);
    });
    document.querySelectorAll('.ri-note').forEach(inp => {
        inp.removeEventListener('input', onResearchNoteInput);
        inp.addEventListener('input', onResearchNoteInput);
    });
}

function onResearchCheckChange(e) {
    const key = e.target.dataset.key;
    const checked = e.target.checked;
    const noteEl = document.querySelector(`.ri-note[data-key="${key}"]`);
    const note = noteEl ? noteEl.value : '';
    saveResearchItemDebounced(key, checked, note, 100);
}

function onResearchNoteInput(e) {
    const key = e.target.dataset.key;
    const cbEl = document.querySelector(`.ri-check[data-key="${key}"]`);
    // Auto-check when typing
    if (cbEl && !cbEl.checked && e.target.value.trim()) {
        cbEl.checked = true;
    }
    const checked = cbEl ? cbEl.checked : false;
    saveResearchItemDebounced(key, checked, e.target.value, 600);
}

function saveResearchItemDebounced(key, checked, note, delay) {
    clearTimeout(_researchSaveTimeouts[key]);
    _researchSaveTimeouts[key] = setTimeout(() => {
        const leadId = document.getElementById('modalLeadId').value;
        if (!leadId) return;

        fetch(`/api/leads/${leadId}/research`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ item_key: key, checked, note }),
        })
        .then(res => res.json())
        .then(data => {
            if (data.progress) {
                _currentResearchProgress = data.progress.checked;
                updateResearchProgress(data.progress.checked, data.progress.total);
                // Update funnel zone gating
                if (_currentModalLead) {
                    updateFunnelZone(_currentModalLead);
                }
            }
        })
        .catch(e => console.error('Save research item failed:', e));
    }, delay);
}

function updateResearchProgress(checked, total) {
    const countEl = document.getElementById('researchCount');
    const fillEl = document.getElementById('researchProgressFill');

    if (countEl) {
        countEl.textContent = `${checked}/${total} items`;
        if (checked >= total) {
            countEl.style.color = '#22c55e';
        } else if (checked >= 8) {
            countEl.style.color = '#06b6d4';
        } else {
            countEl.style.color = 'var(--text-muted)';
        }
    }
    if (fillEl) {
        const pct = total > 0 ? (checked / total) * 100 : 0;
        fillEl.style.width = `${pct}%`;
        if (checked >= total) {
            fillEl.style.background = 'linear-gradient(90deg, #22c55e, #10b981)';
        } else if (checked >= 8) {
            fillEl.style.background = 'linear-gradient(90deg, #06b6d4, #8b5cf6)';
        } else {
            fillEl.style.background = 'var(--gradient-primary)';
        }
    }
}

async function generateResearchSummary() {
    const leadId = document.getElementById('modalLeadId').value;
    if (!leadId) return;

    const btn = document.getElementById('btnResearchSummary');
    btn.disabled = true;
    btn.textContent = '⏳ Generating summary...';

    try {
        const res = await fetch(`/api/leads/${leadId}/research/summary`, { method: 'POST' });
        const data = await res.json();

        if (data.success && data.summary) {
            document.getElementById('modalNotas').value = data.summary;
            updateCharCounter();
            showToast('✅ Research summary generated and saved to notes');
        }
    } catch (e) {
        console.error('Generate summary failed:', e);
        showToast('✗ Failed to generate summary');
    }

    btn.disabled = false;
    btn.textContent = '📝 Generate Research Summary';
}

