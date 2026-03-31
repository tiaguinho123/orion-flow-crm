/* ═══════════════════════════════════════════════════════════
   Table Module — Lead table rendering, filtering, and sorting
   ═══════════════════════════════════════════════════════════ */

/**
 * Get CSS class for a status badge.
 */
function getStatusClass(status) {
    const map = {
        'New':              'status-new',
        'Contacted':        'status-contacted',
        'Thinking':         'status-thinking',
        'Interested':       'status-interested',
        'Not Interested':   'status-not-interested',
        'Closed':           'status-closed',
        'Lost':             'status-lost',
        // Legacy Portuguese mappings
        'Novo':             'status-new',
        'Contatado':        'status-contacted',
    };
    return map[status] || 'status-new';
}

/**
 * Get CSS class for a channel badge.
 */
function getChannelClass(channel) {
    const map = {
        'Email':          'channel-email',
        'Instagram DM':   'channel-instagram',
        'Facebook DM':    'channel-facebook',
        'WhatsApp':       'channel-whatsapp',
    };
    return map[channel] || 'channel-none';
}

/**
 * Render star rating HTML.
 */
function renderStars(rating) {
    if (!rating || rating === '' || rating === 'nan') return '—';
    const num = parseFloat(rating);
    if (isNaN(num)) return '—';
    return `<span class="star-icon">★</span> ${num.toFixed(1)}`;
}

/**
 * Render the leads table from the given data.
 */
function renderLeadsTable(leads) {
    const tbody = document.getElementById('leadsTableBody');
    if (!tbody) return;

    if (!leads || leads.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="10" style="text-align: center; padding: 3rem; color: var(--text-muted);">
                    Nenhum lead encontrado.
                </td>
            </tr>
        `;
        document.getElementById('tableCount').textContent = '0 leads';
        return;
    }

    let html = '';
    for (const lead of leads) {
        const statusClass = getStatusClass(lead.status_lead);
        const channelClass = getChannelClass(lead.canal_abordagem);
        const channelText = lead.canal_abordagem || '—';
        const lastContact = lead.data_contato || '—';
        const priLevel = lead.priority_level || 'LOW';
        const priColor = lead.priority_color || '#6b7280';
        const priPoints = lead.priority_points || 0;

        html += `
            <tr onclick="openLeadModal(${lead.id})" style="cursor:pointer;">
                <td class="td-name">${escapeHtml(lead.nome_negocio || '')}</td>
                <td>${escapeHtml(lead.categoria || '—')}</td>
                <td>${escapeHtml(lead.telefone || '—')}</td>
                <td class="td-rating">${renderStars(lead.avaliacao_google)}</td>
                <td>${lead.num_reviews || '—'}</td>
                <td>
                    <span class="priority-badge priority-${priLevel.toLowerCase()}" title="${priPoints} pts">
                        ${priLevel}
                    </span>
                </td>
                <td><span class="channel-badge ${channelClass}">${channelText}</span></td>
                <td>
                    <span class="status-badge ${statusClass}">
                        <span class="status-dot"></span>
                        ${lead.status_lead || 'Novo'}
                    </span>
                </td>
                <td>${lastContact}</td>
                <td style="text-align:center;">
                    <button class="btn-action" onclick="event.stopPropagation(); openLeadModal(${lead.id})" title="Editar lead">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                        </svg>
                    </button>
                </td>
            </tr>
        `;
    }

    tbody.innerHTML = html;
    document.getElementById('tableCount').textContent = `${leads.length} lead${leads.length !== 1 ? 's' : ''}`;
}

/**
 * Escape HTML to prevent XSS.
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
