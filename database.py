"""
Database module — SQLite models and helpers for CRM Lead Dashboard.
"""
import sqlite3
import os
from datetime import datetime

# On Vercel (serverless), only /tmp/ is writable
if os.environ.get('VERCEL'):
    DB_PATH = '/tmp/crm.db'
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crm.db')

LEAD_STATUSES = [
    'New', 'Contacted', 'Thinking', 'Interested',
    'Not Interested', 'Closed', 'Lost'
]

# Map funnel stages to their display info
FUNNEL_STAGES = {
    'New':             {'color': '#3b82f6', 'icon': '🔵', 'stage': 'first_contact',  'action': 'Generate First Contact Message'},
    'Contacted':       {'color': '#8b5cf6', 'icon': '🟣', 'stage': None,             'action': None},
    'Thinking':        {'color': '#eab308', 'icon': '🟡', 'stage': 'follow_up',      'action': 'Generate Follow-up Message'},
    'Interested':      {'color': '#22c55e', 'icon': '🟢', 'stage': 'closing',        'action': 'Generate Closing Message'},
    'Not Interested':  {'color': '#ef4444', 'icon': '🔴', 'stage': 'recovery',       'action': 'Generate Recovery Message'},
    'Closed':          {'color': '#6b7280', 'icon': '⚫', 'stage': 'review_request', 'action': 'Generate Review Request'},
    'Lost':            {'color': '#374151', 'icon': '💀', 'stage': None,             'action': None},
}

CHANNELS = ['Email', 'Instagram DM', 'Facebook DM', 'WhatsApp']

def get_db():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    """Initialize the database schema."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_id TEXT,
            data_coleta TEXT,
            nome_negocio TEXT NOT NULL,
            categoria TEXT,
            endereco TEXT,
            cidade TEXT,
            estado TEXT,
            cep TEXT,
            telefone TEXT,
            email TEXT,
            site TEXT,
            avaliacao_google REAL,
            num_reviews INTEGER,
            google_maps_link TEXT,
            instagram TEXT,
            facebook TEXT,
            tiktok TEXT,
            yelp TEXT,
            tem_site TEXT,
            qualidade_site TEXT,
            tem_agendamento TEXT,
            presenca_digital_score REAL,
            score_prioridade REAL,
            canal_abordagem TEXT DEFAULT '',
            justificativa_score TEXT,
            status_lead TEXT DEFAULT 'Novo',
            data_contato TEXT,
            data_resposta TEXT,
            proximo_passo TEXT,
            notas TEXT,
            email_aberto INTEGER DEFAULT 0,
            email_clicou INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS lead_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            field_changed TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            changed_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );

        CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status_lead);
        CREATE INDEX IF NOT EXISTS idx_leads_canal ON leads(canal_abordagem);
        CREATE INDEX IF NOT EXISTS idx_leads_categoria ON leads(categoria);
        CREATE INDEX IF NOT EXISTS idx_history_lead ON lead_history(lead_id);

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS lead_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            subject TEXT,
            message TEXT NOT NULL,
            funnel_stage TEXT NOT NULL DEFAULT 'first_contact',
            status_at_generation TEXT,
            copied INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );
        CREATE INDEX IF NOT EXISTS idx_messages_lead ON lead_messages(lead_id);
        CREATE INDEX IF NOT EXISTS idx_messages_stage ON lead_messages(funnel_stage);

        CREATE TABLE IF NOT EXISTS lead_research (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            item_key TEXT NOT NULL,
            checked INTEGER DEFAULT 0,
            note TEXT DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (lead_id) REFERENCES leads(id),
            UNIQUE(lead_id, item_key)
        );
        CREATE INDEX IF NOT EXISTS idx_research_lead ON lead_research(lead_id);
    ''')

    # Set defaults for settings if not exist
    defaults = {
        'apify_api_key': '',
        'anthropic_api_key': '',
        'search_keywords': 'barbershop, spa, nail salon',
        'search_location': 'Danbury, CT',
        'max_leads_per_search': '50',
    }
    for key, value in defaults.items():
        existing = cursor.execute('SELECT 1 FROM settings WHERE key = ?', (key,)).fetchone()
        if not existing:
            cursor.execute('INSERT INTO settings (key, value) VALUES (?, ?)', (key, value))

    conn.commit()
    conn.close()

def import_leads(dataframe):
    """Import leads from a pandas DataFrame into SQLite."""
    conn = get_db()
    cursor = conn.cursor()

    # Column mapping from Excel sub-headers to DB columns
    col_map = {
        'ID': 'original_id',
        'Data Coleta': 'data_coleta',
        'Nome do Negócio': 'nome_negocio',
        'Categoria': 'categoria',
        'Endereço': 'endereco',
        'Cidade': 'cidade',
        'Estado': 'estado',
        'CEP': 'cep',
        'Telefone': 'telefone',
        'Email': 'email',
        'Site': 'site',
        'Avaliação Google': 'avaliacao_google',
        'Nº Reviews': 'num_reviews',
        'Google Maps Link': 'google_maps_link',
        'Instagram': 'instagram',
        'Facebook': 'facebook',
        'TikTok': 'tiktok',
        'Yelp': 'yelp',
        'Tem Site?': 'tem_site',
        'Qualidade Site': 'qualidade_site',
        'Tem Agendamento Online?': 'tem_agendamento',
        'Presença Digital Score': 'presenca_digital_score',
        'Score Prioridade (1-10)': 'score_prioridade',
        'Canal Abordagem': 'canal_abordagem',
        'Justificativa Score': 'justificativa_score',
        'Status Lead': 'status_lead',
        'Data Contato': 'data_contato',
        'Data Resposta': 'data_resposta',
        'Próximo Passo': 'proximo_passo',
        'Notas': 'notas',
    }

    imported = 0
    skipped = 0

    for _, row in dataframe.iterrows():
        nome = None
        for excel_col, db_col in col_map.items():
            if excel_col in dataframe.columns and db_col == 'nome_negocio':
                val = row.get(excel_col)
                if val and str(val).strip() and str(val) != 'nan':
                    nome = str(val).strip()
                break

        if not nome:
            skipped += 1
            continue

        # Check if lead already exists (by name)
        existing = cursor.execute(
            'SELECT id, status_lead, canal_abordagem, notas, data_contato, data_resposta, proximo_passo, email_aberto, email_clicou FROM leads WHERE nome_negocio = ?',
            (nome,)
        ).fetchone()

        data = {}
        for excel_col, db_col in col_map.items():
            if excel_col in dataframe.columns:
                val = row.get(excel_col)
                if val is not None and str(val).strip() and str(val) != 'nan':
                    data[db_col] = str(val).strip()
                else:
                    data[db_col] = ''
            else:
                data[db_col] = ''

        if existing:
            # Preserve ALL user-modified fields — never overwrite dashboard edits
            preserved_fields = [
                'status_lead', 'canal_abordagem', 'notas', 'data_contato',
                'data_resposta', 'proximo_passo', 'telefone', 'email', 'site',
                'instagram', 'facebook', 'tiktok', 'yelp',
                'score_prioridade', 'justificativa_score', 'qualidade_site',
            ]
            for field in preserved_fields:
                old_val = existing[field] if field in existing.keys() else None
                if old_val is not None and str(old_val).strip() and str(old_val) != 'nan':
                    data[field] = old_val

            cols = ', '.join(f'{k} = ?' for k in data.keys())
            vals = list(data.values()) + [existing['id']]
            cursor.execute(f'UPDATE leads SET {cols}, updated_at = datetime("now", "localtime") WHERE id = ?', vals)
            skipped += 1
        else:
            cols = ', '.join(data.keys())
            placeholders = ', '.join(['?'] * len(data))
            cursor.execute(f'INSERT INTO leads ({cols}) VALUES ({placeholders})', list(data.values()))
            imported += 1

    conn.commit()
    conn.close()
    return {'imported': imported, 'skipped': skipped, 'total': imported + skipped}

# ─── Priority Calculation ───────────────────────────────────────────────────

def calculate_priority(lead, research_checked=0):
    """Calculate lead priority based on multiple criteria.
    Returns dict with 'points', 'level', 'color'.
    """
    points = 0

    # No website → +3
    tem_site = str(lead.get('tem_site', '') or '').lower().strip()
    site = str(lead.get('site', '') or '').strip()
    if tem_site in ('não', 'nao', 'no', 'n', '') and not site:
        points += 3
    # Bad/outdated website → +2
    elif str(lead.get('qualidade_site', '') or '').lower().strip() in ('ruim', 'bad', 'poor', 'baixa', 'low', ''):
        points += 2

    # No online booking → +2
    tem_agendamento = str(lead.get('tem_agendamento', '') or '').lower().strip()
    if tem_agendamento in ('não', 'nao', 'no', 'n', ''):
        points += 2

    # Instagram abandoned or nonexistent → +1
    ig = str(lead.get('instagram', '') or '').strip()
    if not ig or ig == '@usuario' or ig.lower() in ('no', 'não', 'n/a', 'nan', ''):
        points += 1

    # Facebook abandoned or nonexistent → +1
    fb = str(lead.get('facebook', '') or '').strip()
    if not fb or fb.lower() in ('no', 'não', 'n/a', 'nan', 'facebook.com/pagina', ''):
        points += 1

    # Google rating above 4.0 → +2 (good business, weak digital = ideal client)
    try:
        rating = float(lead.get('avaliacao_google', 0) or 0)
        if rating >= 4.0:
            points += 2
    except (ValueError, TypeError):
        pass

    # More than 20 reviews → +1
    try:
        reviews = int(lead.get('num_reviews', 0) or 0)
        if reviews > 20:
            points += 1
    except (ValueError, TypeError):
        pass

    # Research checklist complete (≥12 items) → +1
    if research_checked >= 12:
        points += 1

    # Manual notes filled → +1
    notas = str(lead.get('notas', '') or '').strip()
    if len(notas) > 10:
        points += 1

    # Determine level
    if points >= 13:
        level, color = 'TOP', '#10b981'
    elif points >= 9:
        level, color = 'HIGH', '#f97316'
    elif points >= 5:
        level, color = 'MEDIUM', '#eab308'
    else:
        level, color = 'LOW', '#6b7280'

    return {'points': points, 'level': level, 'color': color}

def get_all_leads(filters=None):
    """Get all leads with optional filters, including computed priority."""
    conn = get_db()
    query = 'SELECT * FROM leads WHERE 1=1'
    params = []

    if filters:
        if filters.get('status'):
            query += ' AND status_lead = ?'
            params.append(filters['status'])
        if filters.get('canal'):
            query += ' AND canal_abordagem = ?'
            params.append(filters['canal'])
        if filters.get('categoria'):
            query += ' AND categoria = ?'
            params.append(filters['categoria'])
        if filters.get('search'):
            query += ' AND nome_negocio LIKE ?'
            params.append(f'%{filters["search"]}%')

    query += ' ORDER BY nome_negocio ASC'

    leads = conn.execute(query, params).fetchall()
    result = []
    for lead in leads:
        d = dict(lead)
        # Get research progress for this lead
        progress = conn.execute(
            'SELECT COUNT(*) as cnt FROM lead_research WHERE lead_id = ? AND checked = 1',
            (d['id'],)
        ).fetchone()
        research_checked = progress['cnt'] if progress else 0
        pri = calculate_priority(d, research_checked)
        d['priority_points'] = pri['points']
        d['priority_level'] = pri['level']
        d['priority_color'] = pri['color']
        result.append(d)

    # Sort by priority points descending (TOP first)
    result.sort(key=lambda x: x['priority_points'], reverse=True)

    conn.close()
    return result

def get_lead(lead_id):
    """Get a single lead by ID."""
    conn = get_db()
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    result = dict(lead) if lead else None
    conn.close()
    return result

def update_lead(lead_id, data):
    """Update a lead and record changes in history."""
    conn = get_db()
    cursor = conn.cursor()

    current = cursor.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not current:
        conn.close()
        return None

    tracked_fields = ['status_lead', 'canal_abordagem', 'notas', 'data_contato',
                      'data_resposta', 'proximo_passo', 'email_aberto', 'email_clicou',
                      'score_prioridade']

    for field in tracked_fields:
        if field in data:
            old_val = str(current[field]) if current[field] else ''
            new_val = str(data[field]) if data[field] else ''
            if old_val != new_val:
                cursor.execute(
                    'INSERT INTO lead_history (lead_id, field_changed, old_value, new_value) VALUES (?, ?, ?, ?)',
                    (lead_id, field, old_val, new_val)
                )

    allowed = ['status_lead', 'canal_abordagem', 'notas', 'data_contato',
               'data_resposta', 'proximo_passo', 'email_aberto', 'email_clicou',
               'score_prioridade', 'telefone', 'email', 'site',
               'instagram', 'facebook', 'tiktok', 'yelp']

    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        cols = ', '.join(f'{k} = ?' for k in updates.keys())
        vals = list(updates.values()) + [lead_id]
        cursor.execute(f'UPDATE leads SET {cols}, updated_at = datetime("now", "localtime") WHERE id = ?', vals)
    
    conn.commit()

    updated = cursor.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    result = dict(updated) if updated else None
    conn.close()
    return result

def get_lead_history(lead_id):
    """Get interaction history for a lead."""
    conn = get_db()
    history = conn.execute(
        'SELECT * FROM lead_history WHERE lead_id = ? ORDER BY changed_at DESC',
        (lead_id,)
    ).fetchall()
    result = [dict(h) for h in history]
    conn.close()
    return result

def get_metrics():
    """Calculate dashboard metrics."""
    conn = get_db()
    cursor = conn.cursor()

    total = cursor.execute('SELECT COUNT(*) FROM leads').fetchone()[0]

    # Status counts
    status_counts = {}
    for status in LEAD_STATUSES:
        count = cursor.execute('SELECT COUNT(*) FROM leads WHERE status_lead = ?', (status,)).fetchone()[0]
        status_counts[status] = count

    # Channel counts
    channel_counts = {}
    for channel in CHANNELS:
        count = cursor.execute('SELECT COUNT(*) FROM leads WHERE canal_abordagem = ?', (channel,)).fetchone()[0]
        channel_counts[channel] = count
    no_channel = cursor.execute('SELECT COUNT(*) FROM leads WHERE canal_abordagem = "" OR canal_abordagem IS NULL').fetchone()[0]
    channel_counts['Não definido'] = no_channel

    # Category counts
    categories = cursor.execute('SELECT categoria, COUNT(*) FROM leads GROUP BY categoria').fetchall()
    category_counts = {row[0]: row[1] for row in categories if row[0]}

    # Email metrics
    total_contacted = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE status_lead NOT IN ("New", "Not Interested", "Lost")'
    ).fetchone()[0]
    emails_sent = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE canal_abordagem = "Email"'
    ).fetchone()[0]
    emails_opened = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE email_aberto = 1'
    ).fetchone()[0]
    emails_clicked = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE email_clicou = 1'
    ).fetchone()[0]
    responded = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE status_lead IN ("Thinking", "Interested", "Closed")'
    ).fetchone()[0]
    in_negotiation = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE status_lead = "Interested"'
    ).fetchone()[0]
    closed = cursor.execute(
        'SELECT COUNT(*) FROM leads WHERE status_lead = "Closed"'
    ).fetchone()[0]

    # Daily contacts (for line chart)
    daily_contacts = cursor.execute('''
        SELECT data_contato, COUNT(*) 
        FROM leads 
        WHERE data_contato IS NOT NULL AND data_contato != ""
        GROUP BY data_contato 
        ORDER BY data_contato
    ''').fetchall()

    # Funnel data — follows the new English statuses
    funnel_order = ['New', 'Contacted', 'Thinking', 'Interested', 'Closed']
    funnel = [{'stage': s, 'count': status_counts.get(s, 0)} for s in funnel_order]

    conn.close()

    open_rate = (emails_opened / emails_sent * 100) if emails_sent > 0 else 0
    click_rate = (emails_clicked / emails_sent * 100) if emails_sent > 0 else 0
    response_rate = (responded / total_contacted * 100) if total_contacted > 0 else 0

    return {
        'total': total,
        'status_counts': status_counts,
        'channel_counts': channel_counts,
        'category_counts': category_counts,
        'emails_sent': emails_sent,
        'emails_opened': emails_opened,
        'emails_clicked': emails_clicked,
        'open_rate': round(open_rate, 1),
        'click_rate': round(click_rate, 1),
        'response_rate': round(response_rate, 1),
        'responded': responded,
        'in_negotiation': in_negotiation,
        'closed': closed,
        'daily_contacts': [{'date': d[0], 'count': d[1]} for d in daily_contacts],
        'funnel': funnel,
    }

# ─── Settings ─────────────────────────────────────────────────────────────────

def save_setting(key, value):
    """Save a setting to the database."""
    conn = get_db()
    conn.execute(
        'INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime("now", "localtime"))',
        (key, value)
    )
    conn.commit()
    conn.close()

def get_setting(key, default=''):
    """Get a setting value."""
    conn = get_db()
    row = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    conn.close()
    return row['value'] if row else default

def get_all_settings():
    """Get all settings as a dict."""
    conn = get_db()
    rows = conn.execute('SELECT key, value FROM settings').fetchall()
    conn.close()
    return {row['key']: row['value'] for row in rows}

# ─── Messages ─────────────────────────────────────────────────────────────────

def save_funnel_message(lead_id, channel, message, funnel_stage, status_at_generation, subject=None):
    """Save a generated funnel message for a lead."""
    conn = get_db()
    conn.execute('''
        INSERT INTO lead_messages (lead_id, channel, subject, message, funnel_stage, status_at_generation, created_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
    ''', (lead_id, channel, subject, message, funnel_stage, status_at_generation))
    conn.commit()
    conn.close()

def get_lead_messages(lead_id):
    """Get all messages for a lead, ordered by creation time."""
    conn = get_db()
    rows = conn.execute(
        'SELECT * FROM lead_messages WHERE lead_id = ? ORDER BY created_at DESC', (lead_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def mark_message_copied(message_id):
    """Mark a message as copied."""
    conn = get_db()
    conn.execute('UPDATE lead_messages SET copied = 1 WHERE id = ?', (message_id,))
    conn.commit()
    conn.close()

def has_stage_messages(lead_id, funnel_stage):
    """Check if a lead already has messages for a given funnel stage."""
    conn = get_db()
    row = conn.execute(
        'SELECT COUNT(*) as cnt FROM lead_messages WHERE lead_id = ? AND funnel_stage = ?',
        (lead_id, funnel_stage)
    ).fetchone()
    conn.close()
    return row['cnt'] > 0 if row else False

# ─── Research Checklist ─────────────────────────────────────────────────────

RESEARCH_ITEMS = [
    # Instagram
    'ig_has_profile', 'ig_followers', 'ig_last_post', 'ig_bio_complete', 'ig_link_in_bio',
    # Facebook
    'fb_has_page', 'fb_active', 'fb_last_post',
    # Website
    'ws_has_website', 'ws_mobile', 'ws_booking', 'ws_contact_form',
    # Google
    'go_profile_complete', 'go_has_photos', 'go_responds_reviews', 'go_review_count',
]

def get_lead_research(lead_id):
    """Get all research checklist items for a lead."""
    conn = get_db()
    rows = conn.execute(
        'SELECT item_key, checked, note FROM lead_research WHERE lead_id = ?', (lead_id,)
    ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        result[r['item_key']] = {'checked': bool(r['checked']), 'note': r['note'] or ''}
    return result

def save_research_item(lead_id, item_key, checked, note):
    """Upsert a single research checklist item."""
    conn = get_db()
    conn.execute('''
        INSERT INTO lead_research (lead_id, item_key, checked, note, updated_at)
        VALUES (?, ?, ?, ?, datetime('now', 'localtime'))
        ON CONFLICT(lead_id, item_key)
        DO UPDATE SET checked = excluded.checked, note = excluded.note, updated_at = excluded.updated_at
    ''', (lead_id, item_key, 1 if checked else 0, note or ''))
    conn.commit()
    conn.close()

def get_research_progress(lead_id):
    """Get research progress for a lead."""
    conn = get_db()
    row = conn.execute(
        'SELECT COUNT(*) as cnt FROM lead_research WHERE lead_id = ? AND checked = 1',
        (lead_id,)
    ).fetchone()
    conn.close()
    checked = row['cnt'] if row else 0
    return {'checked': checked, 'total': len(RESEARCH_ITEMS)}
