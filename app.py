"""
Orion Flow CRM Lead Dashboard — Flask Application
"""
import os
import webbrowser
import threading
from flask import Flask, render_template, jsonify, request
from database import (init_db, get_all_leads, get_lead, update_lead, get_lead_history,
                      get_metrics, import_leads, LEAD_STATUSES, CHANNELS, FUNNEL_STAGES,
                      save_setting, get_setting, get_all_settings,
                      get_lead_messages, save_funnel_message, mark_message_copied,
                      has_stage_messages,
                      get_lead_research, save_research_item, get_research_progress)
from data_loader import load_leads, get_file_info
from lead_generator import (start_generation, get_progress,
                             test_apify_key, test_claude_key,
                             generate_funnel_message)

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

LEADS_FOLDER = r'C:\Users\tdeca\OneDrive\Desktop\empresa\Projeto CRM Leads'

# ─── Pages ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    """Serve the main dashboard."""
    return render_template('index.html')

@app.route('/settings')
def settings_page():
    """Serve the settings page."""
    return render_template('settings.html')

# ─── API Endpoints ────────────────────────────────────────────────────────────

@app.route('/api/leads', methods=['GET'])
def api_leads():
    """Get all leads with optional filters."""
    filters = {
        'status': request.args.get('status', ''),
        'canal': request.args.get('canal', ''),
        'categoria': request.args.get('categoria', ''),
        'search': request.args.get('search', ''),
    }
    # Remove empty filters
    filters = {k: v for k, v in filters.items() if v}
    leads = get_all_leads(filters if filters else None)
    return jsonify({'leads': leads, 'count': len(leads)})

@app.route('/api/leads/<int:lead_id>', methods=['GET'])
def api_lead_detail(lead_id):
    """Get a single lead."""
    lead = get_lead(lead_id)
    if not lead:
        return jsonify({'error': 'Lead not found'}), 404
    return jsonify(lead)

@app.route('/api/leads/<int:lead_id>', methods=['PUT'])
def api_update_lead(lead_id):
    """Update a lead."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    updated = update_lead(lead_id, data)
    if not updated:
        return jsonify({'error': 'Lead not found'}), 404
    return jsonify(updated)

@app.route('/api/leads/<int:lead_id>/history', methods=['GET'])
def api_lead_history(lead_id):
    """Get interaction history for a lead."""
    history = get_lead_history(lead_id)
    return jsonify({'history': history})

@app.route('/api/metrics', methods=['GET'])
def api_metrics():
    """Get dashboard metrics."""
    metrics = get_metrics()
    return jsonify(metrics)

@app.route('/api/reload', methods=['POST'])
def api_reload():
    """Re-import data from lead files."""
    try:
        df = load_leads(LEADS_FOLDER)
        result = import_leads(df)
        return jsonify({
            'success': True,
            'message': f'Imported {result["imported"]} new leads, updated {result["skipped"]} existing.',
            'details': result
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/files', methods=['GET'])
def api_files():
    """Get info about available data files."""
    files = get_file_info(LEADS_FOLDER)
    return jsonify({'files': files})

@app.route('/api/config', methods=['GET'])
def api_config():
    """Get dashboard configuration."""
    return jsonify({
        'statuses': LEAD_STATUSES,
        'channels': CHANNELS,
        'funnel_stages': FUNNEL_STAGES,
        'leads_folder': LEADS_FOLDER
    })

# ─── Settings API ─────────────────────────────────────────────────────────────

@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    """Get all settings (API keys masked)."""
    settings = get_all_settings()
    # Mask API keys for display
    masked = dict(settings)
    for key in ['apify_api_key', 'anthropic_api_key']:
        val = masked.get(key, '')
        if val and len(val) > 8:
            masked[key] = val[:4] + '****' + val[-4:]
        elif val:
            masked[key] = '****'
    return jsonify({'settings': masked, 'has_apify_key': bool(settings.get('apify_api_key', '')), 'has_claude_key': bool(settings.get('anthropic_api_key', ''))})

@app.route('/api/settings', methods=['POST'])
def api_save_settings():
    """Save settings."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data'}), 400
    
    allowed_keys = ['apify_api_key', 'anthropic_api_key', 'search_keywords', 'search_location', 'max_leads_per_search']
    saved = []
    for key in allowed_keys:
        if key in data:
            # Don't overwrite keys with masked values
            if '****' not in str(data[key]):
                save_setting(key, str(data[key]))
                saved.append(key)
    
    return jsonify({'success': True, 'saved': saved})

@app.route('/api/settings/test', methods=['POST'])
def api_test_keys():
    """Test API key validity."""
    data = request.get_json() or {}
    results = {}
    
    if data.get('test_apify'):
        key = get_setting('apify_api_key')
        results['apify'] = test_apify_key(key) if key else {'valid': False, 'error': 'Nenhuma key configurada'}
    
    if data.get('test_claude'):
        key = get_setting('anthropic_api_key')
        results['claude'] = test_claude_key(key) if key else {'valid': False, 'error': 'Nenhuma key configurada'}
    
    return jsonify(results)

# ─── Lead Generation API ──────────────────────────────────────────────────────

@app.route('/api/generate', methods=['POST'])
def api_generate():
    """Start lead generation."""
    progress = get_progress()
    if progress.get('running'):
        return jsonify({'error': 'Geração já em andamento'}), 409
    
    settings = get_all_settings()
    apify_key = settings.get('apify_api_key', '')
    claude_key = settings.get('anthropic_api_key', '')
    
    if not apify_key:
        return jsonify({'error': 'Configure a Apify API Key nas Configurações primeiro.'}), 400
    
    # Get search params from request body or settings
    data = request.get_json() or {}
    keywords_str = data.get('keywords', settings.get('search_keywords', 'barbershop, spa, nail salon'))
    location = data.get('location', settings.get('search_location', 'Danbury, CT'))
    max_results = int(data.get('max_results', settings.get('max_leads_per_search', '50')))
    
    keywords = [k.strip() for k in keywords_str.split(',') if k.strip()]
    
    start_generation(keywords, location, max_results, apify_key, claude_key)
    
    return jsonify({'success': True, 'message': 'Geração iniciada!'})

@app.route('/api/generate/status', methods=['GET'])
def api_generate_status():
    """Get current lead generation progress."""
    return jsonify(get_progress())

# ─── Per-Lead Funnel Message Generation API ─────────────────────────────────────

@app.route('/api/leads/<int:lead_id>/generate-message', methods=['POST'])
def api_generate_lead_message(lead_id):
    """Generate messages for a single lead at a specific funnel stage."""
    claude_key = get_setting('anthropic_api_key')
    if not claude_key:
        return jsonify({'error': 'Configure a Anthropic API Key nas Configurações primeiro.'}), 400

    data = request.get_json() or {}
    stage = data.get('stage')

    if not stage:
        return jsonify({'error': 'Funnel stage is required'}), 400

    lead = get_lead(lead_id)
    if not lead:
        return jsonify({'error': 'Lead not found'}), 404

    # Check if already has messages for this stage
    if has_stage_messages(lead_id, stage):
        return jsonify({'error': f'Messages already generated for stage: {stage}', 'already_exists': True}), 409

    try:
        previous_messages = get_lead_messages(lead_id)
        messages = generate_funnel_message(lead, stage, claude_key, previous_messages=previous_messages)
        if not messages:
            return jsonify({'error': 'Claude did not return valid messages'}), 500

        # Save each channel message to database
        saved = []
        for channel, content in messages.items():
            subject = content.get('subject') if isinstance(content, dict) else None
            body = content.get('body') if isinstance(content, dict) else content
            save_funnel_message(lead_id, channel, body, stage, lead.get('status_lead', ''), subject)
            saved.append({'channel': channel, 'subject': subject, 'message': body})

        return jsonify({'success': True, 'messages': saved, 'stage': stage})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/leads/<int:lead_id>/messages', methods=['GET'])
def api_lead_messages(lead_id):
    """Get generated messages for a lead."""
    messages = get_lead_messages(lead_id)
    return jsonify({'messages': messages})

@app.route('/api/leads/<int:lead_id>/messages/<int:msg_id>/copied', methods=['POST'])
def api_mark_copied(lead_id, msg_id):
    """Mark a message as copied."""
    mark_message_copied(msg_id)
    return jsonify({'success': True})

# ─── Research Checklist API ──────────────────────────────────────────────────

@app.route('/api/leads/<int:lead_id>/research', methods=['GET'])
def api_get_research(lead_id):
    """Get research checklist state for a lead."""
    items = get_lead_research(lead_id)
    progress = get_research_progress(lead_id)
    return jsonify({'items': items, 'progress': progress})

@app.route('/api/leads/<int:lead_id>/research', methods=['PUT'])
def api_save_research(lead_id):
    """Save a single research checklist item."""
    data = request.get_json() or {}
    item_key = data.get('item_key', '')
    checked = data.get('checked', False)
    note = data.get('note', '')
    if not item_key:
        return jsonify({'error': 'item_key required'}), 400
    save_research_item(lead_id, item_key, checked, note)
    progress = get_research_progress(lead_id)
    return jsonify({'success': True, 'progress': progress})

@app.route('/api/leads/<int:lead_id>/research/summary', methods=['POST'])
def api_research_summary(lead_id):
    """Generate research summary and save to lead notes."""
    items = get_lead_research(lead_id)
    
    ITEM_LABELS = {
        'ig_has_profile': 'Has Instagram profile',
        'ig_followers': 'Followers',
        'ig_last_post': 'Last post',
        'ig_bio_complete': 'Bio complete',
        'ig_link_in_bio': 'Link in bio',
        'fb_has_page': 'Has Facebook page',
        'fb_active': 'Active',
        'fb_last_post': 'Last post',
        'ws_has_website': 'Has website',
        'ws_mobile': 'Mobile-friendly',
        'ws_booking': 'Online booking',
        'ws_contact_form': 'Contact form',
        'go_profile_complete': 'Profile complete',
        'go_has_photos': 'Has photos',
        'go_responds_reviews': 'Responds to reviews',
        'go_review_count': 'Review count',
    }
    
    categories = {
        'Instagram': ['ig_has_profile', 'ig_followers', 'ig_last_post', 'ig_bio_complete', 'ig_link_in_bio'],
        'Facebook': ['fb_has_page', 'fb_active', 'fb_last_post'],
        'Website': ['ws_has_website', 'ws_mobile', 'ws_booking', 'ws_contact_form'],
        'Google': ['go_profile_complete', 'go_has_photos', 'go_responds_reviews', 'go_review_count'],
    }
    
    lines = ['RESEARCH SUMMARY:']
    weaknesses = []
    
    for cat_name, keys in categories.items():
        cat_lines = []
        for key in keys:
            item = items.get(key, {})
            if item.get('checked'):
                note = item.get('note', '').strip()
                label = ITEM_LABELS.get(key, key)
                cat_lines.append(f"  - {label}: {note if note else 'Yes'}")
                # Detect weaknesses
                note_lower = (note or '').lower()
                if any(w in note_lower for w in ['no', 'none', 'inactive', 'broken', 'slow', 'never', 'old', 'abandoned', 'missing', 'doesn']):
                    weaknesses.append(f"{cat_name}: {label} — {note}")
        if cat_lines:
            lines.append(f"{cat_name}:")
            lines.extend(cat_lines)
    
    if weaknesses:
        lines.append(f"\nMain weaknesses identified:")
        for w in weaknesses:
            lines.append(f"  ⚠ {w}")
    
    summary = '\n'.join(lines)
    
    # Save to lead notes
    lead = get_lead(lead_id)
    if lead:
        update_lead(lead_id, {'notas': summary})
    
    return jsonify({'success': True, 'summary': summary})

# ─── Startup ──────────────────────────────────────────────────────────────────

def open_browser():
    """Open browser after a short delay."""
    import time
    time.sleep(1.5)
    webbrowser.open('http://localhost:5000')

def startup():
    """Initialize database schema only — does NOT re-import data."""
    print("\n" + "=" * 60)
    print("  🚀 Orion Flow — CRM Lead Dashboard")
    print("=" * 60)
    
    # Init database schema (CREATE IF NOT EXISTS — safe, never drops data)
    print("\n📦 Initializing database...")
    init_db()
    
    # On Vercel, skip file-based import
    if os.environ.get('VERCEL'):
        print("\n☁️  Running on Vercel (serverless mode)")
        return
    
    # Check if database is empty (first run) → auto-import only once
    from database import get_db
    conn = get_db()
    count = conn.execute('SELECT COUNT(*) FROM leads').fetchone()[0]
    conn.close()
    
    if count == 0:
        print(f"\n📂 First run — importing leads from: {LEADS_FOLDER}")
        try:
            files = get_file_info(LEADS_FOLDER)
            for f in files:
                print(f"  📄 {f['filename']} ({f['size_kb']} KB)")
            df = load_leads(LEADS_FOLDER)
            result = import_leads(df)
            print(f"\n✅ Data loaded: {result['imported']} new, {result['skipped']} existing")
        except Exception as e:
            print(f"\n⚠️  Could not load leads: {e}")
            print("  You can reload data later from the dashboard.")
    else:
        print(f"\n✅ Database OK — {count} leads already in database")
        print("  Use 'Recarregar Dados' on the dashboard to import new data from files.")
    
    print(f"\n🌐 Dashboard available at: http://localhost:5000")
    print("   Press Ctrl+C to stop the server.\n")

# Always init DB on import (needed for Vercel)
startup()

if __name__ == '__main__':
    # Open browser in a background thread (only when running locally)
    threading.Thread(target=open_browser, daemon=True).start()
    
    app.run(host='0.0.0.0', port=5000, debug=False)
