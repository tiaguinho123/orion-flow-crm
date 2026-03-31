# 🚀 Orion Flow — CRM Lead Dashboard

Dashboard local para gerenciamento de leads e campanhas de outreach.

## Instalação Rápida (Windows)

**Opção 1 — Instalação automática:**
```
Clique duas vezes no arquivo install.bat
```

**Opção 2 — Instalação manual:**
```bash
# 1. Crie um ambiente virtual
python -m venv venv

# 2. Ative o ambiente
venv\Scripts\activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Inicie o dashboard
python app.py
```

## Uso

O dashboard abre automaticamente em **http://localhost:5000**

### Funcionalidades:
- **📊 Métricas** — Cards com totais, taxas de abertura, clique e resposta
- **📈 Gráficos** — Funil de conversão, evolução diária, distribuição por canal
- **📋 Tabela** — Todos os leads com filtros, busca e ordenação
- **✏️ Edição** — Clique em um lead para atualizar status, notas e canal
- **🔄 Reload** — Botão para reimportar dados do Excel

### Dados:
Os leads são lidos automaticamente de:
```
C:\Users\tdeca\OneDrive\Desktop\empresa\Projeto CRM Leads
```

Formatos suportados: `.xlsx`, `.csv`, `.json`

## Stack
- **Backend:** Python + Flask
- **Frontend:** HTML + CSS + JavaScript (sem frameworks)
- **Banco:** SQLite (persistência local)
- **Gráficos:** Chart.js
