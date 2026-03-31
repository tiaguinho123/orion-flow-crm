/* ═══════════════════════════════════════════════════════════
   Charts Module — Chart.js visualizations
   ═══════════════════════════════════════════════════════════ */

// Chart.js global defaults for dark theme
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(99, 102, 241, 0.1)';
Chart.defaults.font.family = "'Inter', sans-serif";

let funnelChart = null;
let dailyChart = null;
let channelChart = null;

/**
 * Create or update the funnel bar chart.
 */
function renderFunnelChart(funnel) {
    const ctx = document.getElementById('funnelChart');
    if (!ctx) return;

    const labels = funnel.map(f => f.stage);
    const data = funnel.map(f => f.count);
    
    const colors = [
        'rgba(148, 163, 184, 0.8)',   // Novo — slate
        'rgba(59, 130, 246, 0.8)',     // Contatado — blue
        'rgba(6, 182, 212, 0.8)',      // Visualizou — cyan
        'rgba(249, 115, 22, 0.8)',     // Clicou — orange
        'rgba(16, 185, 129, 0.8)',     // Respondeu — green
        'rgba(245, 158, 11, 0.8)',     // Reunião — yellow
        'rgba(139, 92, 246, 0.8)',     // Fechado — purple
    ];

    const borderColors = colors.map(c => c.replace('0.8', '1'));

    if (funnelChart) {
        funnelChart.data.labels = labels;
        funnelChart.data.datasets[0].data = data;
        funnelChart.update();
        return;
    }

    funnelChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: colors,
                borderColor: borderColors,
                borderWidth: 1,
                borderRadius: 6,
                barPercentage: 0.7,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1a1f35',
                    titleColor: '#f1f5f9',
                    bodyColor: '#94a3b8',
                    borderColor: 'rgba(99, 102, 241, 0.3)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 10,
                    displayColors: false,
                    callbacks: {
                        label: function(context) {
                            return `${context.parsed.x} leads`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 1,
                        font: { size: 11 }
                    },
                    grid: {
                        color: 'rgba(99, 102, 241, 0.06)'
                    }
                },
                y: {
                    ticks: {
                        font: { size: 12, weight: '500' }
                    },
                    grid: { display: false }
                }
            }
        }
    });
}

/**
 * Create or update the daily contacts line chart.
 */
function renderDailyChart(dailyContacts) {
    const ctx = document.getElementById('dailyChart');
    if (!ctx) return;

    let labels, data;

    if (dailyContacts && dailyContacts.length > 0) {
        labels = dailyContacts.map(d => {
            const date = new Date(d.date);
            return date.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' });
        });
        data = dailyContacts.map(d => d.count);
    } else {
        // Show placeholder data
        const today = new Date();
        labels = [];
        data = [];
        for (let i = 6; i >= 0; i--) {
            const d = new Date(today);
            d.setDate(d.getDate() - i);
            labels.push(d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' }));
            data.push(0);
        }
    }

    if (dailyChart) {
        dailyChart.data.labels = labels;
        dailyChart.data.datasets[0].data = data;
        dailyChart.update();
        return;
    }

    dailyChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                borderColor: '#06b6d4',
                backgroundColor: 'rgba(6, 182, 212, 0.1)',
                borderWidth: 2.5,
                fill: true,
                tension: 0.4,
                pointBackgroundColor: '#06b6d4',
                pointBorderColor: '#0b0f1a',
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1a1f35',
                    titleColor: '#f1f5f9',
                    bodyColor: '#94a3b8',
                    borderColor: 'rgba(6, 182, 212, 0.3)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 10,
                    displayColors: false,
                    callbacks: {
                        label: function(context) {
                            return `${context.parsed.y} contatos`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { font: { size: 10 } },
                    grid: { color: 'rgba(99, 102, 241, 0.06)' }
                },
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 1,
                        font: { size: 11 }
                    },
                    grid: { color: 'rgba(99, 102, 241, 0.06)' }
                }
            }
        }
    });
}

/**
 * Create or update the channel distribution pie chart.
 */
function renderChannelChart(channelCounts) {
    const ctx = document.getElementById('channelChart');
    if (!ctx) return;

    const channelConfig = {
        'Email':         { color: 'rgba(59, 130, 246, 0.85)',  border: '#3b82f6' },
        'Instagram DM':  { color: 'rgba(236, 72, 153, 0.85)',  border: '#ec4899' },
        'Facebook DM':   { color: 'rgba(96, 165, 250, 0.85)',  border: '#60a5fa' },
        'WhatsApp':      { color: 'rgba(16, 185, 129, 0.85)',  border: '#10b981' },
        'Não definido':  { color: 'rgba(100, 116, 139, 0.6)',  border: '#64748b' },
    };

    const labels = Object.keys(channelCounts);
    const data = Object.values(channelCounts);
    const bgColors = labels.map(l => (channelConfig[l] || channelConfig['Não definido']).color);
    const borderColors = labels.map(l => (channelConfig[l] || channelConfig['Não definido']).border);

    if (channelChart) {
        channelChart.data.labels = labels;
        channelChart.data.datasets[0].data = data;
        channelChart.data.datasets[0].backgroundColor = bgColors;
        channelChart.data.datasets[0].borderColor = borderColors;
        channelChart.update();
        return;
    }

    channelChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: bgColors,
                borderColor: borderColors,
                borderWidth: 2,
                hoverOffset: 8,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '60%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 12,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        font: { size: 11 }
                    }
                },
                tooltip: {
                    backgroundColor: '#1a1f35',
                    titleColor: '#f1f5f9',
                    bodyColor: '#94a3b8',
                    borderColor: 'rgba(99, 102, 241, 0.3)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 10,
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = total > 0 ? Math.round(context.parsed / total * 100) : 0;
                            return ` ${context.label}: ${context.parsed} (${pct}%)`;
                        }
                    }
                }
            }
        }
    });
}
