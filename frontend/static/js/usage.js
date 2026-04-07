// ── Professional Time Formatting ──
function formatAge(value) {
    if (!value) return '—';

    let diffMinutes;

    // ISO timestamp string
    if (typeof value === 'string' && value.includes('T')) {
        const then = new Date(value);
        if (isNaN(then)) return '—';
        diffMinutes = Math.round((Date.now() - then.getTime()) / 60000);
    } else {
        // plain number (minutes)
        diffMinutes = Math.round(parseFloat(value));
        if (isNaN(diffMinutes)) return '—';
    }

    if (diffMinutes < 1) return 'Just now';
    if (diffMinutes < 60) return `${diffMinutes}m ago`;
    const hours = Math.floor(diffMinutes / 60);
    const mins = diffMinutes % 60;
    if (hours < 24) return mins > 0 ? `${hours}h ${mins}m ago` : `${hours}h ago`;
    const days = Math.floor(hours / 24);
    const hrs = hours % 24;
    if (days < 7) return hrs > 0 ? `${days}d ${hrs}h ago` : `${days}d ago`;
    return `${Math.floor(days / 7)}w ago`;
}

// ── Chart Helpers ──
const PALETTE = ['#22d3ee', '#a78bfa', '#f472b6', '#34d399', '#facc15', '#60a5fa', '#fb923c', '#e879f9', '#4ade80', '#f87171'];

function buildLineChart(ctx, labels, values) {
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Records Synced',
                data: values,
                borderColor: '#22d3ee',
                backgroundColor: 'rgba(34,211,238,0.08)',
                borderWidth: 2.5,
                pointBackgroundColor: '#22d3ee',
                pointBorderColor: '#0f172a',
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 7,
                fill: true,
                tension: 0.45,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(15,23,42,0.92)',
                    borderColor: 'rgba(34,211,238,0.3)',
                    borderWidth: 1,
                    titleColor: '#a1b0c9',
                    bodyColor: '#22d3ee',
                    padding: 12,
                    callbacks: { label: c => ` ${c.parsed.y.toLocaleString()} records` }
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: { color: '#64748b', font: { size: 11 }, maxTicksLimit: 7 },
                    border: { color: 'rgba(255,255,255,0.06)' }
                },
                y: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: {
                        color: '#64748b', font: { size: 11 },
                        callback: v => v >= 1000 ? (v / 1000).toFixed(1) + 'k' : v
                    },
                    border: { color: 'rgba(255,255,255,0.06)' }
                }
            }
        }
    });
}

function buildBarChart(ctx, labels, values) {
    const colors = labels.map((_, i) => PALETTE[i % PALETTE.length]);
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Records',
                data: values,
                backgroundColor: colors.map(c => c + '33'),
                borderColor: colors,
                borderWidth: 2,
                borderRadius: 8,
                borderSkipped: false,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(15,23,42,0.92)',
                    borderColor: 'rgba(34,211,238,0.3)',
                    borderWidth: 1,
                    titleColor: '#a1b0c9',
                    bodyColor: '#e2e8f0',
                    padding: 12,
                    callbacks: { label: c => ` ${c.parsed.y.toLocaleString()} records` }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: '#64748b', font: { size: 11 } },
                    border: { color: 'rgba(255,255,255,0.06)' }
                },
                y: {
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: {
                        color: '#64748b', font: { size: 11 },
                        callback: v => v >= 1000 ? (v / 1000).toFixed(0) + 'k' : v
                    },
                    border: { color: 'rgba(255,255,255,0.06)' }
                }
            }
        }
    });
}

async function loadUsage() {
    try {
        const res = await fetch("/_backend/api/usage", { credentials: "include" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        // Basic Stats
        const s = data.stats || {};
        document.getElementById('connectedConnectors').innerText = s.connectedConnectors ?? data.connectors.connected;
        document.getElementById('totalRecords').innerText = (s.totalRecords ?? data.data_volume.total_records_synced).toLocaleString();
        document.getElementById('activeDestinations').innerText = s.activeDestinations ?? data.destinations.active;
        document.getElementById('successRate').innerText = (s.successRate ?? data.health.sync_success_rate) + "%";

        // Account
        document.getElementById('accountCreated').innerText = data.account.created_at || "—";
        document.getElementById('accountType').innerText = data.account.is_individual ? "Individual" : "Company";
        document.getElementById('totalSessions').innerText = data.account.total_sessions;
        document.getElementById('apiCalls').innerText = data.account.total_api_calls.toLocaleString();
        document.getElementById('companyName').innerText = data.account.company_name || "Personal";

        if (data.account.created_at) {
            const created = new Date(data.account.created_at);
            document.getElementById('accountAge').innerText = Math.floor((Date.now() - created) / (1000 * 60 * 60 * 24));
        }

        // Connector Detail
        document.getElementById('firstConnected').innerText = data.connectors.first_connected || "N/A";
        document.getElementById('lastSync').innerText = data.connectors.last_sync ? formatAge(data.connectors.last_sync) : "N/A";
        document.getElementById('totalRuns').innerText = data.connectors.total_sync_runs;
        document.getElementById('failedRuns').innerText = data.connectors.failed_sync_runs;
        document.getElementById('scheduledJobs').innerText = data.scheduler.scheduled_jobs;

        // Destination Table
        const table = document.getElementById("destinationTable");
        table.innerHTML = "";
        data.destinations.rows_per_destination.forEach(d => {
            table.innerHTML += `
                <tr class="border-b border-white/5 hover:bg-white/5 transition-all">
                    <td class="py-4 font-bold text-white">${d[0]}</td>
                    <td class="py-4 text-center font-mono text-cyan-400 font-bold">${d[1].toLocaleString()}</td>
                    <td class="py-4 text-right text-slate-500 text-[11px]">${data.destinations.last_push_time ? formatAge(data.destinations.last_push_time) : "-"}</td>
                </tr>`;
        });

        // ── Data Freshness ──
        const freshnessEl = document.getElementById('dataFreshness');
        if (freshnessEl) {
            freshnessEl.textContent = formatAge(data.last_sync);
        }

        // ── Automation Score & Pie Chart ──
        const breakdown = data.connectors.sync_type_breakdown || {};
        const manual = breakdown.manual || 0;
        const scheduled = breakdown.scheduled || 0;
        const totalSyncs = manual + scheduled;

        // Automation Score circle
        const automationPct = totalSyncs > 0 ? Math.round((scheduled / totalSyncs) * 100) : 0;
        document.getElementById('automationScore').innerText = automationPct + "%";
        const progressCircle = document.getElementById('automationProgress');
        const offset = 339 - (339 * (automationPct / 100));
        progressCircle.style.strokeDashoffset = offset;

        // Pipeline Automation Doughnut
        new Chart(document.getElementById('syncPie'), {
            type: "doughnut",
            data: {
                labels: ["Manual", "Scheduled"],
                datasets: [{
                    data: [manual, scheduled],
                    backgroundColor: ["#64748b", "#22d3ee"],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: "75%",
                plugins: {
                    legend: { position: "bottom", labels: { color: "#cbd5e1", font: { size: 13 } } },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.raw;
                                const pct = totalSyncs > 0 ? ((val / totalSyncs) * 100).toFixed(1) : 0;
                                return `${ctx.label}: ${val.toLocaleString()} runs (${pct}%)`;
                            }
                        }
                    }
                }
            }
        });

        // ── Intelligence Feed ──
        const insights = [];
        if (data.health.sync_success_rate === 100)
            insights.push("✅ All connectors syncing successfully");
        if (data.destinations.active > 0)
            insights.push("📦 Data flowing into " + data.destinations.active + " active destinations");
        if (data.data_volume.total_records_synced > 0)
            insights.push("🚀 " + data.data_volume.total_records_synced.toLocaleString() + " records successfully processed");
        if (data.connectors.connected < 5)
            insights.push("⚡ Connect more sources to unlock full analytics value");

        document.getElementById("usageInsights").innerHTML = insights.map(i => `
            <div class="insight-item p-5 rounded-2xl flex items-start gap-4 transition-transform hover:scale-[1.02]">
                <div class="mt-1 text-cyan-400 shadow-cyan-500/50 shadow-sm">
                    <svg width="18" height="18" fill="currentColor" viewBox="0 0 24 24"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>
                </div>
                <div>
                    <p class="text-slate-200 text-sm font-semibold leading-tight">${i}</p>
                </div>
            </div>
        `).join("");

        // ── Data Throughput Chart ──
        Chart.defaults.color = '#64748b';
        Chart.defaults.font.family = 'Plus Jakarta Sans';

        const ctxBar = document.getElementById('connectorBar').getContext('2d');
        const grad = ctxBar.createLinearGradient(0, 0, 0, 300);
        grad.addColorStop(0, '#22d3ee');
        grad.addColorStop(1, 'rgba(34, 211, 238, 0)');

        new Chart(ctxBar, {
            type: "bar",
            data: {
                labels: data.data_volume.records_per_connector.map(x => x[0]),
                datasets: [{
                    label: "Records",
                    data: data.data_volume.records_per_connector.map(x => x[1]),
                    backgroundColor: grad,
                    borderRadius: 10,
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { grid: { color: 'rgba(255,255,255,0.05)' } },
                    x: { grid: { display: false } }
                }
            }
        });

        // ── Daily Usage Line Chart ──
        const dailyCtx = document.getElementById('dailyUsageChart');
        if (dailyCtx && Array.isArray(data.daily_usage)) {
            const labels = data.daily_usage.map(d => {
                const dt = new Date(d.date + 'T00:00:00');
                return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            });
            const values = data.daily_usage.map(d => d.rows);
            buildLineChart(dailyCtx, labels, values);
        }

        // ── Top Connectors Bar Chart ──
        const topCtx = document.getElementById('topConnectorsChart');
        if (topCtx && Array.isArray(data.top_connectors) && data.top_connectors.length) {
            const labels = data.top_connectors.map(c =>
                c.source.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())
            );
            const values = data.top_connectors.map(c => c.rows);
            buildBarChart(topCtx, labels, values);
        } else if (topCtx) {
            topCtx.style.display = 'none';
            const msg = document.createElement('p');
            msg.className = 'text-slate-500 text-sm text-center pt-16';
            msg.textContent = 'No sync data yet. Run your first connector sync to see results here.';
            topCtx.parentNode.appendChild(msg);
        }

    } catch (err) {
        console.error("Telemetry fetch failed:", err);
    }
}

loadUsage();