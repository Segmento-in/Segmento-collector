async function loadUsage() {
    try {
        const res = await fetch("http://localhost:4000/api/usage", { credentials: "include" });
        const data = await res.json();

        // Basic Stats
        document.getElementById('connectedConnectors').innerText = data.connectors.connected;
        document.getElementById('totalRecords').innerText = data.data_volume.total_records_synced.toLocaleString();
        document.getElementById('activeDestinations').innerText = data.destinations.active;
        document.getElementById('successRate').innerText = data.health.sync_success_rate + "%";

        // Account
        document.getElementById('accountCreated').innerText = data.account.created_at;
        document.getElementById('accountType').innerText = data.account.is_individual ? "Individual" : "Company";
        document.getElementById('totalSessions').innerText = data.account.total_sessions;
        document.getElementById('apiCalls').innerText = data.account.total_api_calls.toLocaleString();
        document.getElementById('companyName').innerText = data.account.company_name || "Personal";

        const created = new Date(data.account.created_at);
        document.getElementById('accountAge').innerText = Math.floor((Date.now() - created) / (1000 * 60 * 60 * 24));

        // Connector Detail
        document.getElementById('firstConnected').innerText = data.connectors.first_connected || "N/A";
        document.getElementById('lastSync').innerText = data.connectors.last_sync || "N/A";
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
                    <td class="py-4 text-right text-slate-500 text-[11px]">${data.destinations.last_push_time || "-"}</td>
                </tr>`;
        });

        // ── Data Freshness ──
        const lastSyncTime = data.connectors.last_sync ? new Date(data.connectors.last_sync) : null;
        const freshnessEl = document.getElementById('dataFreshness');
        if (lastSyncTime) {
            const diffMin = Math.round((Date.now() - lastSyncTime) / 60000);
            let statusText = "Healthy";
            let colorClass = "text-green-400";

            if (diffMin > 60 && diffMin <= 360) { statusText = "Stale"; colorClass = "text-yellow-400"; }
            else if (diffMin > 360) { statusText = "Delayed"; colorClass = "text-red-400"; }

            freshnessEl.innerHTML = `Last sync: ${diffMin} min ago<br><span class="${colorClass}">${statusText}</span>`;
        } else {
            freshnessEl.innerText = "No recent sync";
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

        // ── Intelligence Feed – reverted to your original logic & rendering ──
        const insights = [];
        if(data.health.sync_success_rate === 100)
            insights.push("✅ All connectors syncing successfully");
        if(data.destinations.active > 0)
            insights.push("📦 Data flowing into " + data.destinations.active + " active destinations");
        if(data.data_volume.total_records_synced > 0)
            insights.push("🚀 " + data.data_volume.total_records_synced.toLocaleString() + " records successfully processed");
        if(data.connectors.connected < 5)
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

        // Bar Chart (unchanged)
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

    } catch (err) {
        console.error("Telemetry fetch failed:", err);
    }
}

loadUsage();