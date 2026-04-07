// Account & Billing Dashboard Logic

const PALETTE = ['#22d3ee', '#a78bfa', '#f472b6', '#34d399', '#facc15', '#60a5fa', '#fb923c', '#e879f9', '#4ade80', '#f87171'];

let usageChartInstance = null;

const PLAN_DATA = {
    daily: {
        starter: { price: "₹0", cycle: "Daily Plan", connectors: "5", destinations: "2", records: "1k" },
        pro: { price: "₹5", cycle: "Daily Plan", connectors: "Unlimited", destinations: "8", records: "50k" },
        enterprise: { price: "Custom", cycle: "Daily Plan", connectors: "Custom", destinations: "Custom", records: "Custom" }
    },
    weekly: {
        starter: { price: "₹0", cycle: "Weekly Plan", connectors: "5", destinations: "2", records: "5k" },
        pro: { price: "₹29", cycle: "Weekly Plan", connectors: "Unlimited", destinations: "8", records: "250k" },
        enterprise: { price: "Custom", cycle: "Weekly Plan", connectors: "Custom", destinations: "Custom", records: "Custom" }
    },
    monthly: {
        starter: { price: "₹0", cycle: "Monthly Plan", connectors: "5", destinations: "2", records: "10k" },
        pro: { price: "₹99", cycle: "Monthly Plan", connectors: "Unlimited", destinations: "8", records: "1M" },
        enterprise: { price: "Custom", cycle: "Monthly Plan", connectors: "Custom", destinations: "Custom", records: "Custom" }
    },
    yearly: {
        starter: { price: "₹0", cycle: "Yearly Plan", connectors: "5", destinations: "2", records: "150k" },
        pro: { price: "₹999", cycle: "Yearly Plan", connectors: "Unlimited", destinations: "8", records: "15M" },
        enterprise: { price: "Custom", cycle: "Yearly Plan", connectors: "Custom", destinations: "Custom", records: "Custom" }
    }
};

function switchPlanCycle(cycle, btn) {
    // UI Feedback for tabs
    document.querySelectorAll('.cycle-tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');

    const data = PLAN_DATA[cycle];

    // Update all plan cards
    Object.keys(data).forEach(planId => {
        const card = document.querySelector(`.plan-card[data-plan="${planId}"]`);
        if (!card) return;

        const planInfo = data[planId];
        card.querySelector('.price-value').innerHTML = `${planInfo.price}<span class="text-lg text-slate-500 font-medium">${planInfo.price === 'Custom' ? '' : '/' + (cycle === 'yearly' ? 'yr' : cycle === 'monthly' ? 'mo' : cycle === 'weekly' ? 'wk' : 'day')}</span>`;
        card.querySelector('.cycle-label').innerText = planInfo.cycle;

        if (planId !== 'enterprise') {
            card.querySelector('.connector-limit').innerText = planInfo.connectors;
            card.querySelector('.destination-limit').innerText = planInfo.destinations;
            card.querySelector('.record-limit').innerText = planInfo.records;
        }
    });
}

function initUsageChart() {
    const canvas = document.getElementById('usageOverviewChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    if (usageChartInstance) {
        usageChartInstance.destroy();
    }

    usageChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Records Processed', 'API Calls', 'Storage Usage'],
            datasets: [
                {
                    label: 'Actual Usage',
                    data: [65, 42, 88],
                    backgroundColor: PALETTE[0] + 'CC',
                    borderRadius: 8,
                    borderSkipped: false,
                },
                {
                    label: 'Plan Limit',
                    data: [100, 100, 100],
                    backgroundColor: 'rgba(255,255,255,0.05)',
                    borderRadius: 8,
                    borderSkipped: false,
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#cbd5e1',
                        font: { size: 12, weight: 'bold' },
                        padding: 20,
                        usePointStyle: true,
                        pointStyle: 'circle'
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15,23,42,0.95)',
                    borderColor: 'rgba(34,211,238,0.2)',
                    borderWidth: 1,
                    padding: 16,
                    titleFont: { size: 14, weight: 'bold' },
                    bodyFont: { size: 13 },
                    callbacks: {
                        label: (c) => ` ${c.dataset.label}: ${c.parsed.y}%`
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: '#64748b', font: { size: 11, weight: 'bold' } },
                },
                y: {
                    max: 100,
                    grid: { color: 'rgba(255,255,255,0.04)' },
                    ticks: {
                        color: '#64748b',
                        font: { size: 11 },
                        callback: v => v + '%'
                    }
                }
            }
        }
    });
}

async function loadAccountData() {
    try {
        const res = await fetch("/_backend/api/usage", { credentials: "include" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        // Organization Info
        document.getElementById('accountCreated').innerText = data.account.created_at || "—";
        document.getElementById('accountType').innerText = data.account.is_individual ? "Individual" : "Company";
        document.getElementById('totalSessions').innerText = data.account.total_sessions;
        document.getElementById('companyName').innerText = data.account.company_name || "Personal";

    } catch (err) {
        console.error("Telemetry fetch failed:", err);
    }
}

// Global initialization
document.addEventListener('DOMContentLoaded', () => {
    // Initial charts
    initUsageChart();

    // Default to monthly plan view
    const monthlyBtn = document.querySelector('.cycle-tab[onclick*="monthly"]');
    if (monthlyBtn) switchPlanCycle('monthly', monthlyBtn);

    // Load real data for org section
    loadAccountData();
});
