document.addEventListener('DOMContentLoaded', () => {
    const container = document.getElementById("app-container");
    const toggleBtn = document.getElementById("ai-toggle-btn");
    const closeBtn = document.getElementById("ai-close-btn");
    const resizer = document.getElementById("ai-resizer");
    const chatInput = document.getElementById("ai-chat-input");
    const sendBtn = document.getElementById("ai-send-btn");
    const chatMessages = document.getElementById("ai-chat-messages");

    // ─── Panel Toggle ────────────────────────────────────────────
    function toggleAIPanel() {
        container.classList.toggle("ai-open");
        const isOpen = container.classList.contains("ai-open");
        localStorage.setItem("segmento_ai_panel_open", isOpen ? "true" : "false");
    }

    if (toggleBtn) toggleBtn.addEventListener("click", toggleAIPanel);

    if (closeBtn) {
        closeBtn.addEventListener("click", () => {
            container.classList.remove("ai-open");
            localStorage.setItem("segmento_ai_panel_open", "false");
        });
    }

    if (localStorage.getItem("segmento_ai_panel_open") === "true") {
        container.classList.add("ai-open");
    }

    // ─── Resizer ─────────────────────────────────────────────────
    let isResizing = false;
    const savedWidth = localStorage.getItem("segmento_ai_panel_width");
    if (savedWidth) document.documentElement.style.setProperty('--ai-width', savedWidth);

    if (resizer) {
        resizer.addEventListener("mousedown", (e) => {
            isResizing = true;
            container.classList.add("is-resizing");
            resizer.classList.add("active");
            document.body.style.cursor = 'ew-resize';
            e.preventDefault();
        });

        document.addEventListener("mousemove", (e) => {
            if (!isResizing) return;
            const newWidthPx = window.innerWidth - e.clientX;
            const minWidth = 250;
            const maxWidth = window.innerWidth * 0.5;
            const finalWidth = Math.max(minWidth, Math.min(newWidthPx, maxWidth));
            document.documentElement.style.setProperty('--ai-width', `${finalWidth}px`);
        });

        document.addEventListener("mouseup", () => {
            if (isResizing) {
                isResizing = false;
                container.classList.remove("is-resizing");
                resizer.classList.remove("active");
                document.body.style.cursor = '';
                const currentWidth = document.documentElement.style.getPropertyValue('--ai-width');
                if (currentWidth) localStorage.setItem("segmento_ai_panel_width", currentWidth);
            }
        });
    }

    // ─── Chat state ──────────────────────────────────────────────
    let currentChatId = null;

    const newBtn = document.getElementById("ai-new-btn");
    const seeAllBtn = document.getElementById("ai-see-all-btn");
    const overlay = document.getElementById("ai-conversations-overlay");
    const overlayCloseBtn = document.getElementById("ai-overlay-close-btn");
    const conversationsList = document.getElementById("ai-conversations-list");

    if (overlayCloseBtn) {
        overlayCloseBtn.addEventListener("click", () => _hideOverlay());
    }

    if (seeAllBtn) {
        seeAllBtn.addEventListener("click", () => {
            _showOverlay();
            fetchConversations();
        });
    }

    if (newBtn) {
        newBtn.addEventListener("click", () => {
            currentChatId = null;
            _resetChat();
            _hideOverlay();
        });
    }

    function _showOverlay() {
        overlay.classList.remove("opacity-0", "scale-95", "pointer-events-none");
        overlay.classList.add("opacity-100", "scale-100", "pointer-events-auto");
    }

    function _hideOverlay() {
        overlay.classList.remove("opacity-100", "scale-100", "pointer-events-auto");
        overlay.classList.add("opacity-0", "scale-95", "pointer-events-none");
    }

    function _resetChat() {
        chatMessages.innerHTML = `
            <div class="bg-slate-800/50 rounded-xl p-3 border border-slate-700/50">
                <p class="text-xs text-slate-300">
                    Hello! I'm your Segmento AI Companion. I can help you
                    <strong class="text-cyan-400">connect connectors</strong>,
                    <strong class="text-cyan-400">run syncs</strong>, and
                    <strong class="text-cyan-400">manage your data pipeline</strong>.
                    What would you like to do?
                </p>
            </div>`;
    }

    // ─── Fetch conversation list ──────────────────────────────────
    async function fetchConversations() {
        conversationsList.innerHTML = `<div class="flex justify-center p-4">
            <span class="w-4 h-4 rounded-full border-2 border-cyan-400 border-t-transparent animate-spin"></span>
        </div>`;
        try {
            const res = await fetch('/ai/chats');
            const data = await res.json();
            const chatsList = Array.isArray(data) ? data : (data.chats || []);

            if (!res.ok || chatsList.length === 0) {
                conversationsList.innerHTML =
                    `<p class="text-[11px] text-slate-500 text-center mt-4">No conversations yet.</p>`;
                return;
            }

            conversationsList.innerHTML = '';
            chatsList.slice(0, 20).forEach(chat => {
                const item = document.createElement("div");
                item.className = "p-3 rounded-xl border border-white/5 bg-slate-900/40 hover:bg-slate-800/80 cursor-pointer transition-all group flex flex-col gap-1 shadow-sm";
                const title = chat.title || 'Conversation';
                const timestamp = chat.updated_at || chat.created_at;
                const timeStr = timestamp
                    ? new Date(timestamp).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
                    : '';

                item.innerHTML = `
                    <div class="flex justify-between items-center gap-2">
                        <h4 class="text-[11px] font-bold text-slate-300 group-hover:text-cyan-400 transition-colors truncate">${_esc(title)}</h4>
                        <span class="text-[9px] font-medium text-slate-500 whitespace-nowrap">${timeStr}</span>
                    </div>`;
                item.addEventListener("click", () => loadChat(chat.id));
                conversationsList.appendChild(item);
            });
        } catch {
            conversationsList.innerHTML =
                `<p class="text-[11px] text-red-500/80 text-center mt-4">Error connecting to server.</p>`;
        }
    }

    // ─── Load an existing chat ────────────────────────────────────
    async function loadChat(chatId) {
        currentChatId = chatId;
        _hideOverlay();
        chatMessages.innerHTML = `<div class="flex justify-center p-4">
            <span class="w-4 h-4 rounded-full border-2 border-cyan-400 border-t-transparent animate-spin"></span>
        </div>`;
        try {
            const res = await fetch(`/ai/chat/${chatId}`);
            const data = await res.json();
            chatMessages.innerHTML = '';
            if (res.ok && data.messages) {
                if (data.messages.length === 0) {
                    _resetChat();
                } else {
                    data.messages.forEach(msg => appendMessage(msg.role, msg.content));
                }
            } else {
                appendMessage('ai', 'Failed to load messages.');
            }
        } catch {
            chatMessages.innerHTML = '';
            appendMessage('ai', 'Error connecting to server.');
        }
    }

    // ─── HTML escape helper ──────────────────────────────────────
    function _esc(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    // ─── Markdown-lite formatter ─────────────────────────────────
    function _format(text) {
        return text
            .replace(/\*\*(.+?)\*\*/g, '<strong class="text-cyan-300">$1</strong>')
            .replace(/\n•/g, '<br>•')
            .replace(/\n/g, '<br>');
    }

    // ─── Render a connector link card ────────────────────────────
    function _renderLinks(connectors, links) {
        if (!links || links.length === 0) return '';
        const items = links.map((href, i) => {
            const label = connectors[i]
                ? connectors[i].replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
                : href;
            return `<a href="${_esc(href)}"
                       class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-cyan-500/20
                              hover:bg-cyan-500/40 border border-cyan-500/40 rounded-lg
                              text-[11px] font-semibold text-cyan-300 transition-colors"
                       target="_blank">
                        <span>🔗</span> ${_esc(label)}
                    </a>`;
        }).join('');
        return `<div class="flex flex-wrap gap-2 mt-2">${items}</div>`;
    }

    // ─── Append a chat bubble ─────────────────────────────────────
    /**
     * @param {string} role        - "user" | "ai"
     * @param {string} content     - message text
     * @param {object} [richData]  - optional: { type, connectors, links }
     */
    function appendMessage(role, content, richData = null) {
        const msgDiv = document.createElement("div");
        msgDiv.className = [
            "bg-slate-800/50 rounded-xl p-3 border flex flex-col gap-1",
            role === 'user'
                ? 'border-cyan-500/30 bg-cyan-900/10 ml-4'
                : 'border-slate-700/50 mr-4',
        ].join(' ');

        const label = role === 'user' ? 'You' : 'AI Companion';
        const labelColor = role === 'user' ? 'text-cyan-400' : 'text-slate-400';

        let extraHtml = '';

        // Rich rendering for connect / sync / list / greeting / help
        if (richData && role === 'ai') {
            const { type, connectors = [], links = [] } = richData;

            if ((type === 'connect' || type === 'sync' || type === 'status') && links.length > 0) {
                extraHtml = _renderLinks(connectors, links);
            }

            if (type === 'list' && richData.data && richData.data.connectors) {
                const count = richData.data.connectors.length;
                extraHtml += `<p class="text-[10px] text-slate-500 mt-1">${count} connectors available</p>`;
            }
        }

        msgDiv.innerHTML = `
            <span class="text-[10px] uppercase font-bold tracking-widest ${labelColor}">${label}</span>
            <p class="text-xs text-slate-200 break-words">${_format(_esc(content))}</p>
            ${extraHtml}`;

        chatMessages.appendChild(msgDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    // ─── Send message ─────────────────────────────────────────────
    async function sendMessage() {
        const text = chatInput.value.trim();
        if (!text) return;

        appendMessage('user', text);
        chatInput.value = '';

        // Loading indicator
        const loadingId = "loading-" + Date.now();
        const loadDiv = document.createElement("div");
        loadDiv.id = loadingId;
        loadDiv.className = "bg-slate-800/50 rounded-xl p-3 border border-slate-700/50 mr-4 flex flex-col gap-2";
        loadDiv.innerHTML = `
            <span class="text-[10px] uppercase font-bold tracking-widest text-slate-400">AI Companion</span>
            <div class="flex items-center gap-2">
                <span class="w-2 h-2 rounded-full bg-cyan-400 animate-pulse"></span>
                <p class="text-xs text-slate-400">Thinking…</p>
            </div>`;
        chatMessages.appendChild(loadDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;

        try {
            const response = await fetch('/ai/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: text, chat_id: currentChatId }),
            });

            const data = await response.json();
            document.getElementById(loadingId)?.remove();

            if (response.ok && data.message) {
                if (data.chat_id) currentChatId = data.chat_id;

                // Pass rich data for link rendering
                appendMessage('ai', data.message, {
                    type: data.type,
                    connectors: data.connectors || [],
                    links: data.links || [],
                    data: data.data,
                });
            } else {
                appendMessage('ai', data.error || 'An unexpected error occurred.');
            }
        } catch {
            document.getElementById(loadingId)?.remove();
            appendMessage('ai', 'Error connecting to the AI service. Please try again.');
        }
    }

    if (sendBtn) sendBtn.addEventListener("click", sendMessage);
    if (chatInput) {
        chatInput.addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) { 
                e.preventDefault(); 
                sendMessage(); 
            }
        });

        chatInput.addEventListener("input", function() {
            this.style.height = 'auto';
            const newHeight = Math.min(this.scrollHeight, 120);
            this.style.height = (newHeight > 34 ? newHeight : 34) + 'px';
        });
    }
});