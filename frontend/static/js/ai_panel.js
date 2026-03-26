document.addEventListener('DOMContentLoaded', () => {
    const container = document.getElementById("app-container");
    const toggleBtn = document.getElementById("ai-toggle-btn");
    const closeBtn = document.getElementById("ai-close-btn");
    const resizer = document.getElementById("ai-resizer");
    const chatInput = document.getElementById("ai-chat-input");
    const sendBtn = document.getElementById("ai-send-btn");
    const chatMessages = document.getElementById("ai-chat-messages");

    // --- Panel Toggle Logic ---
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

    // Restore open state
    if (localStorage.getItem("segmento_ai_panel_open") === "true") {
        container.classList.add("ai-open");
    }

    // --- Resizer Logic ---
    let isResizing = false;

    // Restore width state
    const savedWidth = localStorage.getItem("segmento_ai_panel_width");
    if (savedWidth) {
        document.documentElement.style.setProperty('--ai-width', savedWidth);
    }

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
            // Calculate width in pixels from right edge
            const newWidthPx = window.innerWidth - e.clientX;
            // Constrain width between 250px and 50vw
            const minWidth = 250;
            const maxWidth = window.innerWidth * 0.5;
            let finalWidth = Math.max(minWidth, Math.min(newWidthPx, maxWidth));
            
            document.documentElement.style.setProperty('--ai-width', `${finalWidth}px`);
        });

        document.addEventListener("mouseup", () => {
            if (isResizing) {
                isResizing = false;
                container.classList.remove("is-resizing");
                resizer.classList.remove("active");
                document.body.style.cursor = '';
                
                // Save final width
                const currentWidth = document.documentElement.style.getPropertyValue('--ai-width');
                if (currentWidth) {
                    localStorage.setItem("segmento_ai_panel_width", currentWidth);
                }
            }
        });
    }

    // --- Chat Logic ---
    let currentChatId = null;

    const newBtn = document.getElementById("ai-new-btn");
    const seeAllBtn = document.getElementById("ai-see-all-btn");
    const overlay = document.getElementById("ai-conversations-overlay");
    const overlayCloseBtn = document.getElementById("ai-overlay-close-btn");
    const conversationsList = document.getElementById("ai-conversations-list");

    if (overlayCloseBtn) {
        overlayCloseBtn.addEventListener("click", () => {
            overlay.classList.remove("opacity-100", "scale-100", "pointer-events-auto");
            overlay.classList.add("opacity-0", "scale-95", "pointer-events-none");
        });
    }

    if (seeAllBtn) {
        seeAllBtn.addEventListener("click", () => {
            overlay.classList.remove("opacity-0", "scale-95", "pointer-events-none");
            overlay.classList.add("opacity-100", "scale-100", "pointer-events-auto");
            fetchConversations();
        });
    }

    if (newBtn) {
        newBtn.addEventListener("click", () => {
            currentChatId = null;
            chatMessages.innerHTML = `
                <div class="bg-slate-800/50 rounded-xl p-3 border border-slate-700/50">
                    <p class="text-xs text-slate-300">Hello! I'm your Segmento AI Companion. How can I assist you with your data pipeline today?</p>
                </div>
            `;
            overlay.classList.remove("opacity-100", "scale-100", "pointer-events-auto");
            overlay.classList.add("opacity-0", "scale-95", "pointer-events-none");
        });
    }

    async function fetchConversations() {
        conversationsList.innerHTML = `<div class="flex justify-center p-4"><span class="w-4 h-4 rounded-full border-2 border-cyan-400 border-t-transparent animate-spin"></span></div>`;
        try {
            const res = await fetch('/ai/chats');
            const data = await res.json();
            const chatsList = Array.isArray(data) ? data : (data.chats || []);
            
            if (res.ok) {
                if (chatsList.length === 0) {
                    conversationsList.innerHTML = `<p class="text-[11px] text-slate-500 text-center mt-4">No conversations yet.</p>`;
                    return;
                }
                conversationsList.innerHTML = '';
                const chats = chatsList.slice(0, 20);
                chats.forEach(chat => {
                    const item = document.createElement("div");
                    item.className = "p-3 rounded-xl border border-white/5 bg-slate-900/40 hover:bg-slate-800/80 cursor-pointer transition-all group flex flex-col gap-1 shadow-sm";
                    const title = chat.title || 'Conversation';
                    const timestamp = chat.updated_at || chat.created_at;
                    const timeStr = timestamp ? new Date(timestamp).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) : '';
                    
                    item.innerHTML = `
                        <div class="flex justify-between items-center gap-2">
                            <h4 class="text-[11px] font-bold text-slate-300 group-hover:text-cyan-400 transition-colors truncate">${title}</h4>
                            <span class="text-[9px] font-medium text-slate-500 whitespace-nowrap">${timeStr}</span>
                        </div>
                    `;
                    item.addEventListener("click", () => loadChat(chat.id));
                    conversationsList.appendChild(item);
                });
            } else {
                conversationsList.innerHTML = `<p class="text-[11px] text-red-500/80 text-center mt-4">Failed to load conversations.</p>`;
            }
        } catch (err) {
            conversationsList.innerHTML = `<p class="text-[11px] text-red-500/80 text-center mt-4">Error connecting to server.</p>`;
        }
    }

    async function loadChat(chatId) {
        currentChatId = chatId;
        overlay.classList.remove("opacity-100", "scale-100", "pointer-events-auto");
        overlay.classList.add("opacity-0", "scale-95", "pointer-events-none");
        chatMessages.innerHTML = `<div class="flex justify-center p-4"><span class="w-4 h-4 rounded-full border-2 border-cyan-400 border-t-transparent animate-spin"></span></div>`;
        try {
            const res = await fetch(`/ai/chat/${chatId}`);
            const data = await res.json();
            chatMessages.innerHTML = ''; 
            if (res.ok && data.messages) {
                if (data.messages.length === 0) {
                    chatMessages.innerHTML = `
                        <div class="bg-slate-800/50 rounded-xl p-3 border border-slate-700/50">
                            <p class="text-xs text-slate-300">This conversation is empty.</p>
                        </div>
                    `;
                } else {
                    data.messages.forEach(msg => {
                        appendMessage(msg.role, msg.content);
                    });
                }
            } else {
                appendMessage('ai', 'Failed to load messages.');
            }
        } catch (err) {
            chatMessages.innerHTML = '';
            appendMessage('ai', 'Error connecting to server.');
        }
    }

    function appendMessage(role, content) {
        const msgDiv = document.createElement("div");
        msgDiv.className = `bg-slate-800/50 rounded-xl p-3 border flex flex-col gap-1 ${role === 'user' ? 'border-cyan-500/30 bg-cyan-900/10 ml-4' : 'border-slate-700/50 mr-4'}`;
        
        let label = role === 'user' ? 'You' : 'AI Companion';
        let labelColor = role === 'user' ? 'text-cyan-400' : 'text-slate-400';
        
        msgDiv.innerHTML = `
            <span class="text-[10px] uppercase font-bold tracking-widest ${labelColor}">${label}</span>
            <p class="text-xs text-slate-200 break-words">${content}</p>
        `;
        chatMessages.appendChild(msgDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    async function sendMessage() {
        const text = chatInput.value.trim();
        if (!text) return;

        // Append user message
        appendMessage('user', text);
        chatInput.value = '';

        // Temporary loading message
        const loadingId = "loading-" + Date.now();
        const msgDiv = document.createElement("div");
        msgDiv.id = loadingId;
        msgDiv.className = `bg-slate-800/50 rounded-xl p-3 border border-slate-700/50 mr-4 flex flex-col gap-2`;
        msgDiv.innerHTML = `
            <span class="text-[10px] uppercase font-bold tracking-widest text-slate-400">AI Companion</span>
            <div class="flex items-center gap-2">
                <span class="w-2 h-2 rounded-full bg-cyan-400 animate-pulse"></span> 
                <p class="text-xs text-slate-400">Thinking...</p>
            </div>
        `;
        chatMessages.appendChild(msgDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;

        try {
            const response = await fetch('/ai/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: text, chat_id: currentChatId })
            });
            
            const data = await response.json();
            document.getElementById(loadingId)?.remove();

            if (response.ok && data.message) {
                if (data.chat_id) currentChatId = data.chat_id;
                appendMessage('ai', data.message);
            } else {
                appendMessage('ai', data.error || 'Expected error formatting from AI server.');
            }
        } catch (error) {
            document.getElementById(loadingId)?.remove();
            appendMessage('ai', 'Error connecting to AI service.');
        }
    }

    if (sendBtn) sendBtn.addEventListener("click", sendMessage);
    if (chatInput) {
        chatInput.addEventListener("keypress", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                sendMessage();
            }
        });
    }
});
