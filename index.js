const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const DELAY_SEC = 60; // 1 message per minute

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });
const sessions = new Map();

// ---------- Helper: clean and validate cookie string ----------
function cleanCookieString(raw) {
    if (!raw) return '';
    let cleaned = raw.trim();
    // Remove surrounding quotes
    if ((cleaned.startsWith('"') && cleaned.endsWith('"')) ||
        (cleaned.startsWith("'") && cleaned.endsWith("'"))) {
        cleaned = cleaned.slice(1, -1);
    }
    // Replace newlines and multiple spaces with a single space
    cleaned = cleaned.replace(/\s+/g, ' ').trim();
    // Ensure it ends with a semicolon (optional, but good)
    if (!cleaned.endsWith(';')) cleaned += ';';
    return cleaned;
}

// ---------- Create axios client with full cookie string ----------
function createApiClient(cookieString) {
    const client = axios.create({
        baseURL: 'https://i.instagram.com/api/v1/',
        timeout: 15000,
        headers: {
            'User-Agent': 'Instagram 269.0.0.18.65 (Android; 30; en_US)',
            'Accept-Language': 'en-US,en;q=0.9',
            'X-IG-App-ID': '567067343352427',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': cookieString
        },
        withCredentials: true
    });
    return client;
}

class InstagramMessenger {
    constructor(cookieString, socketId) {
        this.socketId = socketId;
        this.originalCookie = cookieString;
        this.userId = null;
        this.username = null;
        this.loggedIn = false;
        this.client = createApiClient(cookieString);
    }

    // Fetch current user info to validate cookies and get user ID
    async fetchUserInfo() {
        try {
            const res = await this.client.get('accounts/current_user/?edit=phone');
            if (res.data?.user) {
                this.userId = res.data.user.pk;
                this.username = res.data.user.username;
                this.loggedIn = true;
                return { success: true, username: this.username };
            }
            throw new Error('Invalid response from Instagram');
        } catch (err) {
            let message = 'Cookie validation failed. ';
            if (err.response) {
                if (err.response.status === 400) {
                    message += 'Instagram rejected the cookies (likely expired or malformed). Please get fresh cookies.';
                } else if (err.response.status === 403) {
                    message += 'Access forbidden – IP may be blocked or cookies invalid.';
                } else {
                    message += `HTTP ${err.response.status}: ${err.response.data?.message || err.message}`;
                }
            } else {
                message += err.message;
            }
            throw new Error(message);
        }
    }

    // Send a message to a thread (numeric thread ID)
    async sendMessage(threadId, text) {
        if (!this.loggedIn) return { success: false, message: 'Not logged in' };
        try {
            const deviceId = uuidv4();
            const payload = new URLSearchParams();
            payload.append('text', text);
            payload.append('thread_id', threadId);
            payload.append('_uid', this.userId);
            payload.append('_uuid', deviceId);
            const signedBody = JSON.stringify({ text });
            payload.append('signed_body', `SIGNATURE.${Buffer.from(signedBody).toString('base64')}`);
            const res = await this.client.post('direct_v2/threads/broadcast/text/', payload.toString());
            if (res.status === 200 && res.data.status === 'ok') {
                return { success: true };
            }
            throw new Error(res.data.message || 'Unknown error');
        } catch (err) {
            let errorMsg = err.response?.data?.message || err.message;
            // Handle rate limiting
            if (errorMsg.includes('rate limit') || errorMsg.includes('too many')) {
                errorMsg = 'Rate limited – waiting longer';
            }
            return { success: false, message: errorMsg };
        }
    }
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);
    sessions.set(socket.id, {
        messenger: null,
        raidActive: false,
        stats: { sent: 0, failed: 0 }
    });

    socket.emit('console_message', { message: '🔌 Connected – use FULL cookie string (all cookies)', type: 'info' });

    // ---------- Login with full cookie string ----------
    socket.on('login_cookies', async (data) => {
        const sess = sessions.get(socket.id);
        if (!sess) return;
        let rawCookies = data.cookies;
        if (!rawCookies) {
            socket.emit('login_status', { success: false, message: 'Cookie string required' });
            return;
        }
        const cookieString = cleanCookieString(rawCookies);
        if (!cookieString.includes('sessionid=')) {
            socket.emit('login_status', { success: false, message: 'Cookie string must contain sessionid' });
            return;
        }
        socket.emit('console_message', { message: '🔐 Authenticating with Instagram...', type: 'info' });
        const messenger = new InstagramMessenger(cookieString, socket.id);
        try {
            const userInfo = await messenger.fetchUserInfo();
            sess.messenger = messenger;
            sess.stats = { sent: 0, failed: 0 };
            socket.emit('login_status', { success: true, username: userInfo.username });
            socket.emit('console_message', { message: `✅ Logged in as @${userInfo.username}`, type: 'success' });
            socket.emit('stats_update', sess.stats);
        } catch (err) {
            socket.emit('login_status', { success: false, message: err.message });
            socket.emit('console_message', { message: `❌ ${err.message}`, type: 'error' });
        }
    });

    // ---------- Logout ----------
    socket.on('logout', () => {
        const sess = sessions.get(socket.id);
        if (sess) {
            sess.raidActive = false;
            sess.messenger = null;
            sess.stats = { sent: 0, failed: 0 };
            socket.emit('login_status', { success: false });
            socket.emit('console_message', { message: '🔓 Logged out', type: 'info' });
            socket.emit('stats_update', { sent: 0, failed: 0 });
            socket.emit('raid_status', { status: 'STOPPED' });
        }
    });

    // ---------- Start raid (fixed 60s delay) ----------
    socket.on('start_raid', async (data) => {
        const sess = sessions.get(socket.id);
        if (!sess?.messenger?.loggedIn) {
            socket.emit('console_message', { message: '❌ Please login first', type: 'error' });
            return;
        }
        if (sess.raidActive) {
            socket.emit('console_message', { message: '⚠️ Raid already active', type: 'warning' });
            return;
        }
        const { thread_id, message } = data;
        if (!thread_id || !message) {
            socket.emit('console_message', { message: '❌ Thread ID and message required', type: 'error' });
            return;
        }
        // Validate thread ID is numeric
        if (!/^\d+$/.test(thread_id)) {
            socket.emit('console_message', { message: '❌ Thread ID must be numeric (e.g., 123456789012345)', type: 'error' });
            return;
        }
        sess.stats = { sent: 0, failed: 0 };
        sess.raidActive = true;
        socket.emit('stats_update', sess.stats);
        socket.emit('raid_status', { status: 'RAIDING' });
        socket.emit('console_message', { message: `🚀 RAID STARTED | Thread: ${thread_id} | 1 message every ${DELAY_SEC} seconds`, type: 'warning' });

        const raidLoop = async () => {
            let counter = 0;
            while (sess.raidActive) {
                try {
                    const result = await sess.messenger.sendMessage(thread_id, message);
                    if (result.success) {
                        counter++;
                        sess.stats.sent++;
                        socket.emit('message_sent', { count: counter, message });
                        socket.emit('stats_update', sess.stats);
                        socket.emit('console_message', { message: `📨 [#${counter}] Message sent`, type: 'success' });
                    } else {
                        sess.stats.failed++;
                        socket.emit('stats_update', sess.stats);
                        socket.emit('console_message', { message: `❌ Failed: ${result.message}`, type: 'error' });
                    }
                    await sleep(DELAY_SEC * 1000);
                } catch (err) {
                    socket.emit('console_message', { message: `⚠️ Raid error: ${err.message}`, type: 'error' });
                    await sleep(2000);
                }
            }
            socket.emit('console_message', { message: `🛑 RAID STOPPED | Sent: ${sess.stats.sent} | Failed: ${sess.stats.failed}`, type: 'info' });
            socket.emit('raid_status', { status: 'STOPPED' });
        };
        raidLoop().catch(console.error);
    });

    // ---------- Stop raid ----------
    socket.on('stop_raid', () => {
        const sess = sessions.get(socket.id);
        if (sess?.raidActive) {
            sess.raidActive = false;
            socket.emit('console_message', { message: '⏹️ Stopping raid...', type: 'warning' });
        } else {
            socket.emit('console_message', { message: '⚠️ No active raid', type: 'error' });
        }
    });

    socket.on('disconnect', () => {
        console.log(`Client disconnected: ${socket.id}`);
        const sess = sessions.get(socket.id);
        if (sess) sess.raidActive = false;
        sessions.delete(socket.id);
    });
});

// ---------- Web UI (clean, with copy-paste guide) ----------
const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Instagram Raider – Session ID (Full Cookies)</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; font-family: 'Consolas', monospace; }
        body { background:#0a0a0a; color:#0f0; padding:20px; }
        .header { text-align:center; margin-bottom:30px; padding:20px; background:linear-gradient(90deg,red,orange,yellow,green,blue,indigo,violet); background-size:400%; animation:gradient 15s infinite; border-radius:10px; }
        @keyframes gradient { 0%{background-position:0%} 100%{background-position:100%} }
        .glowing-text { font-size:3rem; font-weight:900; color:white; text-shadow:0 0 10px red; }
        .tagline { font-size:1.2rem; color:#0f0; text-align:center; margin-top:10px; }
        .container { display:flex; gap:20px; flex-wrap:wrap; margin-top:20px; }
        .left-panel, .right-panel { flex:1; min-width:300px; background:#111; border-radius:10px; padding:25px; border:2px solid red; }
        .right-panel { border-color:#0f0; }
        .panel-title { color:red; font-size:1.5rem; margin-bottom:20px; text-align:center; border-bottom:1px solid red; padding-bottom:10px; }
        .panel-title.green { color:#0f0; border-bottom-color:#0f0; }
        .input-group { margin-bottom:20px; }
        label { display:block; color:#0f0; margin-bottom:8px; font-weight:bold; }
        input, textarea { width:100%; padding:12px; background:#222; border:1px solid #444; color:#0f0; border-radius:5px; font-size:14px; }
        button { padding:12px 20px; margin:5px 5px 0 0; border:none; border-radius:5px; font-weight:bold; cursor:pointer; text-transform:uppercase; }
        .btn-login { background:linear-gradient(45deg,#ff0000,#ff4400); color:white; }
        .btn-start { background:linear-gradient(45deg,#0f0,#0c0); color:black; }
        .btn-stop { background:linear-gradient(45deg,#f44,#f00); color:white; }
        .stats { background:#222; padding:15px; margin-top:20px; border-radius:5px; }
        .stat-item { display:flex; justify-content:space-between; margin-bottom:8px; }
        .console-container { background:#000; padding:15px; height:450px; overflow-y:auto; border:1px solid #333; border-radius:5px; font-size:13px; }
        .console-line { margin-bottom:5px; padding-left:8px; border-left:2px solid transparent; animation:fadeIn 0.3s; }
        .console-line.success { color:#0f0; border-left-color:#0f0; }
        .console-line.error { color:#f00; border-left-color:#f00; }
        .console-line.info { color:#0ff; border-left-color:#0ff; }
        .console-line.warning { color:#ff0; border-left-color:#ff0; }
        @keyframes fadeIn { from { opacity:0; } to { opacity:1; } }
        .status-indicator { display:inline-block; width:12px; height:12px; border-radius:50%; margin-right:8px; }
        .status-online { background:#0f0; box-shadow:0 0 5px #0f0; }
        .status-offline { background:#f00; box-shadow:0 0 5px #f00; }
        .info-note { background:#222; padding:10px; margin-top:15px; border-radius:5px; font-size:12px; color:#ff0; text-align:center; }
        hr { border-color:#333; margin:15px 0; }
        .guide { background:#1a1a1a; padding:12px; border-radius:8px; margin-top:20px; font-size:12px; color:#aaa; }
    </style>
</head>
<body>
<div class="header">
    <h1 class="glowing-text">PRATIK ALI</h1>
    <div class="tagline">INSTAGRAM RAIDER – SESSION ID (FULL COOKIES) – 1 MSG/MIN</div>
</div>
<div class="container">
    <div class="left-panel">
        <h2 class="panel-title">LOGIN</h2>
        <div class="input-group">
            <label>FULL COOKIE STRING</label>
            <input type="text" id="cookies" placeholder="sessionid=...; csrftoken=...; ds_user_id=...; rur=...; urlgen=...; mid=...; ig_did=...">
            <small style="color:#888">Paste all cookies from Instagram (Application → Cookies)</small>
        </div>
        <button class="btn-login" onclick="login()">LOGIN</button>
        <button class="btn-stop" onclick="logout()">LOGOUT</button>

        <hr>
        <h2 class="panel-title">RAID</h2>
        <div class="input-group">
            <label>THREAD ID</label>
            <input type="text" id="threadId" placeholder="e.g., 123456789012345">
            <small style="color:#888">From DM URL: instagram.com/direct/t/<b>123456789012345</b>/</small>
        </div>
        <div class="input-group">
            <label>MESSAGE</label>
            <textarea id="message" rows="2" placeholder="Message to send">MADARCHOD</textarea>
        </div>
        <div class="info-note">⏱️ Delay fixed at 60 seconds (1 message per minute) – safe & stable</div>
        <button class="btn-start" onclick="startRaid()">START RAID</button>
        <button class="btn-stop" onclick="stopRaid()">STOP RAID</button>

        <div class="stats">
            <div class="stat-item"><span>🔗 STATUS:</span><span id="status">OFFLINE</span></div>
            <div class="stat-item"><span>👤 USER:</span><span id="usernameDisplay">-</span></div>
            <div class="stat-item"><span>📤 SENT:</span><span id="sentCount">0</span></div>
            <div class="stat-item"><span>❌ FAILED:</span><span id="failedCount">0</span></div>
            <div class="stat-item"><span>⚔️ RAID:</span><span id="raidStatus">IDLE</span></div>
        </div>

        <div class="guide">
            <strong>📋 How to get full cookie string:</strong><br>
            1. Open Chrome Incognito → log into Instagram.<br>
            2. Press F12 → Application tab → Cookies → https://www.instagram.com.<br>
            3. Copy each cookie as <code>name=value;</code> (include all: sessionid, csrftoken, ds_user_id, rur, urlgen, mid, ig_did).<br>
            4. Paste the whole string here.
        </div>
    </div>
    <div class="right-panel">
        <h2 class="panel-title green">LIVE CONSOLE</h2>
        <div class="console-container" id="console">
            <div class="console-line info">Ready. Paste full cookie string and click LOGIN.</div>
        </div>
    </div>
</div>
<script>
    let socket = io();
    let isRaid = false;

    function addMsg(msg, type) {
        const c = document.getElementById('console');
        const d = document.createElement('div');
        d.className = 'console-line ' + type;
        d.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg;
        c.appendChild(d);
        d.scrollIntoView({ behavior: 'smooth', block: 'end' });
        while(c.children.length > 250) c.removeChild(c.firstChild);
    }

    socket.on('console_message', data => addMsg(data.message, data.type));
    socket.on('message_sent', data => {
        document.getElementById('sentCount').innerText = parseInt(document.getElementById('sentCount').innerText) + 1;
    });
    socket.on('stats_update', stats => {
        document.getElementById('sentCount').innerText = stats.sent;
        document.getElementById('failedCount').innerText = stats.failed;
    });
    socket.on('login_status', data => {
        if(data.success) {
            document.getElementById('status').innerHTML = '<span class="status-indicator status-online"></span> ONLINE';
            document.getElementById('usernameDisplay').innerText = data.username;
            addMsg('✅ Login successful as ' + data.username, 'success');
        } else {
            document.getElementById('status').innerHTML = '<span class="status-indicator status-offline"></span> OFFLINE';
            addMsg('❌ Login failed: ' + data.message, 'error');
        }
    });
    socket.on('raid_status', data => {
        document.getElementById('raidStatus').innerText = data.status;
        isRaid = (data.status === 'RAIDING');
        if(isRaid) addMsg('🔥 RAID is now active', 'warning');
        else if(data.status === 'STOPPED') addMsg('🛑 Raid stopped', 'info');
    });

    function login() {
        let cookies = document.getElementById('cookies').value.trim();
        if(!cookies) return addMsg('Enter full cookie string', 'error');
        socket.emit('login_cookies', { cookies });
        addMsg('🔐 Authenticating...', 'info');
    }
    function logout() {
        socket.emit('logout');
        document.getElementById('status').innerHTML = '<span class="status-indicator status-offline"></span> OFFLINE';
        document.getElementById('usernameDisplay').innerText = '-';
        document.getElementById('sentCount').innerText = '0';
        document.getElementById('failedCount').innerText = '0';
        addMsg('Logged out', 'info');
    }
    function startRaid() {
        if(isRaid) return addMsg('Raid already running', 'error');
        let tid = document.getElementById('threadId').value.trim();
        let msg = document.getElementById('message').value.trim();
        if(!tid || !msg) return addMsg('Thread ID and message required', 'error');
        if(!/^\d+$/.test(tid)) return addMsg('Thread ID must be numeric', 'error');
        socket.emit('start_raid', { thread_id: tid, message: msg });
        addMsg('Starting raid (1 msg/min)...', 'warning');
    }
    function stopRaid() {
        if(!isRaid) return addMsg('No active raid', 'error');
        socket.emit('stop_raid');
        addMsg('Stopping raid...', 'warning');
    }
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

// ---------- Start server ----------
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n============================================================`);
    console.log(`✅ Instagram Raider (Full Cookie Login) running on port ${PORT}`);
    console.log(`⏱️  1 message per minute (${DELAY_SEC} sec delay)`);
    console.log(`🍪 Login requires full cookie string (sessionid, csrftoken, ds_user_id, rur, urlgen, ...)`);
    console.log(`============================================================\n`);
});
