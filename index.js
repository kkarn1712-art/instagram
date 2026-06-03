const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const DELAY_SEC = 60;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });
const sessions = new Map();

function cleanSessionId(raw) {
    if (!raw) return '';
    let cleaned = raw.trim();
    if ((cleaned.startsWith('"') && cleaned.endsWith('"')) ||
        (cleaned.startsWith("'") && cleaned.endsWith("'"))) {
        cleaned = cleaned.slice(1, -1);
    }
    return cleaned.replace(/\s/g, '');
}

function createApiClient(sessionId) {
    const client = axios.create({
        baseURL: 'https://i.instagram.com/api/v1/',
        timeout: 15000,
        headers: {
            'User-Agent': 'Instagram 269.0.0.18.65 (Android; 30; en_US)',
            'Accept-Language': 'en-US,en;q=0.9',
            'X-IG-App-ID': '567067343352427',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        },
        withCredentials: true
    });
    client.defaults.headers.Cookie = `sessionid=${sessionId};`;
    return client;
}

class InstagramMessenger {
    constructor(sessionId, socketId) {
        this.socketId = socketId;
        this.sessionId = sessionId;
        this.userId = null;
        this.username = null;
        this.loggedIn = false;
        this.client = createApiClient(sessionId);
    }
    async fetchUserInfo() {
        try {
            const res = await this.client.get('accounts/current_user/?edit=phone');
            if (res.data?.user) {
                this.userId = res.data.user.pk;
                this.username = res.data.user.username;
                this.loggedIn = true;
                return { success: true, username: this.username };
            }
            throw new Error('Invalid response');
        } catch (err) {
            throw new Error(err.response?.status === 400 ? 'Session ID malformed or expired' : `Validation failed: ${err.message}`);
        }
    }
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
            if (res.status === 200 && res.data.status === 'ok') return { success: true };
            throw new Error(res.data.message || 'Unknown');
        } catch (err) {
            return { success: false, message: err.response?.data?.message || err.message };
        }
    }
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);
    sessions.set(socket.id, { messenger: null, raidActive: false, stats: { sent: 0, failed: 0 } });
    socket.emit('console_message', { message: 'Connected to Render raider', type: 'info' });

    socket.on('login_session', async (data) => {
        const sess = sessions.get(socket.id);
        if (!sess) return;
        let raw = data.session_id;
        if (!raw) return socket.emit('login_status', { success: false, message: 'Session ID required' });
        const sessionId = cleanSessionId(raw);
        if (sessionId.length < 10) return socket.emit('login_status', { success: false, message: 'Session ID too short' });
        socket.emit('console_message', { message: 'Validating session ID...', type: 'info' });
        const messenger = new InstagramMessenger(sessionId, socket.id);
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

    socket.on('logout', () => {
        const sess = sessions.get(socket.id);
        if (sess) {
            sess.raidActive = false;
            sess.messenger = null;
            sess.stats = { sent: 0, failed: 0 };
            socket.emit('login_status', { success: false });
            socket.emit('console_message', { message: 'Logged out', type: 'info' });
            socket.emit('stats_update', { sent: 0, failed: 0 });
            socket.emit('raid_status', { status: 'STOPPED' });
        }
    });

    socket.on('start_raid', async (data) => {
        const sess = sessions.get(socket.id);
        if (!sess?.messenger?.loggedIn) return socket.emit('console_message', { message: 'Login first', type: 'error' });
        if (sess.raidActive) return socket.emit('console_message', { message: 'Raid active', type: 'warning' });
        const { thread_id, message } = data;
        if (!thread_id || !message) return socket.emit('console_message', { message: 'Thread ID & message required', type: 'error' });
        sess.stats = { sent: 0, failed: 0 };
        sess.raidActive = true;
        socket.emit('stats_update', sess.stats);
        socket.emit('raid_status', { status: 'RAIDING' });
        socket.emit('console_message', { message: `🚀 RAID STARTED | Thread: ${thread_id} | 1 msg/${DELAY_SEC}s`, type: 'warning' });
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
                        socket.emit('console_message', { message: `📨 [#${counter}] Sent`, type: 'success' });
                    } else {
                        sess.stats.failed++;
                        socket.emit('stats_update', sess.stats);
                        socket.emit('console_message', { message: `❌ Failed: ${result.message}`, type: 'error' });
                    }
                    await sleep(DELAY_SEC * 1000);
                } catch (err) {
                    socket.emit('console_message', { message: `⚠️ Error: ${err.message}`, type: 'error' });
                    await sleep(2000);
                }
            }
            socket.emit('console_message', { message: `🛑 STOPPED | Sent: ${sess.stats.sent} | Failed: ${sess.stats.failed}`, type: 'info' });
            socket.emit('raid_status', { status: 'STOPPED' });
        };
        raidLoop();
    });

    socket.on('stop_raid', () => {
        const sess = sessions.get(socket.id);
        if (sess?.raidActive) {
            sess.raidActive = false;
            socket.emit('console_message', { message: 'Stopping raid...', type: 'warning' });
        } else {
            socket.emit('console_message', { message: 'No active raid', type: 'error' });
        }
    });

    socket.on('disconnect', () => {
        const sess = sessions.get(socket.id);
        if (sess) sess.raidActive = false;
        sessions.delete(socket.id);
    });
});

// ---------- FULL HTML UI ----------
const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PRATIK ALI - Instagram Raider (Render)</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; font-family: 'Consolas', monospace; }
        body { background:#0a0a0a; color:#0f0; padding:20px; }
        .header { text-align:center; margin-bottom:30px; padding:20px; background:linear-gradient(90deg,#ff0000,#ff7300,#fffb00,#48ff00,#00ffd5,#002bff,#7a00ff,#ff00c8,#ff0000); background-size:400% 400%; animation:gradient 15s ease infinite; border-radius:10px; box-shadow:0 0 30px rgba(255,0,0,0.5); }
        @keyframes gradient { 0%{background-position:0% 50%;} 50%{background-position:100% 50%;} 100%{background-position:0% 50%;} }
        .glowing-text { font-size:3.5rem; font-weight:900; color:white; text-shadow:0 0 10px #ff0000,0 0 20px #ff0000,0 0 30px #ff0000; margin-bottom:10px; }
        .tagline { font-size:1.5rem; color:#0f0; font-weight:bold; text-shadow:0 0 10px #0f0; }
        .container { display:flex; gap:20px; flex-wrap:wrap; }
        .left-panel, .right-panel { flex:1; min-width:300px; background:#111; border-radius:10px; padding:25px; border:2px solid #ff0000; }
        .right-panel { border-color:#0f0; }
        .panel-title { color:#ff0000; font-size:1.8rem; margin-bottom:20px; text-align:center; border-bottom:2px solid #ff0000; padding-bottom:10px; }
        .panel-title.green { color:#0f0; border-bottom-color:#0f0; }
        .input-group { margin-bottom:20px; }
        .input-group label { display:block; color:#0f0; margin-bottom:8px; font-weight:bold; }
        input, textarea { width:100%; padding:12px; background:#222; border:2px solid #444; border-radius:5px; color:#0f0; font-size:1rem; outline:none; }
        input:focus, textarea:focus { border-color:#0f0; box-shadow:0 0 10px rgba(0,255,0,0.3); }
        button { padding:15px; border:none; border-radius:5px; font-weight:bold; cursor:pointer; text-transform:uppercase; margin:5px; }
        .btn-login { background:linear-gradient(45deg,#ff0000,#ff4400); color:white; }
        .btn-start { background:linear-gradient(45deg,#0f0,#0c0); color:black; }
        .btn-stop { background:linear-gradient(45deg,#ff4444,#ff0000); color:white; }
        .stats { background:#222; padding:20px; border-radius:10px; margin-top:30px; }
        .stat-item { display:flex; justify-content:space-between; margin-bottom:10px; }
        .console-container { background:#000; border-radius:10px; padding:20px; height:500px; overflow-y:auto; border:2px solid #333; font-size:1rem; line-height:1.5; }
        .console-line { margin-bottom:8px; padding-left:10px; border-left:3px solid transparent; animation:fadeIn 0.5s; }
        .console-line.success { color:#0f0; border-left-color:#0f0; text-shadow:0 0 5px #0f0; }
        .console-line.error { color:#ff0000; border-left-color:#ff0000; text-shadow:0 0 5px #ff0000; }
        .console-line.info { color:#0ff; border-left-color:#0ff; text-shadow:0 0 5px #0ff; }
        .console-line.warning { color:#ff0; border-left-color:#ff0; text-shadow:0 0 5px #ff0; }
        @keyframes fadeIn { from { opacity:0; transform:translateX(-10px); } to { opacity:1; transform:translateX(0); } }
        .status-indicator { display:inline-block; width:12px; height:12px; border-radius:50%; margin-right:8px; }
        .status-online { background:#0f0; box-shadow:0 0 10px #0f0; }
        .status-offline { background:#ff0000; box-shadow:0 0 10px #ff0000; }
        .info-note { background:#222; padding:10px; border-radius:5px; margin-top:20px; text-align:center; color:yellow; }
        .counter { text-align:right; margin-bottom:10px; color:#ff0; }
    </style>
</head>
<body>
<div class="header">
    <h1 class="glowing-text">PRATIK ALI</h1>
    <div class="tagline">INSTAGRAM RAIDER | RENDER | 1 MSG/MIN</div>
</div>
<div class="container">
    <div class="left-panel">
        <h2 class="panel-title">CONTROL PANEL</h2>
        <div class="input-group">
            <label>SESSION ID</label>
            <input type="text" id="sessionId" placeholder="Paste Instagram sessionid cookie (no quotes)">
        </div>
        <button class="btn-login" onclick="login()">LOGIN</button>
        <button class="btn-stop" onclick="logout()">LOGOUT</button>

        <div class="input-group" style="margin-top:20px">
            <label>THREAD ID</label>
            <input type="text" id="threadId" placeholder="Numeric thread ID from DM URL">
        </div>
        <div class="input-group">
            <label>MESSAGE</label>
            <textarea id="message" placeholder="Message to send">MADARCHOD</textarea>
        </div>
        <div class="info-note">⏱️ Delay fixed at 60 seconds (1 message per minute) – very safe</div>
        <button class="btn-start" onclick="startRaid()">START RAID</button>
        <button class="btn-stop" onclick="stopRaid()">STOP RAID</button>

        <div class="session-info" style="margin-top:20px">
            <div id="statusDisplay"><span class="status-indicator status-offline"></span> STATUS: OFFLINE</div>
            <div id="usernameDisplay">USERNAME: NOT LOGGED IN</div>
        </div>
        <div class="stats">
            <div class="stat-item"><span>MESSAGES SENT:</span><span id="sentCount">0</span></div>
            <div class="stat-item"><span>FAILED:</span><span id="failedCount">0</span></div>
            <div class="stat-item"><span>RAID STATUS:</span><span id="raidStatus">IDLE</span></div>
            <div class="stat-item"><span>LAST ACTIVITY:</span><span id="lastActivity">--:--:--</span></div>
        </div>
    </div>
    <div class="right-panel">
        <h2 class="panel-title green">LIVE CONSOLE</h2>
        <div class="counter">MESSAGE #<span id="msgCounter">0</span></div>
        <div class="console-container" id="console">
            <div class="console-line info">[SYSTEM] Render raider ready. Enter session ID and thread ID.</div>
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
        c.scrollTop = c.scrollHeight;
        while(c.children.length > 200) c.removeChild(c.firstChild);
    }
    socket.on('console_message', data => addMsg(data.message, data.type));
    socket.on('message_sent', data => {
        document.getElementById('msgCounter').innerText = data.count;
        let sent = parseInt(document.getElementById('sentCount').innerText) + 1;
        document.getElementById('sentCount').innerText = sent;
        document.getElementById('lastActivity').innerText = new Date().toLocaleTimeString();
    });
    socket.on('stats_update', stats => {
        document.getElementById('sentCount').innerText = stats.sent;
        document.getElementById('failedCount').innerText = stats.failed;
    });
    socket.on('login_status', data => {
        if(data.success) {
            document.getElementById('statusDisplay').innerHTML = '<span class="status-indicator status-online"></span> STATUS: ONLINE';
            document.getElementById('usernameDisplay').innerText = 'USERNAME: ' + data.username;
            addMsg('✅ Logged in as ' + data.username, 'success');
        } else {
            addMsg('❌ Login failed: ' + data.message, 'error');
        }
    });
    socket.on('raid_status', data => {
        document.getElementById('raidStatus').innerText = data.status;
        isRaid = (data.status === 'RAIDING');
    });
    function login() {
        let sid = document.getElementById('sessionId').value.trim();
        if(!sid) return addMsg('Enter session ID', 'error');
        socket.emit('login_session', { session_id: sid });
        addMsg('🔐 Logging in...', 'info');
    }
    function logout() {
        socket.emit('logout');
        document.getElementById('statusDisplay').innerHTML = '<span class="status-indicator status-offline"></span> STATUS: OFFLINE';
        document.getElementById('usernameDisplay').innerText = 'USERNAME: NOT LOGGED IN';
        document.getElementById('sentCount').innerText = '0';
        document.getElementById('failedCount').innerText = '0';
        document.getElementById('msgCounter').innerText = '0';
        addMsg('Logged out', 'info');
    }
    function startRaid() {
        if(isRaid) return addMsg('Raid already active', 'error');
        let tid = document.getElementById('threadId').value.trim();
        let msg = document.getElementById('message').value.trim();
        if(!tid || !msg) return addMsg('Thread ID and message required', 'error');
        socket.emit('start_raid', { thread_id: tid, message: msg });
        addMsg('Starting raid (1 msg/min)...', 'warning');
    }
    function stopRaid() {
        if(!isRaid) return addMsg('No active raid', 'error');
        socket.emit('stop_raid');
        addMsg('Stopping raid...', 'warning');
    }
    addMsg('Ready. Paste session ID (from Instagram cookies) and thread ID.', 'info');
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n✅ Instagram raider running on Render | Port ${PORT} | 1 msg/min`);
});
