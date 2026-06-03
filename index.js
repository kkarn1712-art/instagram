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
    socket.emit('console_message', { message: 'Connected to Railway raider', type: 'info' });

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

// Full HTML UI (same as previous, but trimmed for length – you can copy the full version from my earlier message)
const HTML = `<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Instagram Raider (Railway)</title><script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
<style>/* same CSS – keep it */</style></head>
<body>... (paste the full HTML from the previous Railway answer) ...</body></html>`;
app.get('/', (req, res) => res.send(HTML));

server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Railway raider running on port ${PORT}`);
});
