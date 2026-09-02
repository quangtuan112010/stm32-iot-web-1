const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const MQTT_BROKER = process.env.MQTT_BROKER;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;

const TOPIC_UPLINK = 'stm32/sensor-data';
const TOPIC_DOWNLINK = 'stm32/control-value';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Biến lưu trạng thái và gói tin tức thời gần nhất
let lastDeviceTime = 0;
let lastTelemetryPacket = null;

function getVietnamTimeString() {
    return new Date().toLocaleString('vi-VN', {
        timeZone: 'Asia/Ho_Chi_Minh',
        hour12: false,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        day: '2-digit',
        month: '2-digit',
        year: 'numeric'
    });
}

function formatSupabaseTime(rawTime) {
    if (!rawTime) return '';
    try {
        const clean = rawTime.replace('T', ' ').split('.')[0];
        const [dPart, tPart] = clean.split(' ');
        const [y, m, d] = dPart.split('-');
        return `${tPart} ${d}/${m}/${y}`;
    } catch (e) {
        return rawTime;
    }
}

const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

// Nhận gói tin JSON Uplink từ vi điều khiển
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const timeVN = getVietnamTimeString();
        lastDeviceTime = Date.now();
        let data = {};

        try {
            data = JSON.parse(rawStr);
        } catch (e) {
            console.error('[JSON ERROR]: Khong the parse payload:', rawStr);
            return;
        }

        console.log(`[MQTT] [${timeVN}] Nhan du lieu:`, data);

        // Lưu gói tin mới nhất vào RAM server để phục vụ client mở web tức thì
        lastTelemetryPacket = {
            time_vn: timeVN,
            device_timestamp: lastDeviceTime,
            data: data
        };

        const { error } = await supabase
            .from('telemetry_logs')
            .insert([{
                T: parseFloat(data.T) || 0.0,
                S: parseFloat(data.S) || 0.0,
                pH: parseFloat(data.pH) || 0.0,
                DO: parseFloat(data.DO) || 0.0,
                alk: parseFloat(data.alk) !== undefined ? parseFloat(data.alk) : -1.0,
                btri: parseFloat(data.btri) || 0.0,
                fan: parseInt(data.fan) || 0,
                il: parseInt(data.il) || 0,
                dom: parseInt(data.dom) || 0,
                surv: parseInt(data.surv) || 0,
                adapt_acc: parseInt(data.adapt_acc) || 0,
                cs: parseInt(data.cs) || 0
            }]);

        if (error) {
            console.error('[SUPABASE ERROR]:', error.message);
        }

        io.emit('new_telemetry', lastTelemetryPacket);
    }
});

// 1. API lấy dữ liệu biểu đồ
app.get('/api/chart-data', async (req, res) => {
    try {
        const { mode = 'recent', value = 30, unit = 'minute', from_time, to_time } = req.query;
        let queryGte = null;
        let queryLte = null;

        if (mode === 'range' || from_time || to_time) {
            if (from_time) queryGte = from_time;
            if (to_time) queryLte = to_time;
        } else {
            const valNum = parseInt(value) || 30;
            const nowVN = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
            let cutoffVN = new Date(nowVN.getTime());

            if (unit === 'minute') cutoffVN.setMinutes(cutoffVN.getMinutes() - valNum);
            else if (unit === 'hour') cutoffVN.setHours(cutoffVN.getHours() - valNum);
            else if (unit === 'day') cutoffVN.setDate(cutoffVN.getDate() - valNum);
            else if (unit === 'week') cutoffVN.setDate(cutoffVN.getDate() - (valNum * 7));
            else if (unit === 'month') cutoffVN.setMonth(cutoffVN.getMonth() - valNum);
            else if (unit === 'year') cutoffVN.setFullYear(cutoffVN.getFullYear() - valNum);

            const pad = (n) => String(n).padStart(2, '0');
            queryGte = `${cutoffVN.getFullYear()}-${pad(cutoffVN.getMonth() + 1)}-${pad(cutoffVN.getDate())} ${pad(cutoffVN.getHours())}:${pad(cutoffVN.getMinutes())}:${pad(cutoffVN.getSeconds())}`;
        }

        const CHUNK_SIZE = 1000;
        const MAX_CHUNKS = 10;
        const fetchPromises = [];

        for (let i = 0; i < MAX_CHUNKS; i++) {
            const from = i * CHUNK_SIZE;
            const to = from + CHUNK_SIZE - 1;
            let q = supabase
                .from('telemetry_logs')
                .select('T, S, pH, DO, created_at')
                .order('id', { ascending: false })
                .range(from, to);

            if (queryGte) q = q.gte('created_at', queryGte);
            if (queryLte) q = q.lte('created_at', queryLte);

            fetchPromises.push(q);
        }

        const results = await Promise.all(fetchPromises);
        let allData = [];

        for (const r of results) {
            if (r.data && r.data.length > 0) {
                allData = allData.concat(r.data);
                if (r.data.length < CHUNK_SIZE) break;
            } else {
                break;
            }
        }

        if (allData.length === 0) {
            const fallback = await supabase
                .from('telemetry_logs')
                .select('T, S, pH, DO, created_at')
                .order('id', { ascending: false })
                .limit(100);
            return res.json(fallback.data ? fallback.data.reverse() : []);
        }

        let sampled = allData;
        const maxPoints = 300;
        if (allData.length > maxPoints) {
            const step = Math.ceil(allData.length / maxPoints);
            sampled = allData.filter((_, idx) => idx % step === 0);
        }

        res.json(sampled.reverse());
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 2. API phân trang & tìm kiếm lịch sử
app.get('/api/logs-paged', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 1000, 1000);
        const from = (page - 1) * limit;
        const to = from + limit - 1;
        const { from_time, to_time } = req.query;

        let query = supabase
            .from('telemetry_logs')
            .select('*', { count: 'exact' });

        if (from_time) query = query.gte('created_at', from_time);
        if (to_time) query = query.lte('created_at', to_time);

        const { data, count, error } = await query
            .order('id', { ascending: false })
            .range(from, to);

        if (error) return res.status(500).json({ error: error.message });

        res.json({
            data: data || [],
            total: count || 0,
            page,
            limit,
            totalPages: Math.ceil((count || 0) / limit)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 3. API xuất file CSV
app.get('/api/export-csv', async (req, res) => {
    try {
        const { mode = 'all', limit = 1000, from_time, to_time } = req.query;
        const maxRows = mode === 'limit' ? parseInt(limit) || 1000 : 100000;
        
        let allData = [];
        const CHUNK_SIZE = 1000;
        let fetched = 0;

        while (fetched < maxRows) {
            const from = fetched;
            const to = Math.min(fetched + CHUNK_SIZE - 1, maxRows - 1);

            let query = supabase
                .from('telemetry_logs')
                .select('*')
                .order('id', { ascending: false })
                .range(from, to);

            if (mode === 'range' || from_time || to_time) {
                if (from_time) query = query.gte('created_at', from_time);
                if (to_time) query = query.lte('created_at', to_time);
            }

            const { data, error } = await query;
            if (error) break;
            if (!data || data.length === 0) break;

            allData = allData.concat(data);
            fetched += data.length;

            if (data.length < CHUNK_SIZE) break;
        }

        let csv = "ID,Thoi_Gian,Nhiet_Do_T,Do_Man_S,pH,DO,Do_Kiem_Alk,Btri,Fan,IL,DOM,Surv,Adapt,CS\n";
        allData.forEach(r => {
            const timeFormatted = formatSupabaseTime(r.created_at);
            csv += `${r.id},"${timeFormatted}",${r.T},${r.S},${r.pH},${r.DO},${r.alk},${r.btri},${r.fan},${r.il},${r.dom},${r.surv},${r.adapt_acc},${r.cs}\n`;
        });

        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="telemetry_logs_${Date.now()}.csv"`);
        res.status(200).send('\uFEFF' + csv);
    } catch (err) {
        res.status(500).send("Lỗi xuất file: " + err.message);
    }
});

// Xử lý Socket.io & Downlink
io.on('connection', (socket) => {
    // Gửi ngay trạng thái kết nối VÀ gói tin tức thời gần nhất để Web không phải chờ
    socket.emit('device_heartbeat', {
        lastDeviceTime: lastDeviceTime,
        isOnline: (Date.now() - lastDeviceTime < 25000),
        latestPacket: lastTelemetryPacket
    });

    socket.on('send_control', async (commandStr) => {
        console.log(`[DOWNLINK] Phat lenh: ${commandStr}`);
        mqttClient.publish(TOPIC_DOWNLINK, String(commandStr));

        await supabase
            .from('device_controls')
            .update({ last_command: commandStr, updated_at: new Date() })
            .eq('device_id', 'STM32_Tilapia_01');

        io.emit('control_status', `Đã phát lệnh: ${commandStr}`);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
