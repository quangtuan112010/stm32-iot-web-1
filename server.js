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

let lastDeviceTime = 0;
let lastTelemetryPacket = null;

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

function formatDurationSeconds(totalSeconds) {
    totalSeconds = Math.max(0, Math.floor(totalSeconds));
    if (totalSeconds < 60) return `${totalSeconds} giây`;
    const mins = Math.floor(totalSeconds / 60);
    const sec = totalSeconds % 60;
    if (mins < 60) return sec > 0 ? `${mins} phút ${sec} giây` : `${mins} phút`;
    const hours = Math.floor(mins / 60);
    const remMin = mins % 60;
    return remMin > 0 ? `${hours} giờ ${remMin} phút ${sec} giây` : `${hours} giờ ${sec} giây`;
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

// Nhận gói tin Uplink chứa trọn vẹn 47 trường dữ liệu từ STM32
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        lastDeviceTime = Date.now();
        let data = {};

        try {
            data = JSON.parse(rawStr);
        } catch (e) {
            console.error('[JSON ERROR] Khong the parse payload:', rawStr);
            return;
        }

        const insertPayload = {
            // 12 trường cốt lõi
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
            cs: parseInt(data.cs) || 0,

            // 16 trường chẩn đoán & dự báo mở rộng
            rate: parseFloat(data.rate) || 0.0,
            eta: parseFloat(data.eta) || 0.0,
            braw: parseFloat(data.braw) || 0.0,
            il8: parseInt(data.il8) || 0,
            il8cal: parseInt(data.il8cal) || 0,
            il8rdy: parseInt(data.il8rdy) || 0,
            phoff: parseFloat(data.phoff) || 0.0,
            slope: parseFloat(data.slope) || 0.0,
            iqrph: parseFloat(data.iqrph) || 0.0,
            iqrdo: parseFloat(data.iqrdo) || 0.0,
            tspr: parseFloat(data.tspr) || 0.0,
            fph: parseInt(data.fph) || 0,
            fec: parseInt(data.fec) || 0,
            fdo: parseInt(data.fdo) || 0,
            wcet: parseInt(data.wcet) || 0,
            hleft: parseInt(data.hleft) || 0,

            // 5 trường phần cứng & viễn thông
            csq: data.csq !== undefined ? parseInt(data.csq) : 99,
            rstr: parseInt(data.rstr) || 0,
            boot: parseInt(data.boot) || 0,
            up: parseInt(data.up) || 0,
            flfail: parseInt(data.flfail) || 0,

            // 14 trường AI PINN & NLMS Adapter & FreeRTOS mới
            dopred: parseFloat(data.dopred) || 0.0,
            dosat: parseFloat(data.dosat) || 0.0,
            aisig: parseFloat(data.aisig) || 0.0,
            aivalid: parseInt(data.aivalid) || 0,
            aistruct: parseInt(data.aistruct) || 0,
            aistep: parseInt(data.aistep) || 0,
            adwk: parseInt(data.adwk) || 0,
            adacc: parseInt(data.adacc) || 0,
            adrej: parseInt(data.adrej) || 0,
            admseb: parseFloat(data.admseb) || 0.0,
            admsea: parseFloat(data.admsea) || 0.0,
            adlast: parseInt(data.adlast) || 0,
            nvlog: parseInt(data.nvlog) || 0,
            stackmin: parseInt(data.stackmin) || 0
        };

        const { data: insertedRows, error } = await supabase
            .from('telemetry_logs')
            .insert([insertPayload])
            .select();

        let dbId = '--';
        let dbTimeFormatted = '';

        if (error) {
            console.error('\x1b[31m[SUPABASE INSERT ERROR]:\x1b[0m', error.message);
            console.error('Chi tiết lỗi:', error.details || error.hint);
        } else if (insertedRows && insertedRows.length > 0) {
            dbId = insertedRows[0].id;
            dbTimeFormatted = formatSupabaseTime(insertedRows[0].created_at);
        }

        lastTelemetryPacket = {
            db_id: dbId,
            db_time: dbTimeFormatted,
            device_timestamp: lastDeviceTime,
            data: data
        };

        io.emit('new_telemetry', lastTelemetryPacket);
    }
});

app.get('/api/dismissed-incidents', async (req, res) => {
    try {
        const { data, error } = await supabase.from('incident_dismissals').select('incident_id');
        if (error) return res.json([]);
        res.json(data.map(d => d.incident_id));
    } catch (err) {
        res.json([]);
    }
});

app.post('/api/dismiss-incident', async (req, res) => {
    try {
        const { incident_id } = req.body;
        if (!incident_id) return res.status(400).json({ error: 'Missing incident_id' });

        const { error } = await supabase
            .from('incident_dismissals')
            .upsert([{ incident_id }], { onConflict: 'incident_id' });

        if (error) return res.status(500).json({ error: error.message });
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/audit-incidents', async (req, res) => {
    try {
        const hours = parseInt(req.query.hours) || 72;
        const nowVN = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
        const cutoffVN = new Date(nowVN.getTime() - hours * 60 * 60 * 1000);

        const pad = (n) => String(n).padStart(2, '0');
        const cutoffStr = `${cutoffVN.getFullYear()}-${pad(cutoffVN.getMonth() + 1)}-${pad(cutoffVN.getDate())} ${pad(cutoffVN.getHours())}:${pad(cutoffVN.getMinutes())}:${pad(cutoffVN.getSeconds())}`;

        const [minRes, maxRes] = await Promise.all([
            supabase.from('telemetry_logs').select('id').gte('created_at', cutoffStr).order('id', { ascending: true }).limit(1),
            supabase.from('telemetry_logs').select('id').order('id', { ascending: false }).limit(1)
        ]);

        if (!minRes.data || minRes.data.length === 0 || !maxRes.data || maxRes.data.length === 0) {
            return res.json({ incidents: [], scanned_records: 0, time_window_hours: hours });
        }

        const minId = minRes.data[0].id;
        const maxId = maxRes.data[0].id;
        const totalRowsInRange = maxId - minId + 1;
        const CHUNK_SIZE = 1000;
        const neededChunks = Math.min(Math.ceil(totalRowsInRange / CHUNK_SIZE), 30);

        const fetchPromises = [];
        for (let i = 0; i < neededChunks; i++) {
            const fromId = minId + i * CHUNK_SIZE;
            const toId = Math.min(minId + (i + 1) * CHUNK_SIZE - 1, maxId);
            fetchPromises.push(
                supabase
                    .from('telemetry_logs')
                    .select('id, created_at, il, dom, fan, surv, btri, cs, adapt_acc, il8')
                    .gte('id', fromId)
                    .lte('id', toId)
                    .order('id', { ascending: true })
            );
        }

        const results = await Promise.all(fetchPromises);
        let logs = [];
        for (const r of results) {
            if (r.data && r.data.length > 0) logs = logs.concat(r.data);
        }

        if (logs.length === 0) {
            return res.json({ incidents: [], scanned_records: 0, time_window_hours: hours });
        }

        const incidents = [];

        for (let i = 1; i < logs.length; i++) {
            const tPrev = new Date(logs[i - 1].created_at).getTime();
            const tCurr = new Date(logs[i].created_at).getTime();
            const gapSec = (tCurr - tPrev) / 1000;

            if (gapSec >= 35) {
                incidents.push({
                    id: `outage_${logs[i - 1].id}_${logs[i].id}`,
                    type: 'OFFLINE_GAP',
                    severity: 'critical',
                    category: 'Mất kết nối / Nghẽn dữ liệu 4G',
                    title: `Gián đoạn truyền tin IoT (${formatDurationSeconds(gapSec)})`,
                    start_time: formatSupabaseTime(logs[i - 1].created_at),
                    end_time: formatSupabaseTime(logs[i].created_at),
                    duration: formatDurationSeconds(gapSec),
                    start_raw: logs[i - 1].created_at,
                    details: `Thiết bị không gửi dữ liệu từ ${formatSupabaseTime(logs[i - 1].created_at)} đến ${formatSupabaseTime(logs[i].created_at)}.`
                });
            }
        }

        function analyzeContinuousRun(keyName, conditionFn, createIncidentFn) {
            let active = null;
            for (let i = 0; i < logs.length; i++) {
                const row = logs[i];
                if (conditionFn(row)) {
                    if (!active) active = { start_row: row, end_row: row, rows: [row] };
                    else { active.end_row = row; active.rows.push(row); }
                } else {
                    if (active) { incidents.push(createIncidentFn(active)); active = null; }
                }
            }
            if (active) incidents.push(createIncidentFn(active));
        }

        analyzeContinuousRun('il', (r) => parseInt(r.il) > 0, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            let unionMask = 0;
            act.rows.forEach(r => unionMask |= parseInt(r.il));

            let bitDescs = [];
            if (unionMask & 0x01) bitDescs.push("NH3 vượt ngưỡng");
            if (unionMask & 0x02) bitDescs.push("H2S vượt ngưỡng");
            if (unionMask & 0x04) bitDescs.push("Oxy thấp");
            if (unionMask & 0x08) bitDescs.push("Oxy NGUY CẤP (< 2mg/L)");
            if (unionMask & 0x10) bitDescs.push("Kiềm sụt giảm");
            if (unionMask & 0x20) bitDescs.push("pH nguy hiểm");
            if (unionMask & 0x40) bitDescs.push("Lỗi cảm biến");

            return {
                id: `il_${act.start_row.id}_${act.end_row.id}`,
                type: 'INTERLOCK',
                severity: (unionMask & 0x28) ? 'critical' : 'warning',
                category: 'Khóa liên động sự cố (il)',
                title: `Kích hoạt cờ liên động (0x${unionMask.toString(16).toUpperCase()})`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Các sự cố phát hiện: ${bitDescs.join("; ")}.`
            };
        });

        analyzeContinuousRun('il8', (r) => parseInt(r.il8) > 0, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            return {
                id: `il8_${act.start_row.id}_${act.end_row.id}`,
                type: 'PROBE_DIRT',
                severity: 'warning',
                category: 'Cảnh báo bám bẩn đầu dò (il8)',
                title: `Đầu dò cảm biến bị bám bẩn / trôi điện cực (${formatDurationSeconds(durSec)})`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Thuật toán phát hiện bám bẩn kích hoạt cờ il8. Kỹ thuật viên cần kiểm tra và vệ sinh đầu dò.`
            };
        });

        analyzeContinuousRun('fan', (r) => parseInt(r.fan) === 1, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            return {
                id: `fan_${act.start_row.id}_${act.end_row.id}`,
                type: 'FAN_RUN',
                severity: 'warning',
                category: 'Quạt sục khí khẩn cấp (fan)',
                title: `Quạt oxy tự động BẬT liên tục (${formatDurationSeconds(durSec)})`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Relay quạt sục khí đã đóng để cấp cứu oxy.`
            };
        });

        analyzeContinuousRun('surv', (r) => parseInt(r.surv) === 1, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            return {
                id: `surv_${act.start_row.id}_${act.end_row.id}`,
                type: 'SURVIVAL',
                severity: 'critical',
                category: 'Chế độ sinh tồn vi điều khiển (surv)',
                title: `MPU kích hoạt chế độ Sinh tồn (${formatDurationSeconds(durSec)})`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Cảm biến hỏng hoặc ngoài biên, firmware cưỡng bức bật quạt khẩn cấp.`
            };
        });

        analyzeContinuousRun('btri', (r) => parseFloat(r.btri) >= 50.0, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            const maxBtri = Math.max(...act.rows.map(r => parseFloat(r.btri) || 0));
            return {
                id: `btri_${act.start_row.id}_${act.end_row.id}`,
                type: 'BTRI_HIGH',
                severity: maxBtri >= 75 ? 'critical' : 'warning',
                category: 'Rủi ro độc chất sinh hóa (btri)',
                title: `Rủi ro sinh hóa vượt ngưỡng - Đỉnh: ${maxBtri.toFixed(1)} điểm`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Chỉ số rủi ro độc chất sinh hóa BTRI đạt đỉnh ${maxBtri.toFixed(1)} điểm.`
            };
        });

        analyzeContinuousRun('dom', (r) => parseInt(r.dom) > 0, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            let domMask = 0;
            act.rows.forEach(r => domMask |= parseInt(r.dom));
            return {
                id: `dom_${act.start_row.id}_${act.end_row.id}`,
                type: 'DOMAIN_GUARD',
                severity: 'warning',
                category: 'Miền sinh học cá rô phi (dom)',
                title: `Vi phạm giới hạn sinh thái (0x${domMask.toString(16).toUpperCase()})`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Thông số nước nằm ngoài dải sinh thái tối ưu.`
            };
        });

        analyzeContinuousRun('cs1', (r) => parseInt(r.cs) === 1, (act) => {
            const t1 = new Date(act.start_row.created_at).getTime();
            const t2 = new Date(act.end_row.created_at).getTime();
            const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
            return {
                id: `cs1_${act.start_row.id}_${act.end_row.id}`,
                type: 'COLD_START_PENDING',
                severity: 'warning',
                category: 'Chu trình khởi động lạnh (cs)',
                title: `Hệ thống chờ nạp Độ Kiềm neo (CS_ANCHOR_PENDING)`,
                start_time: formatSupabaseTime(act.start_row.created_at),
                end_time: formatSupabaseTime(act.end_row.created_at),
                duration: formatDurationSeconds(durSec),
                start_raw: act.start_row.created_at,
                details: `Hệ thống kết thúc 72h ổn định ban đầu và chờ nạp độ kiềm thực tế.`
            };
        });

        incidents.sort((a, b) => new Date(b.start_raw).getTime() - new Date(a.start_raw).getTime());

        res.json({
            incidents: incidents,
            scanned_records: logs.length,
            time_window_hours: hours
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

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

        let minQ = supabase.from('telemetry_logs').select('id').order('id', { ascending: true }).limit(1);
        let maxQ = supabase.from('telemetry_logs').select('id').order('id', { ascending: false }).limit(1);

        if (queryGte) { minQ = minQ.gte('created_at', queryGte); maxQ = maxQ.gte('created_at', queryGte); }
        if (queryLte) { minQ = minQ.lte('created_at', queryLte); maxQ = maxQ.lte('created_at', queryLte); }

        const [minRes, maxRes] = await Promise.all([minQ, maxQ]);

        if (!minRes.data || minRes.data.length === 0 || !maxRes.data || maxRes.data.length === 0) {
            const fallback = await supabase
                .from('telemetry_logs')
                .select('T, S, pH, DO, created_at')
                .order('id', { ascending: false })
                .limit(100);
            return res.json(fallback.data ? fallback.data.reverse() : []);
        }

        const minId = minRes.data[0].id;
        const maxId = maxRes.data[0].id;
        const idSpan = maxId - minId;

        if (idSpan <= 1000) {
            let q = supabase
                .from('telemetry_logs')
                .select('T, S, pH, DO, created_at')
                .gte('id', minId)
                .lte('id', maxId)
                .order('id', { ascending: true })
                .limit(1000);
            const { data } = await q;
            return res.json(data || []);
        }

        const targetPoints = 300;
        const step = idSpan / targetPoints;
        const targetIds = [];
        for (let i = 0; i < targetPoints; i++) {
            targetIds.push(Math.round(minId + i * step));
        }
        if (!targetIds.includes(maxId)) targetIds.push(maxId);

        const { data, error } = await supabase
            .from('telemetry_logs')
            .select('T, S, pH, DO, created_at')
            .in('id', targetIds)
            .order('id', { ascending: true });

        if (error || !data || data.length === 0) {
            const fallback = await supabase
                .from('telemetry_logs')
                .select('T, S, pH, DO, created_at')
                .order('id', { ascending: false })
                .limit(300);
            return res.json(fallback.data ? fallback.data.reverse() : []);
        }

        res.json(data);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/logs-paged', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 1000, 1000);
        const from = (page - 1) * limit;
        const to = from + limit - 1;
        const { from_time, to_time } = req.query;

        let query = supabase.from('telemetry_logs').select('*', { count: 'exact' });

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

// Xuất file CSV 49 cột chuẩn xác
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

        let csv = "ID,Thoi_Gian,Nhiet_Do_T,Do_Man_S,pH,DO,Do_Kiem_Alk,Btri,Fan,IL,DOM,Surv,Adapt,CS,Rate,ETA_Min,BTRI_Raw,IL8_Probe,IL8_Calib,IL8_Ready,pH_Offset,pH_Slope,IQR_pH,IQR_DO,T_Spread,Fail_pH,Fail_EC,Fail_DO,WCET,Hours_Left,CSQ_Signal,Reset_Reason,Boot_Count,Uptime_Sec,Flash_Fail,DO_Pred,DO_Sat,AI_Sigma,AI_Valid,AI_Struct,AI_Step,Adapt_Week,Adapt_Acc,Adapt_Rej,MSE_Before,MSE_After,Adapt_Last,NV_Log,Stack_Min_Pct\n";
        allData.forEach(r => {
            const timeFormatted = formatSupabaseTime(r.created_at);
            csv += `${r.id},"${timeFormatted}",${r.T},${r.S},${r.pH},${r.DO},${r.alk},${r.btri},${r.fan},${r.il},${r.dom},${r.surv},${r.adapt_acc},${r.cs},${r.rate || 0},${r.eta || 0},${r.braw || 0},${r.il8 || 0},${r.il8cal || 0},${r.il8rdy || 0},${r.phoff || 0},${r.slope || 0},${r.iqrph || 0},${r.iqrdo || 0},${r.tspr || 0},${r.fph || 0},${r.fec || 0},${r.fdo || 0},${r.wcet || 0},${r.hleft || 0},${r.csq ?? 99},${r.rstr || 0},${r.boot || 0},${r.up || 0},${r.flfail || 0},${r.dopred || 0},${r.dosat || 0},${r.aisig || 0},${r.aivalid || 0},${r.aistruct || 0},${r.aistep || 0},${r.adwk || 0},${r.adacc || 0},${r.adrej || 0},${r.admseb || 0},${r.admsea || 0},${r.adlast || 0},${r.nvlog || 0},${r.stackmin || 0}\n`;
        });

        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="telemetry_logs_49fields_${Date.now()}.csv"`);
        res.status(200).send('\uFEFF' + csv);
    } catch (err) {
        res.status(500).send("Lỗi xuất file: " + err.message);
    }
});

app.get(['/api', '/api/'], (req, res) => res.redirect('/'));
app.use((req, res, next) => {
    if (req.method === 'GET' && !req.path.startsWith('/socket.io')) {
        return res.redirect('/');
    }
    res.status(404).send('Not Found');
});

io.on('connection', (socket) => {
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

        if (commandStr === 'RESET') {
            io.emit('control_status', `Đã gửi lệnh RESET! STM32 đang khởi động lại (giữ Flash). Sẽ kết nối lại sau ~8-12 giây.`);
        } else {
            io.emit('control_status', `Đã phát lệnh: ${commandStr}`);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
