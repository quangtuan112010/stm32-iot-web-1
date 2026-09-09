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

// TỌA ĐỘ DUY NHẤT: XÃ XUÂN ĐỊNH, HUYỆN XUÂN LỘC, ĐỒNG NAI
const XUAN_DINH_LAT = 10.91;
const XUAN_DINH_LON = 107.21;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

let lastDeviceTime = 0;
let lastTelemetryPacket = null;
let currentRainStatus = { 
    isRainingNow: false, 
    hasRainedToday: false, 
    todayRainCount: 0,
    currentMm: 0,
    text: 'Đang kiểm tra thời tiết thực tế tại Xuân Định...',
    events: []
};

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

function formatMinutesToHours(totalMin) {
    if (totalMin <= 0) return 'Đợt đầu tiên trong ngày';
    const hrs = Math.floor(totalMin / 60);
    const m = totalMin % 60;
    if (hrs === 0) return `Cách đợt trước ${m} phút`;
    return m > 0 ? `Cách đợt trước ${hrs} giờ ${m} phút` : `Cách đợt trước ${hrs} giờ`;
}

// ================= QUAN TRẮC MƯA XÃ XUÂN ĐỊNH (LƯU LỊCH VĨNH VIỄN BẰNG UPSERT) =================
async function syncRainData() {
    try {
        const now = Date.now();
        const url = `https://api.open-meteo.com/v1/forecast?latitude=${XUAN_DINH_LAT}&longitude=${XUAN_DINH_LON}&current=precipitation,rain&hourly=precipitation,rain&past_days=7&forecast_days=1&timezone=Asia%2FHo_Chi_Minh`;
        
        const res = await fetch(url, { headers: { 'User-Agent': 'STM32-Tilapia-IoT/1.0' } });
        const data = await res.json();

        if (data.error || !data.hourly || !data.hourly.time) return;

        const currentP = parseFloat(data.current?.precipitation) || 0.0;
        const isRainingNow = currentP >= 0.5;

        const times = data.hourly.time;
        const precips = data.hourly.precipitation;

        let inRain = false;
        let startIdx = 0;
        let peakVal = 0.0;
        let totalVal = 0.0;
        const allParsedEvents = [];

        for (let i = 0; i < times.length; i++) {
            const itemTimestamp = new Date(times[i] + ":00+07:00").getTime();

            // Chặn dữ liệu dự báo tương lai
            if (itemTimestamp > now) {
                if (inRain) {
                    const actualEndIdx = i - 1;
                    const startIso = times[startIdx];
                    const endIso = times[actualEndIdx];
                    const durMin = Math.max(60, (actualEndIdx - startIdx + 1) * 60);
                    const [y, m, d] = startIso.split('T')[0].split('-');

                    if (totalVal >= 0.3) {
                        allParsedEvents.push({
                            rain_date: `${d}/${m}/${y}`,
                            start_time: times[startIdx].split('T')[1],
                            end_time: 'Đang mưa',
                            start_full: `${times[startIdx].split('T')[1]} ${d}/${m}/${y}`,
                            end_full: `Đang mưa ${d}/${m}/${y}`,
                            start_timestamp: new Date(startIso + ":00+07:00").getTime(),
                            end_timestamp: new Date(endIso + ":00+07:00").getTime(),
                            duration_min: durMin,
                            peak_mm: parseFloat(peakVal.toFixed(2)),
                            total_mm: parseFloat(totalVal.toFixed(2))
                        });
                    }
                    inRain = false;
                }
                break;
            }

            const p = parseFloat(precips[i]) || 0.0;
            const isRaining = p >= 0.5;

            // Cắt đợt khi chuyển sang ngày mới (23:00)
            const curDateStr = times[i].split('T')[0];
            const prevDateStr = i > 0 ? times[i - 1].split('T')[0] : curDateStr;
            const isNewDay = (curDateStr !== prevDateStr);

            if (isNewDay && inRain) {
                const actualEndIdx = i - 1;
                const startIso = times[startIdx];
                const endIso = times[actualEndIdx];
                const durMin = Math.max(60, (actualEndIdx - startIdx + 1) * 60);
                const [y, m, d] = startIso.split('T')[0].split('-');

                if (totalVal >= 0.3) {
                    allParsedEvents.push({
                        rain_date: `${d}/${m}/${y}`,
                        start_time: times[startIdx].split('T')[1],
                        end_time: '23:00',
                        start_full: `${times[startIdx].split('T')[1]} ${d}/${m}/${y}`,
                        end_full: `23:00 ${d}/${m}/${y}`,
                        start_timestamp: new Date(startIso + ":00+07:00").getTime(),
                        end_timestamp: new Date(endIso + ":00+07:00").getTime(),
                        duration_min: durMin,
                        peak_mm: parseFloat(peakVal.toFixed(2)),
                        total_mm: parseFloat(totalVal.toFixed(2))
                    });
                }
                inRain = false;
            }

            if (isRaining) {
                if (!inRain) {
                    inRain = true;
                    startIdx = i;
                    peakVal = p;
                    totalVal = p;
                } else {
                    if (p > peakVal) peakVal = p;
                    totalVal += p;
                }
            } else {
                if (inRain) {
                    inRain = false;
                    const actualEndIdx = i;
                    const startIso = times[startIdx];
                    const endIso = times[actualEndIdx];
                    const durMin = Math.max(60, (actualEndIdx - startIdx) * 60);
                    const [y, m, d] = startIso.split('T')[0].split('-');

                    if (totalVal >= 0.3) {
                        allParsedEvents.push({
                            rain_date: `${d}/${m}/${y}`,
                            start_time: times[startIdx].split('T')[1],
                            end_time: times[actualEndIdx].split('T')[1],
                            start_full: `${times[startIdx].split('T')[1]} ${d}/${m}/${y}`,
                            end_full: `${times[actualEndIdx].split('T')[1]} ${d}/${m}/${y}`,
                            start_timestamp: new Date(startIso + ":00+07:00").getTime(),
                            end_timestamp: new Date(endIso + ":00+07:00").getTime(),
                            duration_min: durMin,
                            peak_mm: parseFloat(peakVal.toFixed(2)),
                            total_mm: parseFloat(totalVal.toFixed(2))
                        });
                    }
                }
            }
        }

        // Gom nhóm theo ngày và tính khoảng cách giữa các đợt
        const eventsByDate = {};
        allParsedEvents.forEach(ev => {
            if (!eventsByDate[ev.rain_date]) eventsByDate[ev.rain_date] = [];
            eventsByDate[ev.rain_date].push(ev);
        });

        const rowsToSave = [];
        Object.keys(eventsByDate).forEach(dateKey => {
            const dayList = eventsByDate[dateKey];
            dayList.sort((a, b) => a.start_timestamp - b.start_timestamp);

            for (let idx = 0; idx < dayList.length; idx++) {
                const cur = dayList[idx];
                cur.episode_no = idx + 1;

                if (idx === 0) {
                    cur.gap_desc = 'Đợt đầu tiên trong ngày';
                } else {
                    const prev = dayList[idx - 1];
                    const diffMs = cur.start_timestamp - prev.end_timestamp;
                    const diffMin = Math.max(0, Math.floor(diffMs / (60 * 1000)));
                    cur.gap_desc = formatMinutesToHours(diffMin);
                }

                // Khóa event_key độc nhất để upsert không xóa mất lịch sử ngày cũ
                const eventKey = `${cur.rain_date.replace(/\//g, '-')}_ep_${cur.episode_no}`;

                rowsToSave.push({
                    event_key: eventKey,
                    rain_date: cur.rain_date,
                    episode_no: cur.episode_no,
                    start_time: cur.start_full,
                    end_time: cur.end_full,
                    duration_min: cur.duration_min,
                    peak_mm: cur.peak_mm,
                    total_mm: cur.total_mm,
                    gap_desc: cur.gap_desc
                });
            }
        });

        // Ghi vào Supabase bằng UPSERT (bảo tồn toàn bộ lịch sử)
        if (rowsToSave.length > 0) {
            await supabase.from('rain_history').upsert(rowsToSave, { onConflict: 'event_key' });
        }

        const nowVN = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
        const pad = (n) => String(n).padStart(2, '0');
        const todayStr = `${pad(nowVN.getDate())}/${pad(nowVN.getMonth() + 1)}/${nowVN.getFullYear()}`;
        const todayEvs = eventsByDate[todayStr] || [];

        let textMsg = '';
        if (isRainingNow) {
            textMsg = `🌧️ HIỆN TẠI ĐANG CÓ MƯA THỰC TẾ (Lượng mưa: ${currentP.toFixed(1)} mm)`;
        } else if (todayEvs.length > 0) {
            const last = todayEvs[todayEvs.length - 1];
            textMsg = `☀️ Hiện tại tạnh ráo. Hôm nay đã có ${todayEvs.length} đợt mưa (gần nhất: ${last.start_time.split(' ')[0]} - ${last.end_time.split(' ')[0]})`;
        } else {
            textMsg = `☀️ Hôm nay chưa có mưa tại xã Xuân Định.`;
        }

        currentRainStatus = {
            isRainingNow: isRainingNow,
            hasRainedToday: todayEvs.length > 0 || isRainingNow,
            todayRainCount: todayEvs.length,
            currentMm: currentP,
            text: textMsg,
            events: todayEvs
        };

        io.emit('rain_status_update', currentRainStatus);
    } catch (err) {
        console.error('[RAIN ERROR]:', err.message);
    }
}

syncRainData();
setInterval(syncRainData, 10 * 60 * 1000);

app.get('/api/rain-history', async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('rain_history')
            .select('*')
            .order('id', { ascending: true });
        if (error) return res.status(500).json({ error: error.message });
        res.json({ current: currentRainStatus, history: data || [] });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ================= KẾT NỐI MQTT HIVEMQ CLOUD =================
const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

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

            csq: data.csq !== undefined ? parseInt(data.csq) : 99,
            rstr: parseInt(data.rstr) || 0,
            boot: parseInt(data.boot) || 0,
            up: parseInt(data.up) || 0,
            flfail: parseInt(data.flfail) || 0,

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
            console.error('[SUPABASE INSERT ERROR]:', error.message);
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

// ================= API RÀ SOÁT 72H SỰ CỐ: THUẬT TOÁN TÁCH ĐỢT THEO GIÁ TRỊ NGUYÊN BẢN =================
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

        // 1. Gián đoạn kết nối / Nghẽn dữ liệu 4G
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
                    details: `Thiết bị không gửi dữ liệu từ ${formatSupabaseTime(logs[i - 1].created_at)} đến ${formatSupabaseTime(logs[i].created_at)}. Nguyên nhân có thể do mất nguồn thiết bị hoặc mất sóng 4G.`
                });
            }
        }

        // THUẬT TOÁN EXACT-VALUE STATE-RUN SEGMENTATION (TÁCH ĐỢT THEO GIÁ TRỊ)
        function analyzeExactValueRuns(keyFn, validFn, createIncidentFn) {
            let active = null;
            for (let i = 0; i < logs.length; i++) {
                const row = logs[i];
                const isValid = validFn(row);
                const keyVal = isValid ? keyFn(row) : null;

                if (isValid) {
                    if (!active) {
                        active = { val: keyVal, start_row: row, end_row: row, rows: [row] };
                    } else if (active.val === keyVal) {
                        active.end_row = row;
                        active.rows.push(row);
                    } else {
                        // Giá trị thay đổi -> Chốt kết thúc đợt trước và tạo ngay đợt mới!
                        incidents.push(createIncidentFn(active));
                        active = { val: keyVal, start_row: row, end_row: row, rows: [row] };
                    }
                } else {
                    if (active) {
                        incidents.push(createIncidentFn(active));
                        active = null;
                    }
                }
            }
            if (active) incidents.push(createIncidentFn(active));
        }

        // 2. Cờ liên động il (Tách riêng từng giá trị il chính xác)
        analyzeExactValueRuns(
            (r) => parseInt(r.il) || 0,
            (r) => (parseInt(r.il) || 0) > 0,
            (act) => {
                const t1 = new Date(act.start_row.created_at).getTime();
                const t2 = new Date(act.end_row.created_at).getTime();
                const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
                const mask = act.val;

                let bitDescs = [];
                if (mask & 0x01) bitDescs.push("Khí độc NH3 vượt ngưỡng (R_NH3 ≥ 1.48)");
                if (mask & 0x02) bitDescs.push("Khí độc H2S vượt ngưỡng (R_H2S ≥ 1.05)");
                if (mask & 0x04) bitDescs.push("Oxy tương đối thấp (R_DO ≥ 1.44)");
                if (mask & 0x08) bitDescs.push("Oxy NGUY CẤP (DO < 2.0 mg/L) -> CƯỠNG BỨC QUẠT");
                if (mask & 0x10) bitDescs.push("Độ kiềm sụt giảm (< 50 mg/L) -> Mất hệ đệm");
                if (mask & 0x20) bitDescs.push("pH nguy hiểm (< 6.0 hoặc > 9.5) -> CƯỠNG BỨC QUẠT");
                if (mask & 0x40) bitDescs.push("Lỗi cảm biến / Mất gói liên tiếp");

                return {
                    id: `il_${mask}_${act.start_row.id}_${act.end_row.id}`,
                    type: 'INTERLOCK',
                    severity: (mask & 0x28) ? 'critical' : 'warning',
                    category: 'Khóa liên động sự cố (il)',
                    title: `Cờ liên động: 0x${mask.toString(16).toUpperCase()} (${bitDescs.length} lỗi đồng thời)`,
                    start_time: formatSupabaseTime(act.start_row.created_at),
                    end_time: formatSupabaseTime(act.end_row.created_at),
                    duration: formatDurationSeconds(durSec),
                    start_raw: act.start_row.created_at,
                    details: `Chi tiết các lỗi trong đợt này: ${bitDescs.join("; ")}.`
                };
            }
        );

        // 3. Cờ vi phạm sinh thái dom (Tách riêng từng giá trị dom)
        analyzeExactValueRuns(
            (r) => parseInt(r.dom) || 0,
            (r) => (parseInt(r.dom) || 0) > 0,
            (act) => {
                const t1 = new Date(act.start_row.created_at).getTime();
                const t2 = new Date(act.end_row.created_at).getTime();
                const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
                const mask = act.val;

                let domDescs = [];
                if (mask & 0x01) domDescs.push("Độ mặn ngoài dải (S > 5.0‰)");
                if (mask & 0x02) domDescs.push("Nhiệt độ ngoài dải (< 20°C hoặc > 35°C)");
                if (mask & 0x04) domDescs.push("pH ngoài dải sinh thái (< 6.5 hoặc > 9.5)");
                if (mask & 0x08) domDescs.push("Cảm biến trả về NaN hoặc đứt dây tín hiệu");

                return {
                    id: `dom_${mask}_${act.start_row.id}_${act.end_row.id}`,
                    type: 'DOMAIN_GUARD',
                    severity: 'warning',
                    category: 'Miền sinh học cá rô phi (dom)',
                    title: `Vi phạm giới hạn sinh thái (0x${mask.toString(16).toUpperCase()})`,
                    start_time: formatSupabaseTime(act.start_row.created_at),
                    end_time: formatSupabaseTime(act.end_row.created_at),
                    duration: formatDurationSeconds(durSec),
                    start_raw: act.start_row.created_at,
                    details: `Các vi phạm trong đợt này: ${domDescs.join("; ")}.`
                };
            }
        );

        // 4. Rủi ro sinh hóa BTRI (Tách riêng mức Rủi ro Cao và Nguy kịch)
        function getBtriLevel(r) {
            const b = parseFloat(r.btri) || 0.0;
            if (b >= 75.0) return 'CRITICAL';
            if (b >= 50.0) return 'HIGH';
            return 'NORMAL';
        }

        analyzeExactValueRuns(
            getBtriLevel,
            (r) => getBtriLevel(r) !== 'NORMAL',
            (act) => {
                const t1 = new Date(act.start_row.created_at).getTime();
                const t2 = new Date(act.end_row.created_at).getTime();
                const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
                const maxBtri = Math.max(...act.rows.map(r => parseFloat(r.btri) || 0));
                const isCrit = act.val === 'CRITICAL';

                return {
                    id: `btri_${act.val}_${act.start_row.id}_${act.end_row.id}`,
                    type: 'BTRI_HIGH',
                    severity: isCrit ? 'critical' : 'warning',
                    category: 'Rủi ro độc chất sinh hóa (btri)',
                    title: isCrit 
                        ? `Rủi ro Nguy kịch (BTRI ≥ 75) - Đỉnh: ${maxBtri.toFixed(1)} điểm`
                        : `Rủi ro Cao (50 ≤ BTRI < 75) - Đỉnh: ${maxBtri.toFixed(1)} điểm`,
                    start_time: formatSupabaseTime(act.start_row.created_at),
                    end_time: formatSupabaseTime(act.end_row.created_at),
                    duration: formatDurationSeconds(durSec),
                    start_raw: act.start_row.created_at,
                    details: `Chỉ số BTRI duy trì mức ${isCrit ? 'NGUY KỊCH' : 'CAO'} (Đạt đỉnh ${maxBtri.toFixed(1)} điểm) trong khoảng thời gian này.`
                };
            }
        );

        // 5. Cờ bám bẩn đầu dò IL8 (Tách theo giá trị il8)
        analyzeExactValueRuns(
            (r) => parseInt(r.il8) || 0,
            (r) => (parseInt(r.il8) || 0) > 0,
            (act) => {
                const t1 = new Date(act.start_row.created_at).getTime();
                const t2 = new Date(act.end_row.created_at).getTime();
                const durSec = Math.max(10, Math.floor((t2 - t1) / 1000) + 10);
                const mask = act.val;
                return {
                    id: `il8_${mask}_${act.start_row.id}_${act.end_row.id}`,
                    type: 'PROBE_DIRT',
                    severity: 'warning',
                    category: 'Cảnh báo bám bẩn đầu dò (il8)',
                    title: `Đầu dò cảm biến bị bám bẩn (0x${mask.toString(16).toUpperCase()})`,
                    start_time: formatSupabaseTime(act.start_row.created_at),
                    end_time: formatSupabaseTime(act.end_row.created_at),
                    duration: formatDurationSeconds(durSec),
                    start_raw: act.start_row.created_at,
                    details: `Thuật toán phát hiện bám bẩn kích hoạt cờ il8 = 0x${mask.toString(16).toUpperCase()} liên tục trong ${formatDurationSeconds(durSec)}. Cần vệ sinh đầu dò.`
                };
            }
        );

        // 6. Quạt sục khí khẩn cấp (fan == 1)
        analyzeExactValueRuns(
            () => 1,
            (r) => parseInt(r.fan) === 1,
            (act) => {
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
                    details: `Relay quạt sục khí đã đóng và vận hành trong ${formatDurationSeconds(durSec)} để cấp cứu oxy.`
                };
            }
        );

        // 7. Chế độ sinh tồn vi điều khiển (surv == 1)
        analyzeExactValueRuns(
            () => 1,
            (r) => parseInt(r.surv) === 1,
            (act) => {
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
                    details: `Cảm biến hỏng hoặc ngoài biên, firmware STM32 cưỡng bức bật quạt khẩn cấp.`
                };
            }
        );

        // 8. Chờ nạp kiềm khởi động lạnh (cs == 1)
        analyzeExactValueRuns(
            () => 1,
            (r) => parseInt(r.cs) === 1,
            (act) => {
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
                    details: `Hệ thống kết thúc 72h ổn định ban đầu và đang chờ kỹ thuật viên nạp giá trị độ kiềm thực tế.`
                };
            }
        );

        // 9. Sự kiện thích nghi PINN (adapt_acc == 1)
        for (let i = 0; i < logs.length; i++) {
            if (parseInt(logs[i].adapt_acc) === 1 && (i === 0 || parseInt(logs[i - 1].adapt_acc) === 0)) {
                incidents.push({
                    id: `adapt_${logs[i].id}`,
                    type: 'PINN_ADAPT',
                    severity: 'info',
                    category: 'Học máy thích nghi PINN (adapt)',
                    title: `Mạng PINN nạp thành công bộ trọng số thích nghi mới`,
                    start_time: formatSupabaseTime(logs[i].created_at),
                    end_time: formatSupabaseTime(logs[i].created_at),
                    duration: 'Sự kiện tức thời',
                    start_raw: logs[i].created_at,
                    details: `Mạng PINN trên STM32 đã hoàn thành chu kỳ học và cập nhật trọng số thích nghi mới vào Flash.`
                });
            }
        }

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

// API Biểu đồ Full-Span ID Sampling
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

// API Phân trang xem Database
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

// API Xuất CSV 49 cột
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

    socket.emit('rain_status_update', currentRainStatus);

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
