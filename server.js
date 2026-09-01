const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Cấu hình Supabase & MQTT từ biến môi trường
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

// Kết nối MQTT Broker qua TLS (Port 8883)
const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

// Nhận gói tin JSON Uplink từ STM32 / Arduino
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const timeVN = getVietnamTimeString();
        let data = {};

        try {
            data = JSON.parse(rawStr);
        } catch (e) {
            console.error('[JSON ERROR]: Khong the parse payload:', rawStr);
            return;
        }

        console.log(`[MQTT] [${timeVN}] Nhan du lieu:`, data);

        // Lưu vào Supabase đúng chuẩn 12 trường
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

        io.emit('new_telemetry', {
            time_vn: timeVN,
            data: data
        });
    }
});

// 1. API lấy dữ liệu biểu đồ (Vượt qua giới hạn 1000 dòng của Supabase để lấy trọn vẹn 12h, 1 ngày, 1 tuần)
app.get('/api/chart-data', async (req, res) => {
    try {
        const { value = 30, unit = 'minute' } = req.query;
        const valNum = parseInt(value) || 30;

        const nowVN = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
        let cutoffVN = new Date(nowVN.getTime());

        if (unit === 'minute') cutoffVN.setMinutes(cutoffVN.getMinutes() - valNum);
        else if (unit === 'hour') cutoffVN.setHours(cutoffVN.getHours() - valNum);
        else if (unit === 'day') cutoffVN.setDate(cutoffVN.getDate() - valNum);
        else if (unit === 'week') cutoffVN.setDate(cutoffVN.getDate() - (valNum * 7));
        else if (unit === 'month') cutoffVN.setMonth(cutoffVN.getMonth() - valNum);

        const pad = (n) => String(n).padStart(2, '0');
        const cutoffStr = `${cutoffVN.getFullYear()}-${pad(cutoffVN.getMonth() + 1)}-${pad(cutoffVN.getDate())} ${pad(cutoffVN.getHours())}:${pad(cutoffVN.getMinutes())}:${pad(cutoffVN.getSeconds())}`;

        // Đọc song song nhiều phân vùng (mỗi phân vùng 1000 dòng) để lấy tới 10.000 dòng
        const CHUNK_SIZE = 1000;
        const MAX_CHUNKS = 10; // Tối đa 10.000 bản ghi (~28 tiếng)
        const fetchPromises = [];

        for (let i = 0; i < MAX_CHUNKS; i++) {
            const from = i * CHUNK_SIZE;
            const to = from + CHUNK_SIZE - 1;
            fetchPromises.push(
                supabase
                    .from('telemetry_logs')
                    .select('T, S, pH, DO, created_at')
                    .gte('created_at', cutoffStr)
                    .order('id', { ascending: false })
                    .range(from, to)
            );
        }

        const results = await Promise.all(fetchPromises);
        let allData = [];

        for (const r of results) {
            if (r.data && r.data.length > 0) {
                allData = allData.concat(r.data);
                if (r.data.length < CHUNK_SIZE) break; // Đã lấy hết dữ liệu trong khoảng thời gian
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

        // Lấy mẫu đều khoảng 300 điểm trải dài trên toàn bộ 12 giờ
        let sampled = allData;
        const maxPoints = 300;
        if (allData.length > maxPoints) {
            const step = Math.ceil(allData.length / maxPoints);
            sampled = allData.filter((_, idx) => idx % step === 0);
        }

        // Đảo ngược lại theo thứ tự thời gian từ quá khứ đến hiện tại
        res.json(sampled.reverse());
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 2. API phân trang xem Database hỗ trợ đến 1000 dòng mỗi trang
app.get('/api/logs-paged', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 1000, 1000);
        const from = (page - 1) * limit;
        const to = from + limit - 1;

        const { data, count, error } = await supabase
            .from('telemetry_logs')
            .select('*', { count: 'exact' })
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

// 3. API xuất toàn bộ dữ liệu ra file CSV
app.get('/api/export-csv', async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('telemetry_logs')
            .select('*')
            .order('id', { ascending: false })
            .limit(5000);

        if (error) return res.status(500).send("Lỗi xuất file");

        let csv = "ID,Thoi_Gian,Nhiet_Do_T,Do_Man_S,pH,DO,Do_Kiem_Alk,Btri,Fan,IL,DOM,Surv,Adapt,CS\n";
        data.forEach(r => {
            const timeStr = new Date(r.created_at).toLocaleString('vi-VN', { timeZone: 'Asia/Ho_Chi_Minh' });
            csv += `${r.id},"${timeStr}",${r.T},${r.S},${r.pH},${r.DO},${r.alk},${r.btri},${r.fan},${r.il},${r.dom},${r.surv},${r.adapt_acc},${r.cs}\n`;
        });

        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="telemetry_logs.csv"');
        res.status(200).send('\uFEFF' + csv);
    } catch (err) {
        res.status(500).send("Lỗi hệ thống");
    }
});

// Xử lý Downlink
io.on('connection', (socket) => {
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
