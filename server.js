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

// Kết nối MQTT Broker qua TLS
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
        let data = {};

        try {
            data = JSON.parse(rawStr);
        } catch (e) {
            console.error('[JSON ERROR]: Khong the parse payload:', rawStr);
            return;
        }

        console.log(`[MQTT] [${timeVN}] Nhan du lieu:`, data);

        // Lưu vào Supabase chuẩn 12 trường
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

// 1. API lấy dữ liệu vẽ đồ thị theo khoảng thời gian tùy chọn
app.get('/api/chart-data', async (req, res) => {
    try {
        const { value = 30, unit = 'minute' } = req.query;
        const valNum = parseInt(value) || 30;

        let cutoff = new Date();
        if (unit === 'minute') cutoff.setMinutes(cutoff.getMinutes() - valNum);
        else if (unit === 'hour') cutoff.setHours(cutoff.getHours() - valNum);
        else if (unit === 'day') cutoff.setDate(cutoff.getDate() - valNum);
        else if (unit === 'week') cutoff.setDate(cutoff.getDate() - (valNum * 7));
        else if (unit === 'month') cutoff.setMonth(cutoff.getMonth() - valNum);
        else cutoff.setMinutes(cutoff.getMinutes() - 30);

        // Lấy dữ liệu từ mốc thời gian đã chọn
        const { data, error } = await supabase
            .from('telemetry_logs')
            .select('T, S, pH, DO, btri, created_at')
            .gte('created_at', cutoff.toISOString())
            .order('id', { ascending: true })
            .limit(1000);

        if (error || !data || data.length === 0) {
            // Dự phòng: lấy 100 bản ghi mới nhất nếu chưa có dữ liệu theo mốc
            const fallback = await supabase
                .from('telemetry_logs')
                .select('T, S, pH, DO, btri, created_at')
                .order('id', { ascending: false })
                .limit(100);
            return res.json(fallback.data ? fallback.data.reverse() : []);
        }

        res.json(data);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 2. API phân trang xem toàn bộ Database lịch sử mà không cần vào Supabase
app.get('/api/logs-paged', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 50;
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

// 3. API tải toàn bộ dữ liệu dạng file CSV (Excel)
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

// Xử lý lệnh Downlink
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
