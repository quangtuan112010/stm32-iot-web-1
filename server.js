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

// Hàm định dạng giờ Việt Nam
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

// Nhận gói tin JSON từ STM32 gửi lên
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

        // Lưu vào Database Supabase khớp 1:1 các trường đặc tả
        const { error } = await supabase
            .from('telemetry_logs')
            .insert([{
                T: parseFloat(data.T) || 0,
                S: parseFloat(data.S) || 0,
                pH: parseFloat(data.pH) || 0,
                DO: parseFloat(data.DO) || 0,
                alk: parseFloat(data.alk) || -1.0,
                btri: parseFloat(data.btri) || 0,
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

        // Bắn dữ liệu Realtime sang Web Dashboard
        io.emit('new_telemetry', {
            time_vn: timeVN,
            data: data
        });
    }
});

// API cung cấp lịch sử log cho web
app.get('/api/logs', async (req, res) => {
    const { data, error } = await supabase
        .from('telemetry_logs')
        .select('*')
        .order('id', { ascending: false })
        .limit(50);
    if (error) {
        return res.status(500).json({ error: error.message });
    }
    res.json(data || []);
});

// Xử lý lệnh Downlink từ Web gửi xuống STM32
io.on('connection', (socket) => {
    socket.on('send_control', async (commandStr) => {
        console.log(`[DOWNLINK] Phat lenh xuong STM32: ${commandStr}`);
        
        // Publish lệnh ASCII thuần qua MQTT Topic stm32/control-value
        mqttClient.publish(TOPIC_DOWNLINK, String(commandStr));

        // Lưu log lệnh vào DB
        await supabase
            .from('device_controls')
            .update({ last_command: commandStr, updated_at: new Date() })
            .eq('device_id', 'STM32_Tilapia_01');

        io.emit('control_status', `Đã phát lệnh: ${commandStr}`);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
