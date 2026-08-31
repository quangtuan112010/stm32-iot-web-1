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

const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

// Nhận chuỗi JSON từ STM32 / Arduino
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const timeVN = getVietnamTimeString();
        let jsonData = {};

        try {
            jsonData = JSON.parse(rawStr);
        } catch (e) {
            jsonData = { raw: rawStr };
        }

        console.log(`[MQTT] [${timeVN}] Nhan JSON:`, jsonData);

        // Lưu trực tiếp các trường vào Database
        const { error } = await supabase
            .from('telemetry_logs')
            .insert([{
                do: jsonData.do || 0,
                ph: jsonData.ph || 0,
                t: jsonData.t || 0,
                salinity: jsonData.salinity || 0,
                kiem: jsonData.kiem || 0
            }]);

        if (error) {
            console.error('[SUPABASE ERROR]:', error.message);
        }

        // Bắn dữ liệu chuẩn JSON sang Web qua Socket.io
        io.emit('new_telemetry', {
            time_vn: timeVN,
            data: jsonData
        });
    }
});

// API trả về lịch sử dưới dạng mảng JSON thuần túy
app.get('/api/logs', async (req, res) => {
    const { data, error } = await supabase
        .from('telemetry_logs')
        .select('do, ph, t, salinity, kiem, created_at')
        .order('id', { ascending: false })
        .limit(50);
    if (error) {
        return res.status(500).json({ error: error.message });
    }
    res.json(data || []);
});

io.on('connection', (socket) => {
    socket.on('send_control', async (newVal) => {
        console.log(`[DOWNLINK] Phat lenh xuong STM32: ${newVal}`);
        mqttClient.publish(TOPIC_DOWNLINK, String(newVal));

        await supabase
            .from('device_controls')
            .update({ target_value: parseInt(newVal) || 0 })
            .eq('id', 1);

        io.emit('control_updated', newVal);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
