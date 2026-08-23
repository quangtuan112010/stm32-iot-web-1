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

// Hàm tạo chuỗi ngày giờ Việt Nam chuẩn 24h
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

mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const sensorVal = parseFloat(rawStr) || 0;
        const timeVN = getVietnamTimeString();

        console.log(`[MQTT] [${timeVN}] Nhan du lieu: ${rawStr}`);

        // 1. Lưu vào Database Supabase
        const { error } = await supabase
            .from('telemetry_logs')
            .insert([{ device_id: 'STM32H7A3_01', sensor_value: sensorVal, raw_payload: rawStr }]);

        if (error) {
            console.error('[SUPABASE ERROR]:', error.message);
        }

        // 2. Gửi chuỗi giờ Việt Nam định dạng sẵn về Web
        io.emit('new_telemetry', {
            time_vn: timeVN,
            time_short: timeVN.split(' ')[0], // HH:mm:ss cho đồ thị
            sensor_value: sensorVal,
            raw_payload: rawStr
        });
    }
});

// API trả về 100 bản ghi
app.get('/api/logs', async (req, res) => {
    const { data, error } = await supabase
        .from('telemetry_logs')
        .select('*')
        .order('id', { ascending: false })
        .limit(100);
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
            .eq('device_id', 'STM32H7A3_01');

        io.emit('control_updated', newVal);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
