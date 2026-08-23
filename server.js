const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// 1. Biến môi trường
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

// 2. Kết nối MQTT HiveMQ Cloud
const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

// 3. Nhận dữ liệu Uplink từ STM32 / SIM7680C
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const sensorVal = parseFloat(rawStr) || 0;

        console.log(`[MQTT] Nhan du lieu: ${rawStr}`);

        // Lưu vào Supabase (để created_at tự gán mặc định giờ VN theo bảng đã tạo)
        const { data, error } = await supabase
            .from('telemetry_logs')
            .insert([{ device_id: 'STM32H7A3_01', sensor_value: sensorVal, raw_payload: rawStr }])
            .select();

        let recordedTime = new Date().toISOString();
        if (data && data[0] && data[0].created_at) {
            recordedTime = data[0].created_at;
        }

        if (error) {
            console.error('[SUPABASE ERROR]:', error.message);
        } else {
            console.log('[SUPABASE SUCCESS] Da ghi vao DB:', recordedTime);
        }

        // Bắn Socket.IO Realtime sang giao diện Web
        io.emit('new_telemetry', {
            created_at: recordedTime,
            sensor_value: sensorVal,
            raw_payload: rawStr
        });
    }
});

// 4. API lấy 100 bản ghi lịch sử
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

// 5. Gửi Downlink xuống STM32
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
