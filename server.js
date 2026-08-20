const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// 1. Cấu hình biến môi trường
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

// 2. Khởi tạo kết nối MQTT Broker
const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

// 3. Nhận dữ liệu chu kỳ 5s từ SIM7680C gửi lên
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const sensorVal = parseFloat(rawStr) || 0;

        console.log(`[MQTT] Nhan du lieu: ${rawStr}`);

        // Lưu bản ghi vào PostgreSQL Database
        const { data, error } = await supabase
            .from('telemetry_logs')
            .insert([{ device_id: 'STM32H7A3_01', sensor_value: sensorVal, raw_payload: rawStr }]);

        if (error) {
            console.error('[SUPABASE ERROR] Khong the luu vao DB:', error.message);
        } else {
            console.log('[SUPABASE SUCCESS] Da luu thanh cong vao telemetry_logs');
        }

        // Phát Realtime lên giao diện Web
        io.emit('new_telemetry', {
            timestamp: new Date().toLocaleTimeString(),
            sensor_value: sensorVal,
            raw_payload: rawStr
        });
    }
});

// 4. API lấy lịch sử dữ liệu
app.get('/api/logs', async (req, res) => {
    const { data, error } = await supabase
        .from('telemetry_logs')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(100);
    if (error) {
        return res.status(500).json({ error: error.message });
    }
    res.json(data || []);
});

// 5. Gửi lệnh điều khiển từ Web xuống STM32
io.on('connection', (socket) => {
    socket.on('send_control', async (newVal) => {
        console.log(`[DOWNLINK] Gui lenh xuong STM32: ${newVal}`);
        mqttClient.publish(TOPIC_DOWNLINK, String(newVal));

        await supabase
            .from('device_controls')
            .update({ target_value: parseInt(newVal) || 0, updated_at: new Date() })
            .eq('device_id', 'STM32H7A3_01');

        io.emit('control_updated', newVal);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
