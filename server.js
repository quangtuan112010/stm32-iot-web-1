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

// 2. Kết nối MQTT Broker
const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false
});

mqttClient.on('connect', () => {
    console.log('[MQTT] Connected to HiveMQ Cloud successfully');
    mqttClient.subscribe(TOPIC_UPLINK);
});

// 3. Nhận dữ liệu từ SIM7680C gửi lên
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        let arrayData = [];

        try {
            if (rawStr.startsWith('[') && rawStr.endsWith(']')) {
                arrayData = JSON.parse(rawStr);
            } else {
                arrayData = rawStr.split(',').map(item => item.trim()).filter(Boolean);
            }
        } catch (e) {
            arrayData = [rawStr];
        }

        console.log(`[MQTT] Nhan du lieu: ${rawStr}`);

        // Lưu bản ghi vào PostgreSQL Database (Supabase)
        const { data, error } = await supabase
            .from('telemetry_logs')
            .insert([{ device_id: 'STM32H7A3_01', sensor_value: parseFloat(arrayData[0]) || 0, raw_payload: rawStr }]);

        if (error) {
            console.error('[SUPABASE ERROR] Khong the luu vao DB:', error.message);
        } else {
            console.log('[SUPABASE SUCCESS] Da luu thanh cong vao telemetry_logs');
        }

        // Bắn dữ liệu Realtime lên Web kèm mốc ISO thời gian
        io.emit('new_telemetry', {
            created_at: new Date().toISOString(),
            array_data: arrayData,
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
            .update({ target_value: parseInt(newVal) || 0, updated_at: new Date().toISOString() })
            .eq('device_id', 'STM32H7A3_01');

        io.emit('control_updated', newVal);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
