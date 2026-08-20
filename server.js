// Nhận dữ liệu chu kỳ 5s từ SIM7680C gửi lên
mqttClient.on('message', async (topic, message) => {
    if (topic === TOPIC_UPLINK) {
        const rawStr = message.toString().trim();
        const sensorVal = parseFloat(rawStr) || 0;

        console.log(`[MQTT] Nhan du lieu: ${rawStr}`);

        // 1. Lưu bản ghi vào PostgreSQL Database
        const { data, error } = await supabase
            .from('telemetry_logs')
            .insert([{ device_id: 'STM32H7A3_01', sensor_value: sensorVal, raw_payload: rawStr }]);

        if (error) {
            console.error('[SUPABASE ERROR] Khong the luu vao DB:', error.message);
        } else {
            console.log('[SUPABASE SUCCESS] Da luu thanh cong vao telemetry_logs');
        }

        // 2. Phát Realtime lên giao diện Web
        io.emit('new_telemetry', {
            timestamp: new Date().toLocaleTimeString(),
            sensor_value: sensorVal,
            raw_payload: rawStr
        });
    }
});
