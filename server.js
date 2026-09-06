// ================= MODULE THEO DÕI MƯA THỰC TẾ XUÂN ĐỊNH (ĐỒNG NAI) =================
let latestRainReport = {
    location: "Xã Xuân Định, Xuân Lộc, Đồng Nai",
    is_raining_now: false,
    current_status: "Trời đang tạnh ráo",
    last_updated: "--:--",
    events_today: []
};

async function updateXuanDinhRainHistory() {
    const lat = 10.90;
    const lon = 107.25;
    // Lấy dữ liệu quan trắc thực tế 15 phút/lần từ hôm qua và hôm nay
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&minutely_15=precipitation&past_days=1&forecast_days=1&timezone=Asia%2FHo_Chi_Minh`;

    try {
        const res = await fetch(url);
        const data = await res.json();
        if (!data.minutely_15) return;

        const times = data.minutely_15.time;
        const precips = data.minutely_15.precipitation;

        // Lấy thời gian hiện tại chuẩn Việt Nam (GMT+7) định dạng YYYY-MM-DDTHH:mm
        const nowVN = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
        const pad = (n) => String(n).padStart(2, '0');
        const todayPrefix = `${nowVN.getFullYear()}-${pad(nowVN.getMonth() + 1)}-${pad(nowVN.getDate())}`;
        const nowIsoStr = `${todayPrefix}T${pad(nowVN.getHours())}:${pad(nowVN.getMinutes())}`;

        let events = [];
        let inRain = false;
        let rainStart = "";
        let rainStartIso = "";
        let maxRainRate = 0;

        for (let i = 0; i < times.length; i++) {
            const timeStr = times[i];
            
            // CHỈ LỌC DỮ LIỆU ĐÃ XẢY RA TRONG NGÀY HÔM NAY (BỎ HOÀN TOÀN TƯƠNG LAI)
            if (!timeStr.startsWith(todayPrefix) || timeStr > nowIsoStr) continue;

            const rainAmount = precips[i] || 0.0;
            const isRaining = rainAmount >= 0.1; // Mưa từ 0.1mm
            const hourMin = timeStr.split("T")[1];

            if (isRaining) {
                if (!inRain) {
                    inRain = true;
                    rainStart = hourMin;
                    rainStartIso = timeStr;
                    maxRainRate = rainAmount;
                } else {
                    if (rainAmount > maxRainRate) maxRainRate = rainAmount;
                }
            } else {
                if (inRain) {
                    inRain = false;
                    const durMinutes = Math.round((new Date(timeStr) - new Date(rainStartIso)) / (1000 * 60));
                    events.push({
                        start: rainStart,
                        end: hourMin,
                        duration: `${durMinutes} phút`,
                        max_rate: `${maxRainRate.toFixed(1)} mm/15p`,
                        status: "Đã tạnh"
                    });
                }
            }
        }

        // Kiểm tra xem hiện tại có đang còn mưa dở hay không
        let isRainingNow = false;
        if (inRain) {
            isRainingNow = true;
            const durMinutes = Math.round((nowVN - new Date(rainStartIso)) / (1000 * 60));
            events.push({
                start: rainStart,
                end: "Hiện tại",
                duration: `${durMinutes} phút (vẫn đang mưa)`,
                max_rate: `${maxRainRate.toFixed(1)} mm/15p`,
                status: "Đang mưa"
            });
        }

        latestRainReport = {
            location: "Xã Xuân Định, Xuân Lộc, Đồng Nai",
            is_raining_now: isRainingNow,
            current_status: isRainingNow ? `Trời đang mưa (từ ${rainStart})` : "Trời đang tạnh ráo",
            last_updated: `${pad(nowVN.getHours())}:${pad(nowVN.getMinutes())}:${pad(nowVN.getSeconds())}`,
            events_today: events.reverse() // Cơn mưa mới nhất xếp lên đầu
        };

        io.emit('rain_status_update', latestRainReport);
    } catch (err) {
        console.error("[RAIN API ERROR]:", err.message);
    }
}

// Cứ mỗi 5 phút quét kiểm tra lịch sử mưa thực tế 1 lần
setInterval(updateXuanDinhRainHistory, 5 * 60 * 1000);
setTimeout(updateXuanDinhRainHistory, 3000); // Chạy ngay sau khi bật server

// API endpoint để web gọi chủ động
app.get('/api/rain-history', (req, res) => {
    res.json(latestRainReport);
});
