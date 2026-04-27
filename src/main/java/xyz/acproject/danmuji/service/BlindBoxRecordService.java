package xyz.acproject.danmuji.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import xyz.acproject.danmuji.entity.blindbox.BlindBoxProfitSummary;
import xyz.acproject.danmuji.tools.file.FileTools;

import javax.annotation.PostConstruct;
import java.io.File;
import java.sql.*;
import java.util.Calendar;

@Service
public class BlindBoxRecordService {
    private static final Logger LOGGER = LogManager.getLogger(BlindBoxRecordService.class);
    private static final long THREE_MONTHS_MS = 90L * 24 * 60 * 60 * 1000;
    private static final String DB_NAME = "DanmujiBlindBox.db";
    private String dbUrl;

    @PostConstruct
    public void init() {
        try {
            FileTools fileTools = new FileTools();
            String storeDir = fileTools.getBaseJarPath().getAbsolutePath();
            File dbFile = new File(storeDir, DB_NAME);
            dbUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS blind_box_record (" +
                        "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                        "room_id INTEGER NOT NULL," +
                        "anchor_uid INTEGER," +
                        "sender_uid INTEGER NOT NULL," +
                        "sender_uname TEXT," +
                        "blind_gift_id INTEGER," +
                        "blind_gift_name TEXT," +
                        "opened_gift_id INTEGER," +
                        "opened_gift_name TEXT," +
                        "box_num INTEGER NOT NULL DEFAULT 1," +
                        "cost_coin INTEGER NOT NULL DEFAULT 0," +
                        "reward_coin INTEGER NOT NULL DEFAULT 0," +
                        "profit_coin INTEGER NOT NULL DEFAULT 0," +
                        "event_time INTEGER NOT NULL," +
                        "created_at INTEGER NOT NULL" +
                        ")");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_blind_box_user_time ON blind_box_record(sender_uid, event_time)");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_blind_box_room_time ON blind_box_record(room_id, event_time)");
            }
        } catch (Exception e) {
            LOGGER.error("初始化盲盒数据库失败", e);
        }
    }

    public void recordBlindBoxGift(JSONObject data, Long roomId, Long anchorUid) {
        if (data == null) {
            return;
        }
        JSONObject blindGift = data.getJSONObject("blind_gift");
        if (blindGift == null) {
            return;
        }
        
        LOGGER.debug("盲盒原始字段 blind_gift={}, uid={}, roomId={}",
                blindGift.toJSONString(),
                data.getLong("uid"),
                roomId);
        
        long senderUid = data.getLongValue("uid");
        if (senderUid <= 0) {
            return;
        }
        int num = positiveInt(data.getInteger("num"), 1);
        long costCoin = data.getLongValue("total_coin");
        if (costCoin <= 0) {
            costCoin = positiveLong(data.getLong("price"), 0L) * num;
        }
        long rewardCoin = blindGift.getLongValue("total_coin");
        if (rewardCoin <= 0) {
            long rewardPrice = firstPositive(
                    blindGift.getLong("gift_tip_price"),
                    blindGift.getLong("r_price"),
                    blindGift.getLong("price"),
                    blindGift.getLong("gift_price"),
                    data.getLong("r_price")
            );
            int rewardNum = positiveInt(firstPositiveInt(
                    blindGift.getInteger("num"),
                    blindGift.getInteger("gift_num"),
                    blindGift.getInteger("original_gift_num")
            ), num);
            rewardCoin = rewardPrice * rewardNum;
        }
        long profitCoin = rewardCoin - costCoin;

        long eventTime = data.getLongValue("timestamp");
        eventTime = eventTime > 1000000000000L ? eventTime : eventTime * 1000;
        long now = System.currentTimeMillis();
        if (eventTime <= 0) {
            eventTime = now;
        }
        long fixedRoomId = roomId == null ? 0L : roomId;
        long fixedAnchorUid = anchorUid == null ? 0L : anchorUid;

        String sql = "INSERT INTO blind_box_record(" +
                "room_id,anchor_uid,sender_uid,sender_uname,blind_gift_id,blind_gift_name,opened_gift_id,opened_gift_name," +
                "box_num,cost_coin,reward_coin,profit_coin,event_time,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, fixedRoomId);
            ps.setLong(2, fixedAnchorUid);
            ps.setLong(3, senderUid);
            ps.setString(4, data.getString("uname"));
            ps.setInt(5, data.getIntValue("giftId"));
            ps.setString(6, firstNotBlank(data.getString("giftName"), blindGift.getString("original_gift_name")));
            ps.setInt(7, blindGift.getIntValue("gift_id"));
            ps.setString(8, firstNotBlank(blindGift.getString("gift_name"), blindGift.getString("name")));
            ps.setInt(9, num);
            ps.setLong(10, costCoin);
            ps.setLong(11, rewardCoin);
            ps.setLong(12, profitCoin);
            ps.setLong(13, eventTime);
            ps.setLong(14, now);
            ps.executeUpdate();
        } catch (Exception e) {
            LOGGER.error("写入盲盒记录失败, data={}", data, e);
        }
    }

    public BlindBoxProfitSummary queryUserLastThreeMonths(Long uid, Long roomId) {
        BlindBoxProfitSummary summary = new BlindBoxProfitSummary();
        summary.setUid(uid);
        summary.setRoomId(roomId);
        fillRange(summary);
        if (uid == null || uid <= 0) {
            return summary;
        }
        String sql = "SELECT COUNT(1) total_count, COALESCE(SUM(cost_coin),0) total_cost, " +
                "COALESCE(SUM(reward_coin),0) total_reward, COALESCE(SUM(profit_coin),0) total_profit " +
                "FROM blind_box_record WHERE sender_uid=? AND event_time>=?" +
                (roomId != null && roomId > 0 ? " AND room_id=?" : "");
        try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, uid);
            ps.setLong(2, summary.getFromTimestamp());
            if (roomId != null && roomId > 0) {
                ps.setLong(3, roomId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summary.setTotalCount(rs.getLong("total_count"));
                    summary.setTotalCostCoin(rs.getLong("total_cost"));
                    summary.setTotalRewardCoin(rs.getLong("total_reward"));
                    summary.setTotalProfitCoin(rs.getLong("total_profit"));
                }
            }
        } catch (Exception e) {
            LOGGER.error("查询用户盲盒统计失败 uid={}, roomId={}", uid, roomId, e);
        }
        return summary;
    }

    public BlindBoxProfitSummary queryRoomLastThreeMonths(Long roomId) {
        BlindBoxProfitSummary summary = new BlindBoxProfitSummary();
        summary.setRoomId(roomId);
        fillRange(summary);
        if (roomId == null || roomId <= 0) {
            return summary;
        }
        String sql = "SELECT COUNT(1) total_count, COALESCE(SUM(cost_coin),0) total_cost, " +
                "COALESCE(SUM(reward_coin),0) total_reward, COALESCE(SUM(profit_coin),0) total_profit " +
                "FROM blind_box_record WHERE room_id=? AND event_time>=?";
        try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, roomId);
            ps.setLong(2, summary.getFromTimestamp());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summary.setTotalCount(rs.getLong("total_count"));
                    summary.setTotalCostCoin(rs.getLong("total_cost"));
                    summary.setTotalRewardCoin(rs.getLong("total_reward"));
                    summary.setTotalProfitCoin(rs.getLong("total_profit"));
                }
            }
        } catch (Exception e) {
            LOGGER.error("查询房间盲盒统计失败 roomId={}", roomId, e);
        }
        return summary;
    }

    public BlindBoxProfitSummary queryUserByMonth(Long uid, Long roomId, int year, int month) {
        BlindBoxProfitSummary summary = new BlindBoxProfitSummary();
        summary.setUid(uid);
        summary.setRoomId(roomId);
        long[] range = getMonthRange(year, month);
        summary.setFromTimestamp(range[0]);
        summary.setToTimestamp(range[1]);
        if (uid == null || uid <= 0) {
            return summary;
        }
        String sql = "SELECT COUNT(1) total_count, COALESCE(SUM(cost_coin),0) total_cost, " +
                "COALESCE(SUM(reward_coin),0) total_reward, COALESCE(SUM(profit_coin),0) total_profit " +
                "FROM blind_box_record WHERE sender_uid=? AND event_time>=? AND event_time<?" +
                (roomId != null && roomId > 0 ? " AND room_id=?" : "");
        try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, uid);
            ps.setLong(2, summary.getFromTimestamp());
            ps.setLong(3, summary.getToTimestamp());
            if (roomId != null && roomId > 0) {
                ps.setLong(4, roomId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summary.setTotalCount(rs.getLong("total_count"));
                    summary.setTotalCostCoin(rs.getLong("total_cost"));
                    summary.setTotalRewardCoin(rs.getLong("total_reward"));
                    summary.setTotalProfitCoin(rs.getLong("total_profit"));
                }
            }
        } catch (Exception e) {
            LOGGER.error("查询用户盲盒月统计失败 uid={}, roomId={}, year={}, month={}", uid, roomId, year, month, e);
        }
        return summary;
    }

    public BlindBoxProfitSummary queryRoomByMonth(Long roomId, int year, int month) {
        BlindBoxProfitSummary summary = new BlindBoxProfitSummary();
        summary.setRoomId(roomId);
        long[] range = getMonthRange(year, month);
        summary.setFromTimestamp(range[0]);
        summary.setToTimestamp(range[1]);
        if (roomId == null || roomId <= 0) {
            return summary;
        }
        String sql = "SELECT COUNT(1) total_count, COALESCE(SUM(cost_coin),0) total_cost, " +
                "COALESCE(SUM(reward_coin),0) total_reward, COALESCE(SUM(profit_coin),0) total_profit " +
                "FROM blind_box_record WHERE room_id=? AND event_time>=? AND event_time<?";
        try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, roomId);
            ps.setLong(2, summary.getFromTimestamp());
            ps.setLong(3, summary.getToTimestamp());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    summary.setTotalCount(rs.getLong("total_count"));
                    summary.setTotalCostCoin(rs.getLong("total_cost"));
                    summary.setTotalRewardCoin(rs.getLong("total_reward"));
                    summary.setTotalProfitCoin(rs.getLong("total_profit"));
                }
            }
        } catch (Exception e) {
            LOGGER.error("查询房间盲盒月统计失败 roomId={}, year={}, month={}", roomId, year, month, e);
        }
        return summary;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl);
    }

    private void fillRange(BlindBoxProfitSummary summary) {
        long now = System.currentTimeMillis();
        summary.setToTimestamp(now);
        summary.setFromTimestamp(now - THREE_MONTHS_MS);
    }

    private long[] getMonthRange(int year, int month) {
        Calendar start = Calendar.getInstance();
        start.set(Calendar.YEAR, year);
        start.set(Calendar.MONTH, month - 1);
        start.set(Calendar.DAY_OF_MONTH, 1);
        start.set(Calendar.HOUR_OF_DAY, 0);
        start.set(Calendar.MINUTE, 0);
        start.set(Calendar.SECOND, 0);
        start.set(Calendar.MILLISECOND, 0);
        Calendar end = (Calendar) start.clone();
        end.add(Calendar.MONTH, 1);
        return new long[]{start.getTimeInMillis(), end.getTimeInMillis()};
    }

    private long firstPositive(Long... values) {
        if (values == null) {
            return 0L;
        }
        for (Long value : values) {
            if (value != null && value > 0) {
                return value;
            }
        }
        return 0L;
    }

    private Integer firstPositiveInt(Integer... values) {
        if (values == null) {
            return null;
        }
        for (Integer value : values) {
            if (value != null && value > 0) {
                return value;
            }
        }
        return null;
    }

    private int positiveInt(Integer value, int fallback) {
        return value == null || value <= 0 ? fallback : value;
    }

    private long positiveLong(Long value, long fallback) {
        return value == null || value <= 0 ? fallback : value;
    }

    private String firstNotBlank(String first, String second) {
        if (StringUtils.isNotBlank(first)) {
            return first;
        }
        return second;
    }
}

