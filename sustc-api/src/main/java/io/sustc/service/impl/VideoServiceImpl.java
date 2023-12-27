package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;

    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {

        //参考b站bv生成方式
        // generate bv(assume use UUID )
        String bv = UUID.randomUUID().toString();
        // commitTime
        Timestamp commitTime = Timestamp.valueOf(LocalDateTime.now());
//        LocalDateTime commitTime=LocalDateTime.now();

        // SQL
        String sql_video = "INSERT INTO VideoRecord (bv, title, ownerMid, ownerName, commitTime, duration, description) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            // verify auth
            if (auth == null || !is_valid_auth(auth, conn)) {
                return null;
            }
            // verrify requst
            if (req == null || !is_valid_req(req, conn, auth)) {
                return null;
            }

            try (PreparedStatement stmt = conn.prepareStatement(sql_video, Statement.RETURN_GENERATED_KEYS)) {
                stmt.setString(1, bv);
                stmt.setString(2, req.getTitle());
                stmt.setLong(3, auth.getMid());
                stmt.setString(4, getOwnerName(auth.getMid(), conn)); // 获取视频所有者名字
                stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis())); // 提交时间为当前时间
                stmt.setFloat(6, req.getDuration());
                stmt.setString(7, req.getDescription());

                int affectedRows = stmt.executeUpdate();
                if (affectedRows == 0) {
                    return null; // insertion failed
                }
                // get bv
                try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        bv = generatedKeys.getString(1);
                    } else {
                        return null; //获取主键失败
                    }
                }
//                // Execute the statement and get generated keys
//                ResultSet generatedKeys = stmt.executeQuery(); // Use executeQuery for returning values
//                if (generatedKeys.next()) {
//                    return generatedKeys.getLong(1); // Return the generated danmu_id
//                } else {
//                    throw new SQLException("Insertion failed, no ID obtained.");
//                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return bv;
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            // get video
            VideoRecord video = getVideoByBV(bv, conn);
            //video does exist according to bv, auth is owner/superuser
            if (video == null) {
                return false;
            }
            //verify auth
            if (!is_valid_auth(auth, conn)) {
                return false;
            }
            //verify identity
            if (!(auth.getMid() == (video.getOwnerMid())) && !isSuperuser(auth, conn)) {
                return false;
            }

            //remove likes, collects, favorites, return coins
            // start deleting
            try {
                conn.setAutoCommit(false);// 关闭自动提交

                // 删除其他关联表中的记录
                // 删除视频相关记录ViewRecord,DanmuRecord,likes,favorites,coins
                if (!deleteRelatedRecords(bv, conn)) {
                    return false;
                }
                // 删除视频本身
                if (!deleteVideoRecord(bv, conn)) {
                    return false;
                }

                conn.commit();

            } catch (SQLException e) {
                try {//gpt给出了很顶的建议，他说可能在rollback的时候抛出异常
                    conn.rollback();
                } catch (SQLException ex) {
                    e.addSuppressed(ex);//补充报错信息
                }
                throw new RuntimeException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        //return false when(1)has not been reviewed (2)invalid
        // 检查输入参数
        if (auth == null || bv == null || req == null) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            // 获取视频信息
            VideoRecord video = getVideoByBV(bv, conn);
            if (video == null) {
                // 视频不存在
                return false;
            }
            //valid auth?
            if (!is_valid_auth(auth, conn)) {
                return false;
            }
            //is onwer?
            if (auth.getMid() != video.getOwnerMid()) {
                return false;
            }
            //valid req?
            if (!is_valid_req(req, conn, auth)) {
                return false;
            }
            //duration changed?(it can't be changed)
            if (video.getDuration() != req.getDuration()) {
                return false;
            }
            //req not changed?
            if (video.getTitle().equals(req.getTitle()) &&
                    video.getDescription().equals(req.getDescription()) &&
                    video.getPublicTime().equals(req.getPublicTime())) {
                // 没有实质性变化
                return false;
            }
            //has been viewed before? if yes, return true
            if (video.getReviewTime() == null) {
                return false;
            }
            // 准备更新视频信息的 SQL 语句
            String sql = "UPDATE VideoRecord SET title = ?, description = ?, publicTime = ? WHERE bv = ?";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                // 设置 SQL 参数
                stmt.setString(1, req.getTitle());
                stmt.setString(2, req.getDescription());
                stmt.setTimestamp(3, req.getPublicTime());
                stmt.setString(4, bv);
                // 执行更新
                int rowsUpdated = stmt.executeUpdate();
                return rowsUpdated > 0;//这里我让执行成功才返回，但是描述是说只要之前被review过就返回true
            }
        } catch (SQLException e) {
            // 异常处理
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        //用一串keyword搜索
        //unreviewed/unpublished时不能被搜索
        //要分状态啊，public且被review前不可见，update后在rereview前不可见
        //要verify req?
        if (auth == null || keywords == null || keywords.isEmpty() || pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        try (Connection conn = dataSource.getConnection()) {
            if (!is_valid_auth(auth, conn)) {
                return null;
            }

            // 分割关键词
            String[] keywordArray = keywords.split("\\s+");
            StringBuilder keywordSearchBuilder = new StringBuilder();
            for (int i = 0; i < keywordArray.length; i++) {
                keywordSearchBuilder.append("LOWER(title) LIKE LOWER(?) OR LOWER(description) LIKE LOWER(?) OR LOWER(ownerName) LIKE LOWER(?)");
                if (i < keywordArray.length - 1) {
                    keywordSearchBuilder.append(" OR ");
                }
            }

            // 构建 SQL 查询语句
            String sql = "SELECT bv, " +
                    "( " +
                    "  SELECT SUM(match_count) FROM ( " +
                    "    SELECT " +
                    "      CASE WHEN LOWER(title) LIKE LOWER(?) THEN 1 ELSE 0 END + " +
                    "      CASE WHEN LOWER(description) LIKE LOWER(?) THEN 1 ELSE 0 END + " +
                    "      CASE WHEN LOWER(ownerName) LIKE LOWER(?) THEN 1 ELSE 0 END AS match_count " +
                    "  ) AS keyword_matches " +
                    ") AS relevance " +
                    "FROM VideoRecord WHERE " + keywordSearchBuilder.toString() +
                    "ORDER BY relevance DESC, (SELECT COUNT(*) FROM ViewRecord WHERE VideoRecord.bv = ViewRecord.bv) DESC " +
                    "LIMIT ? OFFSET ?";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                int paramIndex = 1;
                for (String keyword : keywordArray) {
                    String likePattern = "%" + keyword + "%";
                    for (int i = 0; i < 3; i++) { // Each keyword for title, description, and ownerName
                        stmt.setString(paramIndex++, likePattern);
                    }
                }
                for (String keyword : keywordArray) { // For relevance calculation
                    stmt.setString(paramIndex++, "%" + keyword + "%");
                }
                stmt.setInt(paramIndex++, pageSize);
                stmt.setInt(paramIndex++, (pageNum - 1) * pageSize);

                List<String> videoBVs = new ArrayList<>();
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        videoBVs.add(rs.getString("bv"));
                    }
                }

                return videoBVs;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public double getAverageViewRate(String bv) {
        // 检查 bv 是否有效
        if (bv == null || bv.isEmpty()) {
            return -1;
        }

        try (Connection conn = dataSource.getConnection()) {
            // 检查视频是否存在
            String videoCheckSql = "SELECT duration FROM VideoRecord WHERE bv = ?";
            try (PreparedStatement videoCheckStmt = conn.prepareStatement(videoCheckSql)) {
                videoCheckStmt.setString(1, bv);
                try (ResultSet videoCheckRs = videoCheckStmt.executeQuery()) {
                    if (!videoCheckRs.next()) {
                        // 视频不存在
                        return -1;
                    }
                    // 获取视频时长
                    int videoDuration = videoCheckRs.getInt("duration");
                    if (videoDuration <= 0) {
                        // 视频时长无效
                        return -1;
                    }

                    // 计算平均观看率
                    String viewRateSql = "SELECT AVG(timestamp) AS average_view_time FROM ViewRecord WHERE bv = ?";
                    try (PreparedStatement viewRateStmt = conn.prepareStatement(viewRateSql)) {
                        viewRateStmt.setString(1, bv);
                        try (ResultSet viewRateRs = viewRateStmt.executeQuery()) {
                            if (viewRateRs.next()) {
                                double averageViewTime = viewRateRs.getDouble("average_view_time");
                                if (viewRateRs.wasNull()) {
                                    // 没有人观看这个视频
                                    return -1;
                                }
                                // 计算并返回平均观看率
                                return averageViewTime / videoDuration;
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        if (bv == null || bv.isEmpty()) {
            return Collections.emptySet();
        }

        try (Connection conn = dataSource.getConnection()) {
            // 检查视频是否存在
            String videoCheckSql = "SELECT COUNT(*) FROM VideoRecord WHERE bv = ?";
            try (PreparedStatement videoCheckStmt = conn.prepareStatement(videoCheckSql)) {
                videoCheckStmt.setString(1, bv);
                try (ResultSet videoCheckRs = videoCheckStmt.executeQuery()) {
                    if (!videoCheckRs.next() || videoCheckRs.getInt(1) == 0) {
                        // 视频不存在
                        return Collections.emptySet();
                    }
                }
            }

            // 查询弹幕热点区域
            String hotspotSql = "SELECT FLOOR(time / 10) AS chunk, COUNT(*) AS danmu_count " +
                    "FROM DanmuRecord WHERE bv = ? " +
                    "GROUP BY chunk " +
                    "ORDER BY danmu_count DESC";
            try (PreparedStatement hotspotStmt = conn.prepareStatement(hotspotSql)) {
                hotspotStmt.setString(1, bv);
                try (ResultSet hotspotRs = hotspotStmt.executeQuery()) {
                    Set<Integer> hotspots = new HashSet<>();
                    int maxCount = 0;
                    while (hotspotRs.next()) {
                        int chunk = hotspotRs.getInt("chunk");
                        int count = hotspotRs.getInt("danmu_count");
                        if (hotspots.isEmpty()) {
                            maxCount = count;
                        } else if (count < maxCount) {
                            break; // 只选取最高计数的时间块
                        }
                        hotspots.add(chunk);
                    }
                    return hotspots;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        // 验证输入参数
        if (auth == null || bv == null || bv.isEmpty()) {
            return false;
        }

        try (Connection conn = dataSource.getConnection()) {
            // 验证用户身份
            if (!is_valid_auth(auth, conn) || !isSuperuser(auth, conn)) {
                return false;
            }

            // 检查视频是否存在且未被审核
            String videoCheckSql = "SELECT ownerMid, reviewTime FROM VideoRecord WHERE bv = ?";
            try (PreparedStatement videoCheckStmt = conn.prepareStatement(videoCheckSql)) {
                videoCheckStmt.setString(1, bv);
                try (ResultSet videoCheckRs = videoCheckStmt.executeQuery()) {
                    if (!videoCheckRs.next()) {
                        // 视频不存在
                        return false;
                    }
                    if (auth.getMid() == (videoCheckRs.getLong("ownerMid"))) {
                        // 用户是视频所有者
                        return false;
                    }
                    if (videoCheckRs.getTimestamp("reviewTime") != null) {
                        // 视频已被审核
                        return false;
                    }
                }
            }

            // 更新视频为已审核状态
            String reviewSql = "UPDATE VideoRecord SET reviewTime = CURRENT_TIMESTAMP WHERE bv = ?";
            try (PreparedStatement reviewStmt = conn.prepareStatement(reviewSql)) {
                reviewStmt.setString(1, bv);
                int rowsUpdated = reviewStmt.executeUpdate();
                return rowsUpdated > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        if (auth == null || bv == null) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            // 检查用户权限和是否有硬币
            if (!is_valid_auth(auth, conn) || !doesuserhasCoins(auth, conn)) {
                return false;
            }
            if (!canUserSearchVideo(auth, bv, conn)) {
                return false;
            }
            // get video
            VideoRecord video = getVideoByBV(bv, conn);
            //video does exist according to bv, auth is owner/superuser
            if (video == null) {
                return false;
            }
            //owner不可以coin
            if (auth.getMid() == video.getOwnerMid()) {
                return false;
            }
            // 检查用户是否已赠送硬币
            String coinCheckSql = "SELECT COUNT(*) FROM coins WHERE bv = ? AND mid_coin = ?";
            try (PreparedStatement coinCheckStmt = conn.prepareStatement(coinCheckSql)) {
                coinCheckStmt.setString(1, bv);
                coinCheckStmt.setLong(2, auth.getMid());

                try (ResultSet rs = coinCheckStmt.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        // 用户已赠送硬币
                        return false;
                    }
                }
            }

            //start coining!!!
            try {
                conn.setAutoCommit(false);// 开启 事务
                // 赠送硬币
                String sql = "INSERT INTO coins (BV_coin, mid_coin) VALUES (?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, bv);
                    stmt.setLong(2, auth.getMid());
                    stmt.executeUpdate();
                }
                //user的coin-1
                String sql2 = "UPDATE UserRecord SET coin = coin - 1 WHERE mid = ?";
                try (PreparedStatement stmt = conn.prepareStatement(sql2)) {
                    stmt.setLong(1, auth.getMid());
                    stmt.executeUpdate();
                }
                conn.commit();
                return true;
            } catch (SQLException e) {
                // 发生异常时回滚事务
                conn.rollback();
                e.printStackTrace();
            } finally {
                // 恢复自动提交设置
                conn.setAutoCommit(true);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        if (auth == null || bv == null) {
            return false;
        }

        try (Connection conn = dataSource.getConnection()) {
            if (!canUserSearchVideo(auth, bv, conn)) {
                return false;
            }
            // get video
            VideoRecord video = getVideoByBV(bv, conn);
            //video does exist according to bv
            if (video == null) {
                return false;
            }
            //verify auth
            if (!is_valid_auth(auth, conn)) {
                return false;
            }
            //不能给自己点赞
            if (auth.getMid() == video.getOwnerMid()) {
                return false;
            }
            // 检查用户是否已点赞
            String likeCheckSql = "SELECT COUNT(*) FROM likes WHERE bv = ? AND mid_liked = ?";
            try (PreparedStatement likeCheckStmt = conn.prepareStatement(likeCheckSql)) {
                likeCheckStmt.setString(1, bv);
                likeCheckStmt.setLong(2, auth.getMid());

                boolean alreadyLiked = false;
                try (ResultSet rs = likeCheckStmt.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        alreadyLiked = true;
                    }
                    if (alreadyLiked) {
                        // 如果已经点过赞，取消点赞
                        String deleteSql = "DELETE FROM likes WHERE bv = ? AND mid_liked = ?";
                        try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                            deleteStmt.setString(1, bv);
                            deleteStmt.setLong(2, auth.getMid());
                            deleteStmt.executeUpdate();
                            return false;
                        }
                    } else {
                        // 否则，添加点赞
                        String insertSql = "INSERT INTO likes (BV_liked, mid_liked) VALUES (?, ?)";
                        try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                            insertStmt.setString(1, bv);
                            insertStmt.setLong(2, auth.getMid());
                            insertStmt.executeUpdate();
                            return true;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        if (auth == null || bv == null) {
            return false;
        }

        try (Connection conn = dataSource.getConnection()) {
            if (!canUserSearchVideo(auth, bv, conn)) {
                return false;
            }
            // get video
            VideoRecord video = getVideoByBV(bv, conn);
            //video does exist according to bv, auth is owner/superuser
            if (video == null) {
                return false;
            }
            //verify auth
            if (!is_valid_auth(auth, conn)) {
                return false;
            }
            //verify identity
            if (auth.getMid() == video.getOwnerMid()) {
                return false;
            }
            // 检查用户是否已经收藏了视频
            String collectCheckSql = "SELECT COUNT(*) FROM favorites WHERE bv = ? AND mid_favorite = ?";
            try (PreparedStatement collectCheckStmt = conn.prepareStatement(collectCheckSql)) {
                collectCheckStmt.setString(1, bv);
                collectCheckStmt.setLong(2, auth.getMid());

                boolean alreadyCollected = false;
                try (ResultSet rs = collectCheckStmt.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        alreadyCollected = true;
                    }
                }

                if (alreadyCollected) {
                    // 如果已经收藏，取消收藏
                    String deleteSql = "DELETE FROM favorites WHERE bv = ? AND mid_favorite = ?";
                    try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                        deleteStmt.setString(1, bv);
                        deleteStmt.setLong(2, auth.getMid());
                        deleteStmt.executeUpdate();
                        return false;
                    }
                } else {
                    // 否则，添加收藏
                    String insertSql = "INSERT INTO favorites (BV_favorite, mid_favorite) VALUES (?, ?)";
                    try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                        insertStmt.setString(1, bv);
                        insertStmt.setLong(2, auth.getMid());
                        insertStmt.executeUpdate();
                        return true;
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    private boolean is_valid_auth(AuthInfo authInfo, Connection connection) {
        long mid = authInfo.getMid();
        String sql_is_deleted = """
                select password, qq, wechat, is_deleted
                from userrecord
                where mid = ?;""";
        try {
            PreparedStatement stmt = connection.prepareStatement(sql_is_deleted);
            stmt.setLong(1, mid);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String pwd = rs.getString(1);
                String qq = rs.getString(2);
                String wechat = rs.getString(3);
                boolean is_deleted = rs.getBoolean(4);

                boolean is_pwd = (Objects.equals(pwd, authInfo.getPassword()));
                boolean is_qq = (Objects.equals(qq, authInfo.getQq()));
                boolean is_wechat = (Objects.equals(wechat, authInfo.getWechat()));
                return (is_pwd && is_qq && is_wechat && !is_deleted);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private boolean is_valid_req(PostVideoReq req, Connection connection, AuthInfo auth) {
        // verify title
        if (req.getTitle() == null || req.getTitle().isEmpty()) {
            return false;
        }

        // verify description
        if (req.getDescription() == null || req.getDescription().isEmpty()) {
            return false;
        }

        // verify duration>10
        if (req.getDuration() < 10) {
            return false;
        }
        // verify same title
        if (hasUserPublishedVideoWithSameTitle(req.getTitle(), auth.getMid(), connection)) {
            return false;
        }
        // verify publicTime
        if (req.getPublicTime() != null && req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now()))) {
            return false;
        }

        return true;
    }

    private boolean isSuperuser(AuthInfo auth, Connection conn) {
        // SQL 查询语句，用于检查用户是否是超级用户
        String sql = "SELECT identity FROM UserRecord WHERE mid = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            // 设置查询参数
            stmt.setLong(1, auth.getMid());

            try (ResultSet rs = stmt.executeQuery()) {
                // 检查查询结果
                if (rs.next()) {//如果查到了
                    String identity = rs.getString("identity");
                    // 检查用户的身份是否为 'superuser'
                    return "superuser".equalsIgnoreCase(identity);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在发生异常时，可以选择记录日志或进行其他处理
        }

        return false;
    }

    private String getOwnerName(long mid, Connection conn) {
        // get owner name by mid
        String sql_get_owner_name = "SELECT name FROM UserRecord WHERE mid = ?";
        try {
            PreparedStatement stmt = conn.prepareStatement(sql_get_owner_name);
            stmt.setLong(1, mid);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("name");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null; // failed to get owner name
    }

    private boolean hasUserPublishedVideoWithSameTitle(String title, long ownerMid, Connection connection) {
        String sql = "SELECT COUNT(*) FROM VideoRecord WHERE title = ? AND ownerMid = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, title);
            stmt.setLong(2, ownerMid);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int count = rs.getInt(1);
                return count > 0; // if video with same title exits, return true
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false; // return false when failed to execute sql
    }

    public boolean canUserSearchVideo(AuthInfo auth, String bv, Connection conn) throws SQLException {
        VideoRecord video = getVideoByBV(bv, conn);
        // 检查用户权限（例如，是否为管理员或视频的上传者）
        if (video == null || isSuperuser(auth, conn) || auth.getMid() == video.getOwnerMid()) {
            return true;
        }
        // 查询视频信息，检查是否publish? reviewed?
        String sql = "SELECT publictime, reviewtime FROM VideoRecord WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Timestamp publicTime = rs.getTimestamp("publicTime");
                    Timestamp reviewTime = rs.getTimestamp("reviewTime");

                    // 检查视频是否已发布和已审核
                    boolean isPublished = publicTime != null && publicTime.before(new Timestamp(System.currentTimeMillis()));
                    boolean isReviewed = reviewTime != null;

                    return isPublished && isReviewed;
                }
            }
        }
        return false;
    }

    private boolean doesuserhasCoins(AuthInfo auth, Connection conn) throws SQLException {
        String coinCheckSql = "SELECT coin FROM UserRecord WHERE mid = ?";
        try (PreparedStatement coinCheckStmt = conn.prepareStatement(coinCheckSql)) {
            coinCheckStmt.setLong(1, auth.getMid());
            try (ResultSet rs = coinCheckStmt.executeQuery()) {
                if (rs.next() && rs.getInt("coin") > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public VideoRecord getVideoByBV(String bv, Connection conn) {
        String sql = "SELECT * FROM VideoRecord WHERE bv = ?";
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, bv);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    // 构建 VideoRecord 对象并设置属性
                    VideoRecord video = new VideoRecord();
                    video.setBv(resultSet.getString("bv"));
                    video.setTitle(resultSet.getString("title"));
                    video.setOwnerMid(resultSet.getLong("ownerMid"));
                    // 设置其他属性...

                    return video;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace(); // 处理异常，这里简化为打印堆栈跟踪
        }

        return null; // 如果未找到匹配的视频记录
    }

    //删除视频相关记录
    private boolean deleteRelatedRecords(String bv, Connection conn) throws SQLException {
        // 删除 ViewRecord 表中相关记录
        try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM ViewRecord WHERE bv = ?")) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }

        // 删除 DanmuRecord 表中相关记录
        try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM DanmuRecord WHERE bv = ?")) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }

        // 删除 likes 表中相关记录
        try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM likes WHERE BV_liked = ?")) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }

        // 删除 favorites 表中相关记录
        try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM favorites WHERE BV_favorite = ?")) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }

        // 删除 coins 表中相关记录
        try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM coins WHERE BV_coin = ?")) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        }

        // delete watch record in UserInfoResp
        String sql = "UPDATE UserInfoResp SET watched = array_remove(watched, ?) WHERE ? = ANY (watched)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setString(2, bv);
            stmt.executeUpdate();
        }

        return true; //here if no line is deleted, it will excute sucessfully   (should modify if needed)
    }

    // 删除 VideoRecord 视频记录
    private boolean deleteVideoRecord(String bv, Connection conn) throws SQLException {
        String sql = "DELETE FROM VideoRecord WHERE bv = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            int affectedRows = stmt.executeUpdate();// 返回受影响的行数
            return affectedRows > 0;//here if no line is deleted, it will throw exception
        }
    }

}
