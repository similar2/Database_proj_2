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
        UserImpl userimpl = new UserImpl();
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
            if (auth == null || !userimpl.isValidAuth(auth, conn)) {
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
        UserImpl userimpl = new UserImpl();
        try (Connection conn = dataSource.getConnection()) {
            // get video
            VideoRecord video = getVideoByBV(bv, conn);
            //video does exist according to bv, auth is owner/superuser
            if (video == null) {
                return false;
            }
            //verify auth
            if (!userimpl.isValidAuth(auth, conn)) {
                return false;
            } else {
                auth = userimpl.construct_full_authinfo(auth, conn);
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
        UserImpl userimpl = new UserImpl();
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
            if (!userimpl.isValidAuth(auth, conn)) {
                return false;
            } else {
                auth = userimpl.construct_full_authinfo(auth, conn);
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

    private String escapeKeyword(String keyword) {
        return keyword.replace("%", "\\%").replace("_", "\\_");
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
        UserImpl userimpl = new UserImpl();
        List<String> videoBVs = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            if (!userimpl.isValidAuth(auth, conn)) {
//                throw new IllegalArgumentException("Invalid authentication");
                return null;
            }

            String[] keywordArray = keywords.split("\\s+");

            // 构建 SQL 查询
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT bv, calculate_relevance(title, description, ownerName, ARRAY[");
            for (int i = 0; i < keywordArray.length; i++) {
                sqlBuilder.append("?");
                if (i < keywordArray.length - 1) {
                    sqlBuilder.append(", ");
                }
            }
            sqlBuilder.append("]) AS relevance FROM VideoRecord WHERE ");
            for (int i = 0; i < keywordArray.length; i++) {
                sqlBuilder.append("(LOWER(title) LIKE LOWER(?) OR LOWER(description) LIKE LOWER(?) OR LOWER(ownerName) LIKE LOWER(?))");
                if (i < keywordArray.length - 1) {
                    sqlBuilder.append(" OR ");
                }
            }
            sqlBuilder.append(" ORDER BY relevance DESC, (SELECT COUNT(*) FROM ViewRecord WHERE VideoRecord.bv = ViewRecord.bv) DESC LIMIT ? OFFSET ?");

            String sql = sqlBuilder.toString();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                int paramIndex = 1;
                // Set keywords for the calculate_relevance function
                for (String keyword : keywordArray) {
                    String escapedKeyword = escapeKeyword(keyword);
                    stmt.setString(paramIndex++, "%" + escapedKeyword + "%");
                }
                // Set keywords for the WHERE clause
                for (String keyword : keywordArray) {
                    String escapedKeyword = escapeKeyword(keyword);
                    String likePattern = "%" + escapedKeyword + "%";
                    for (int i = 0; i < 3; i++) {
                        stmt.setString(paramIndex++, likePattern);
                    }
                }
                // Set LIMIT and OFFSET
                stmt.setInt(paramIndex++, pageSize);
                stmt.setInt(paramIndex++, (pageNum - 1) * pageSize);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        videoBVs.add(rs.getString("bv"));
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
//                throw new RuntimeException("Database error occurred: " + e.getMessage());
            return null;
        }
        return videoBVs;
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
        UserImpl userimpl = new UserImpl();
        // 验证输入参数
        if (auth == null || bv == null || bv.isEmpty()) {
            return false;
        }

        try (Connection conn = dataSource.getConnection()) {
            // 验证用户身份
            if (!userimpl.isValidAuth(auth, conn) || !isSuperuser(auth, conn)) {
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
        UserImpl userimpl = new UserImpl();
        if (auth == null || bv == null) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            // 检查用户权限和是否有硬币
            if (!userimpl.isValidAuth(auth, conn)) {
//throw new IllegalArgumentException("invalid auth "+auth.getMid()+" "+bv);
                return false;
            }
            AuthInfo REALauth = userimpl.construct_full_authinfo(auth, conn);

            if(!doesuserhasCoins(REALauth, conn)){
//                throw new IllegalArgumentException("no coins "+auth.getMid()+" "+bv);
                return false;
            }
            if(!canUserSearchVideo(REALauth, bv, conn)){
//                throw new IllegalArgumentException("cannot search video "+auth.getMid()+" "+bv);
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
            String coinCheckSql = "SELECT COUNT(*) FROM coins WHERE bv_coin = ? AND mid_coin = ?";
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
        UserImpl userimpl = new UserImpl();
        if (auth == null || bv == null) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            AuthInfo REALauth = userimpl.construct_full_authinfo(auth, conn);

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
            if (!userimpl.isValidAuth(auth, conn)) {
                return false;
            }
            //不能给自己点赞
            if (auth.getMid() == video.getOwnerMid()) {
                return false;
            }
            // 检查用户是否已点赞
            String likeCheckSql = "SELECT COUNT(*) FROM likes WHERE bv_liked = ? AND mid_liked = ?";
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
                        String deleteSql = "DELETE FROM likes WHERE bv_liked = ? AND mid_liked = ?";
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
        UserImpl userimpl = new UserImpl();
        if (auth == null || bv == null) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            AuthInfo REALauth = userimpl.construct_full_authinfo(auth, conn);
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
            if (!userimpl.isValidAuth(auth, conn)) {
                return false;
            } else {
                auth = userimpl.construct_full_authinfo(auth, conn);
            }
            //verify identity
            if (auth.getMid() == video.getOwnerMid()) {
                return false;
            }
            // 检查用户是否已经收藏了视频
            String collectCheckSql = "SELECT COUNT(*) FROM favorites WHERE bv_favorite = ? AND mid_favorite = ?";
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
                    String deleteSql = "DELETE FROM favorites WHERE bv_favorite = ? AND mid_favorite = ?";
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


    /*
    qq, wechat都存在，但是指向不同的用户
    mid,qq,wechat全部为invalid(empty or not found)
     */

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
        String AuthIdentity = "";
        if(auth.getWechat()!=null&&!auth.getWechat().equals("null")) {
            String sql = "SELECT identity FROM UserRecord WHERE wechat = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getWechat());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // when using ENUM type in postgres, it will return a String type in Java.
                        AuthIdentity = rs.getString(1);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (auth.getQq()!=null&&!auth.getQq().equals("null")) {
            String sql = "SELECT identity FROM UserRecord WHERE qq = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getQq());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        AuthIdentity = rs.getString(1);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }else {
            String sql = "SELECT identity FROM UserRecord WHERE mid = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, auth.getMid());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        AuthIdentity = rs.getString(1);

                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        if (AuthIdentity.equals("SUPERUSER")) {
            return true;
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
        if(auth.getWechat()!=null&&!auth.getWechat().equals("null")) {
            String sql = "SELECT coin FROM UserRecord WHERE wechat = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getWechat());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int num = rs.getInt("coin");
                        if(num > 0) {
                            return true;
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (auth.getQq()!=null&&!auth.getQq().equals("null")) {
            String sql = "SELECT coin FROM UserRecord WHERE qq = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getQq());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int num = rs.getInt("coin");
                        if(num > 0) {
                            return true;
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }else {
            if (auth.getMid() != 0) {
                String sql = "SELECT coin FROM UserRecord WHERE mid = ?";
                try (PreparedStatement coinCheckStmt = conn.prepareStatement(sql)) {
                    coinCheckStmt.setLong(1, auth.getMid());
                    try (ResultSet rs = coinCheckStmt.executeQuery()) {
                        if (rs.next()) {
                            int num = rs.getInt("coin");
                            if (num > 0) {
                                return true;
                            }
                        }
                    }

                }catch (SQLException e) {
                    throw new RuntimeException(e);
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
