package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.lang.model.util.ElementScanner6;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@Service
@Slf4j

public class RecommenderImpl implements RecommenderService {
    @Autowired
    private DataSource dataSource;

    @Override
    public List<String> recommendNextVideo(String bv) {
        String sqlExist = "SELECT COUNT(*) FROM ViewRecord WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmtExist = conn.prepareStatement(sqlExist)) {
            stmtExist.setString(1, bv);
            try (ResultSet rs = stmtExist.executeQuery()) {
                if (rs.next()) {
                    int rowCount = rs.getInt(1);
                    if (rowCount == 0) {
                        return null;
                    }
                }
            }
            String sql = "WITH target_watched_user AS(SELECT * FROM ViewRecord WHERE bv = ?) " +
                    "SELECT ViewRecord.bv FROM ViewRecord JOIN target_watched_user " +
                    "ON ViewRecord.mid = target_watched_user.mid " +
                    "WHERE ViewRecord.bv != target_watched_user.bv " +
                    "GROUP BY ViewRecord.bv " +
                    "ORDER BY COUNT(*) DESC " +
                    "LIMIT 5;";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, bv);
                try (ResultSet rs = stmt.executeQuery()) {
                    List<String> recommendedVideos = new ArrayList<>();
                    while (rs.next()) {
                        recommendedVideos.add(rs.getString("bv"));
                    }
                    return recommendedVideos;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        // pageSize and pageNum are parameters used for pagination.
        // The purpose of these two parameters is to allow clients to fetch large data sets in batches
        if (pageSize > 0 && pageNum > 0) {
            String sql = """
                        WITH view AS(
                        SELECT video.bv, COUNT(vr.mid) AS count 
                        FROM VideoRecord video 
                        LEFT JOIN ViewRecord vr ON video.bv = vr.bv 
                        GROUP BY video.bv
                    ), 
                    like_num AS(
                        SELECT video.bv, COUNT(*) AS count 
                        FROM VideoRecord video 
                        LEFT JOIN likes l ON l.bv_liked = video.bv 
                        GROUP BY video.bv
                    ), 
                    coin_num AS(
                        SELECT video.bv, COUNT(*) AS count 
                        FROM VideoRecord video 
                        LEFT JOIN coins c ON c.bv_coin = video.bv 
                        GROUP BY video.bv
                    ), 
                    favourite_num AS(
                        SELECT video.bv, COUNT(*) AS count 
                        FROM VideoRecord video 
                        LEFT JOIN favorites f ON f.bv_favorite = video.bv 
                        GROUP BY video.bv
                    ), 
                    danmu_num AS(
                        SELECT video.bv, COUNT(*) AS count 
                        FROM VideoRecord video 
                        LEFT JOIN DanmuRecord DR on video.bv = DR.bv 
                        GROUP BY video.bv
                    ), 
                    finish_avg AS (
                        SELECT bv, AVG(timestamp) AS avg_finish 
                        FROM ViewRecord 
                        GROUP BY bv
                    ), 
                    finish_percent AS (
                        SELECT VideoRecord.bv, COALESCE(finish_avg.avg_finish, 0)::float / NULLIF(duration, 0) AS percent_finish 
                        FROM VideoRecord 
                        LEFT JOIN finish_avg ON VideoRecord.bv = finish_avg.bv
                    ), 
                    grade AS(
                        SELECT view.bv, 
                        (SELECT COALESCE(like_num.count, 0)::float / NULLIF(view.count, 0) FROM like_num WHERE like_num.bv = view.bv) AS like_ratio, 
                        (SELECT COALESCE(coin_num.count, 0)::float / NULLIF(view.count, 0) FROM coin_num WHERE coin_num.bv = view.bv) AS coin_ratio, 
                        (SELECT COALESCE(favourite_num.count, 0)::float / NULLIF(view.count, 0) FROM favourite_num WHERE favourite_num.bv = view.bv) AS favourite_ratio, 
                        (SELECT COALESCE(danmu_num.count, 0)::float / NULLIF(view.count, 0) FROM danmu_num WHERE danmu_num.bv = view.bv) AS danmu_ratio, 
                        (SELECT COALESCE(finish_percent.percent_finish, 0) FROM finish_percent WHERE finish_percent.bv = view.bv) AS avg_finish 
                        FROM view
                    ) 
                    SELECT bv, like_ratio+coin_ratio+favourite_ratio+danmu_ratio+avg_finish AS final 
                    FROM grade 
                    ORDER BY final DESC 
                    LIMIT ? OFFSET ?;

                                               """;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                // pageSize represents the number of data entries per page, which is the size of the result set returned for each query.
                stmt.setInt(1, pageSize);
                // pageNum indicates the number of pages requested, i.e., the data you want to get is the data on the first page.
                stmt.setInt(2, (pageNum - 1) * pageSize);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    List<String> recommendations = new ArrayList<>();
                    while (resultSet.next()) {
                        String bv = resultSet.getString("bv");
                        // Add bv to recommendations list
                        recommendations.add(bv);
                    }
                    return recommendations;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }


    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        UserImpl userimpl = new UserImpl();
        List<String> recommendedVideos = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            if (userimpl.isValidAuth(auth, conn) && pageSize > 0 && pageNum > 0) {
                auth = construct_full_authinfo(auth, conn);
                String sql = """
                              WITH friends AS (
                            SELECT UNNEST(following) AS mid
                            FROM UserInfoResp
                            WHERE mid = ?
                            INTERSECT
                            SELECT UNNEST(follower)
                            FROM UserInfoResp
                            WHERE mid = ?
                        ),
                        already AS (
                            SELECT bv
                            FROM ViewRecord
                            WHERE mid = ?
                        ),
                        counted_videos AS (
                            SELECT vr.bv, COUNT(vr.mid) AS c
                            FROM ViewRecord vr
                            JOIN friends f ON vr.mid = f.mid
                            LEFT JOIN already a ON vr.bv = a.bv
                            WHERE a.bv IS NULL
                            GROUP BY vr.bv
                        )
                        SELECT cv.bv
                        FROM counted_videos cv
                        LEFT JOIN VideoRecord vr ON vr.bv = cv.bv
                        LEFT JOIN UserRecord ur ON ur.mid = vr.ownerMid
                        ORDER BY cv.c DESC, ur.level DESC, vr.publictime DESC
                                        
                               LIMIT ? OFFSET ?;
                                    
                              """;
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    // 对于friends中取出的每一个元素 在数据库中搜寻这些mid对应的view表中的所有行，并将它们按照BV分类计算每一类的数目
                    stmt.setLong(1, auth.getMid()); // User's own mid
                    stmt.setLong(2, auth.getMid());
                    stmt.setLong(3, auth.getMid());
                    stmt.setInt(4, pageSize);
                    stmt.setInt(5, (pageNum - 1) * pageSize);

                    // Execute the query and retrieve the results
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {  // 检查结果集是否为空
                            rs.close();
                            return generalRecommendations(pageSize, pageNum);
                        } else {
                            do {
                                recommendedVideos.add(rs.getString("bv"));
                            } while (rs.next());
                            rs.close();
                            return recommendedVideos;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return recommendedVideos;
    }

//    private List<Long> findFriends(AuthInfo auth) {
//        List<Long> friends = new ArrayList<>();
//        String sql = """
//                                            SELECT following AS mid
//                                            FROM UserInfoResp
//                                            WHERE mid = ?
//                                            INTERSECT
//                                            SELECT follower
//                                            FROM UserInfoResp
//                                            WHERE mid = ?
//                """;
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.setLong(1, auth.getMid());
//            try (ResultSet rs = stmt.executeQuery()) {
//                if (rs.next()) {
//                    // Retrieve the SQL array
//                    Array friendArray = rs.getArray(1);
//                    // Convert the SQL array to a Java array
//                    Long[] friendArrayData = (Long[]) friendArray.getArray();
//                    // Convert the array to a List
//                    friends = Arrays.asList(friendArrayData);
//                }
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        return friends;
//    }


    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        // 验证输入参数
        if (auth == null || pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        UserImpl userImpl = new UserImpl();
        try (Connection conn = dataSource.getConnection()) {
            if (!userImpl.isValidAuth(auth, conn)) {
                return Collections.emptyList();
            } else {
                auth = construct_full_authinfo(auth, conn);
            }

            // 构建 SQL 查询
            String sql = """
            
                           SELECT ur.mid, COUNT(DISTINCT f) AS common_followings, ur.level
                    FROM UserRecord ur
                             JOIN UserInfoResp ui ON ur.mid = ui.mid
                             CROSS JOIN UNNEST(ui.following) AS f
                             JOIN UserInfoResp cur_ui ON cur_ui.mid = ?
                             LEFT JOIN UNNEST(cur_ui.following) AS cf ON f = cf
                    WHERE ur.mid <> ?
                      AND cf IS NULL
                    GROUP BY ur.mid, ur.level
                    ORDER BY common_followings DESC, ur.level DESC, ur.mid ASC
                                        LIMIT ? OFFSET ?;
                          
                           """;

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                // 设置 SQL 查询参数
                stmt.setLong(1, auth.getMid()); // 设置当前用户的 mid
                stmt.setLong(2, auth.getMid()); // 重复设置，用于 WHERE 子句
                stmt.setInt(3, pageSize);       // 设置 LIMIT 参数
                stmt.setInt(4, (pageNum - 1) * pageSize); // 设置 OFFSET 参数


                // 执行查询并处理结果
                try (ResultSet rs = stmt.executeQuery()) {
                    List<Long> recommendedFriends = new ArrayList<>();
                    while (rs.next()) {
                        recommendedFriends.add(rs.getLong("mid"));
                    }
                    return recommendedFriends;
                }
            }
        } catch (SQLException e) {
            // 异常处理
            throw new RuntimeException("Database error occurred", e);
        }
    }

    // Helper method to get the current user's followings
    private List<Long> getFollowings(AuthInfo auth, Connection conn) throws SQLException {
        String sql = "SELECT following FROM UserRecord WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            try (ResultSet rs = stmt.executeQuery()) {
                List<Long> followings = new ArrayList<>();
                if (rs.next()) {
                    Array followingArray = rs.getArray("following");
                    Long[] followingArrayData = (Long[]) followingArray.getArray();
                    followings = Arrays.asList(followingArrayData);
                }
                return followings;
            }
        }
    }

    public AuthInfo construct_full_authinfo(AuthInfo authInfo, Connection conn) {
        String sql = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Determine the query based on provided info
            if (authInfo.getQq() != null) {
                sql = "SELECT * FROM authinfo WHERE qq = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, authInfo.getQq());
            } else if (authInfo.getWechat() != null) {
                sql = "SELECT * FROM authinfo WHERE wechat = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, authInfo.getWechat());
            } else if (authInfo.getMid() != 0) {
                sql = "SELECT * FROM authinfo WHERE mid = ?";
                stmt = conn.prepareStatement(sql);
                stmt.setLong(1, authInfo.getMid());
            } else {
                // Handle case where no identifying information is provided
                return null; // or throw an exception
            }

            // Execute the query
            rs = stmt.executeQuery();
            if (rs.next()) {
                // Construct a new AuthInfo object from the ResultSet
                return AuthInfo.builder()
                        .mid(rs.getLong("mid"))
                        .password(rs.getString("password"))
                        .qq(rs.getString("qq"))
                        .wechat(rs.getString("wechat"))
                        .build();
            } else {
                return null; // or handle case where no record is found
            }
        } catch (SQLException e) {
            // Handle SQL exception
            e.printStackTrace();
            return null;
        } finally {
            //
        }
    }

}