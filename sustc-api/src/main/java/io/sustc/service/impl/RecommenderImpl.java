package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
         // pageSize and pageNum are parameters used for pagination.
         // The purpose of these two parameters is to allow clients to fetch large data sets in batches
        if(pageSize>0 && pageNum>0) {
            String sql = "WITH view AS(" +
                    "SELECT video.bv, COUNT(vr.mid) AS count FROM VideoRecord video " +
                    "LEFT JOIN ViewRecord vr ON video.bv = vr.bv " +
                    "GROUP BY video.bv), like_num AS(" +
                    "SELECT video.bv, COUNT(*) AS count " +
                    "FROM VideoRecord video " +
                    "LEFT JOIN likes l ON l.bv_liked = video.bv " +
                    "GROUP BY video.bv), coin_num AS(" +
                    "SELECT video.bv, COUNT(*) AS count " +
                    "FROM VideoRecord video " +
                    "LEFT JOIN coins c ON c.bv_coin = video.bv " +
                    "GROUP BY video.bv), favourite_num AS(" +
                    "SELECT video.bv, COUNT(*) AS count " +
                    "FROM VideoRecord video " +
                    "LEFT JOIN favorites f ON f.bv_favorite = video.bv " +
                    "GROUP BY video.bv), danmu_num AS(" +
                    "SELECT video.bv, COUNT(*) AS count " +
                    "FROM VideoRecord video " +
                    "LEFT JOIN DanmuRecord DR on video.bv = DR.bv " +
                    "GROUP BY video.bv), finish_avg AS (" +
                    "SELECT bv, AVG(timestamp) AS avg_finish " +
                    "FROM ViewRecord " +
                    "GROUP BY bv), finish_percent AS (" +
                    "SELECT VideoRecord.bv, " +
                    "COALESCE(finish_avg.avg_finish, 0)::float / NULLIF(duration, 0) AS percent_finish " +
                    "FROM VideoRecord " +
                    "LEFT JOIN finish_avg " +
                    "ON VideoRecord.bv = finish_avg.bv), " +
                    "grade AS(" +
                    "SELECT view.bv, " +
                    "(SELECT COALESCE(like_num.count, 0)::float / NULLIF(view.count, 0) FROM like_num WHERE like_num.bv = view.bv) AS like_ratio, " +
                    "(SELECT COALESCE(coin_num.count, 0)::float / NULLIF(view.count, 0) FROM coin_num WHERE coin_num.bv = view.bv) AS coin_ratio, " +
                    "(SELECT COALESCE(favourite_num.count, 0)::float / NULLIF(view.count, 0) FROM favourite_num WHERE favourite_num.bv = view.bv) AS favourite_ratio, " +
                    "(SELECT COALESCE(danmu_num.count, 0)::float / NULLIF(view.count, 0) FROM danmu_num WHERE danmu_num.bv = view.bv) AS danmu_ratio, " +
                    "(SELECT COALESCE(finish_percent.percent_finish, 0) FROM finish_percent WHERE finish_percent.bv = view.bv) AS avg_finish FROM view) " +
                    "SELECT bv, like_ratio+coin_ratio+favourite_ratio+danmu_ratio+avg_finish AS final " +
                    "FROM grade " +
                    "ORDER BY final DESC " +
                    "LIMIT ? OFFSET ?";
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
                List<Long> friends = findFriends(auth);
                if (friends.isEmpty()) {
                    return generalRecommendations(pageSize, pageNum);
                }
                String sql0 = "WITH friends AS (" +
                        "SELECT UNNEST(ARRAY (SELECT UNNEST(ARRAY_AGG(following)) " +
                        "INTERSECT " +
                        "SELECT UNNEST(ARRAY_AGG(follower)))) AS mid " +
                        "FROM UserInfoResp WHERE mid = ?)," +
                        "already AS (" +
                        "SELECT bv FROM ViewRecord WHERE mid = ?) " +
                        "SELECT COUNT(vr.bv) FROM ViewRecord vr " +
                        "JOIN friends f ON vr.mid = f.mid " +
                        "WHERE vr.bv NOT IN (SELECT bv FROM already)";
                try (PreparedStatement stmt = conn.prepareStatement(sql0)) {
                    stmt.setLong(1, auth.getMid());
                    stmt.setLong(2, auth.getMid());
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            int rowCount = rs.getInt(1);
                            if (rowCount == 0) {
                                return generalRecommendations(pageSize, pageNum);
                            }
                        }
                    }
                }
                String sql = "WITH friends AS (SELECT ARRAY (" +
                        "SELECT UNNEST(ARRAY_AGG(following))" +
                        "INTERSECT SELECT UNNEST(ARRAY_AGG(follower))) AS friends " +
                        "FROM UserInfoResp WHERE mid = ?), " +
                        "already AS (" +
                        "SELECT bv FROM ViewRecord WHERE mid = ?) " +
                        "SELECT count.bv FROM (" +
                        "SELECT ViewRecord.bv, COUNT(ViewRecord.mid) AS c " +
                        "FROM ViewRecord JOIN friends ON true " +
                        "WHERE ViewRecord.mid = ANY (friends.friends) " +
                        "AND ViewRecord.bv NOT IN (SELECT bv FROM already) " +
                        "GROUP BY ViewRecord.bv) count " +
                        "LEFT JOIN VideoRecord ON VideoRecord.bv = count.bv ORDER BY count.c DESC, (" +
                        "SELECT UserRecord.level FROM UserRecord " +
                        "WHERE UserRecord.mid = VideoRecord.ownerMid) DESC, " +
                        "VideoRecord.publictime DESC " +
                        "LIMIT ? OFFSET ?";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    // 对于friends中取出的每一个元素 在数据库中搜寻这些mid对应的view表中的所有行，并将它们按照BV分类计算每一类的数目
                    stmt.setLong(1, auth.getMid()); // User's own mid
                    stmt.setLong(2, auth.getMid());
                    stmt.setInt(3, pageSize);
                    stmt.setInt(4, (pageNum - 1) * pageSize);

                    // Execute the query and retrieve the results
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            recommendedVideos.add(rs.getString("bv"));
                        }
                        return recommendedVideos;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return recommendedVideos;
    }

    private List<Long> findFriends(AuthInfo auth) {
        List<Long> friends = new ArrayList<>();
        String sql = "SELECT ARRAY(" +
                "SELECT UNNEST(ARRAY_AGG(following)) " +
                "INTERSECT " +
                "SELECT UNNEST(ARRAY_AGG(follower))) " +
                "FROM UserInfoResp " +
                "WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    // Retrieve the SQL array
                    Array friendArray = rs.getArray(1);
                    // Convert the SQL array to a Java array
                    Long[] friendArrayData = (Long[]) friendArray.getArray();
                    // Convert the array to a List
                    friends = Arrays.asList(friendArrayData);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return friends;
    }


    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        UserImpl userImpl = new UserImpl();
        try (Connection conn = dataSource.getConnection()) {
            if (userImpl.isValidAuth(auth, conn) && pageSize > 0 && pageNum > 0) {
                List<Long> currentFollowings = getFollowings(auth, conn);
                // Check if the user has any followings
                if (currentFollowings.isEmpty()) {
                    return Collections.emptyList();
                }
                // Construct the SQL query to find recommended friends
                String sql = "SELECT DISTINCT ur.mid " +
                        "FROM UserRecord ur " +
                        "JOIN UserRecord current_user_ur ON ur.mid = current_user_ur.following " +
                        "LEFT JOIN UserRecord following_ur ON following_ur.mid = current_user_ur.following " +
                        "AND following_ur.follower = ur.follower " +
                        "WHERE current_user_ur.mid = ? " +
                        "AND following_ur.mid IS NULL " +
                        "ORDER BY " +
                        "(SELECT COUNT(DISTINCT mid) FROM UserRecord WHERE mid = ur.mid) DESC, " +
                        "ur.level DESC " +
                        "LIMIT ? OFFSET ?";

                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setLong(1, auth.getMid()); // Current user's mid
                    stmt.setInt(2, pageSize);
                    stmt.setInt(3, (pageNum - 1) * pageSize);

                    // Execute the query and retrieve the results
                    try (ResultSet rs = stmt.executeQuery()) {
                        List<Long> recommendedFriends = new ArrayList<>();
                        while (rs.next()) {
                            recommendedFriends.add(rs.getLong("mid"));
                        }
                        return recommendedFriends;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Collections.emptyList();
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
}