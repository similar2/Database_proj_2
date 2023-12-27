package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
                    "ON VideoRecord.bv = finish_avg.bv), grade AS(" +
                    "SELECT v.bv, " +
                    "COALESCE(l.count, 0)::float / NULLIF(view.count, 0) AS ratio_like, " +
                    "COALESCE(c.count, 0)::float / NULLIF(view.count, 0) AS ratio_coin, " +
                    "COALESCE(f.count, 0)::float / NULLIF(view.count, 0) AS ratio_fav, " +
                    "COALESCE(d.count, 0)::float / NULLIF(view.count, 0) AS avg_danmu, " +
                    "COALESCE(p.percent_finish, 0) AS avg_finish " +
                    "FROM VideoRecord v " +
                    "LEFT JOIN view on v.bv = view.bv " +
                    "LEFT JOIN like_num l ON v.bv = l.bv " +
                    "LEFT JOIN coin_num c ON v.bv = c.bv " +
                    "LEFT JOIN favourite_num f ON v.bv = c.bv " +
                    "LEFT JOIN danmu_num d ON v.bv =d.bv " +
                    "LEFT JOIN finish_percent p on v.bv = p.bv) " +
                    "SELECT bv, ratio_like+ratio_coin+ratio_fav+avg_danmu+avg_finish AS final " +
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
        try (Connection conn = dataSource.getConnection()) {
            if (userimpl.isValidAuth(auth, conn) && pageSize > 0 && pageNum > 0) {
                List<Long> friends = findFriends(auth);
                if (friends.isEmpty()) {
                    return generalRecommendations(pageSize, pageNum);
                }
                String sql0 = "SELECT DISTINCT w.bv " +
                        "FROM Watched w " +
                        "WHERE w.mid IN (" + String.join(",", Collections.nCopies(friends.size(), "?")) + ") " +
                        "AND w.bv NOT IN (SELECT bv FROM Watched WHERE mid = ?) ";
                try (PreparedStatement stmt = conn.prepareStatement(sql0)) {
                    for (Long friend : friends) {
                        stmt.setLong(1, friend);
                    }
                    stmt.setLong(2, auth.getMid());
                    try (ResultSet rs = stmt.executeQuery()) {
                        List<String> interest = new ArrayList<>();
                        while (rs.next()) {
                            interest.add(rs.getString("bv"));
                        }
                        if (interest.isEmpty()) {
                            return generalRecommendations(pageSize, pageNum);
                        }
                    }
                }
                String sql = "SELECT DISTINCT w.bv " +
                        "FROM Watched w " +
                        "WHERE w.mid IN (" + String.join(",", Collections.nCopies(friends.size(), "?")) + ") " +
                        "AND w.bv NOT IN (SELECT bv FROM Watched WHERE mid = ?) " +
                        "ORDER BY " +
                        "   (SELECT COUNT(DISTINCT mid) FROM Watched WHERE bv = w.bv) DESC, " +
                        "   (SELECT u.level FROM UserInfoResp u WHERE u.mid = w.mid) DESC, " +
                        "   w.public_time DESC " +
                        "LIMIT ? OFFSET ?";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    // 对于friends中取出的每一个元素 在数据库中搜寻这些mid对应的watched表中的所有行，并将它们按照BV分类计算每一类的数目
                    for (Long friend : friends) {
                        stmt.setLong(1, friend);
                    }
                    stmt.setLong(2, auth.getMid()); // User's own mid
                    stmt.setInt(3, pageSize);
                    stmt.setInt(4, (pageNum - 1) * pageSize);

                    // Execute the query and retrieve the results
                    try (ResultSet rs = stmt.executeQuery()) {
                        List<String> recommendedVideos = new ArrayList<>();
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
        return null;
    }

    private List<Long> findFriends(AuthInfo auth) {
        List<Long> friends = new ArrayList<>();
        String sql = "SELECT DISTINCT friend_mid " +
                "FROM (SELECT following AS friend_mid " +
                "FROM UserInfoResp WHERE mid = ? " +
                "UNION SELECT follower AS friend_mid " +
                "FROM UserInfoResp WHERE mid = ?) AS friends " +
                "WHERE friend_mid IN (SELECT following FROM UserInfoResp WHERE mid = ?) " +
                "AND friend_mid IN (SELECT follower FROM UserInfoResp WHERE mid = ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            // Find users who are both followers and followees of the current user
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, auth.getMid());
            stmt.setLong(3, auth.getMid());
            stmt.setLong(4, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                friends.add(rs.getLong("friend_mid"));
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
                        "   (SELECT COUNT(DISTINCT mid) FROM UserRecord WHERE mid = ur.mid) DESC, " +
                        "   ur.level DESC " +
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
                while (rs.next()) {
                    followings.add(rs.getLong("following"));
                }
                return followings;
            }
        }
    }
}
