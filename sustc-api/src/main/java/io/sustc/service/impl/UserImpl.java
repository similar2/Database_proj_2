package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.util.*;


@Service
@Slf4j
public class UserImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        if (Objects.equals(req.getPassword(), null)) {
            return -1;
        }
        if (Objects.equals(req.getName(), null)) {
            return -1;
        }
        if (Objects.equals(req.getSex(), null)) {
            return -1;
        }
        if (Objects.equals(req.getBirthday(), null) || !req.getBirthday().matches("\\d{1,2}月\\d{1,2}日")) {
            return -1;
        }
        if (req.getQq() != null) {
            String sql1 = "SELECT COUNT(*) FROM UserRecord WHERE qq = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt1 = conn.prepareStatement(sql1)) {
                stmt1.setString(1, req.getQq());
                try (ResultSet rs = stmt1.executeQuery()) {
                    if (rs.next()) {
                        // rowCount is the answer of COUNT(*)
                        int rowCount = rs.getInt(1);
                        if (rowCount != 0) {
                            return -1;
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        if (req.getWechat() != null) {
            String sql2 = "SELECT COUNT(*) FROM UserRecord WHERE wechat = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt2 = conn.prepareStatement(sql2)) {
                stmt2.setString(1, req.getWechat());
                try (ResultSet rs = stmt2.executeQuery()) {
                    if (rs.next()) {
                        // rowCount is the answer of COUNT(*)
                        int rowCount = rs.getInt(1);
                        if (rowCount != 0) {
                            return -1;
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        long mid = 0;
        String max = "SELECT MAX(mid) FROM UserRecord";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement secondStmt = conn.prepareStatement(max)) {
            try (ResultSet rs = secondStmt.executeQuery()) {
                if (rs.next()) {
                    Random rand = new Random();
                    mid = rs.getLong(1) + rand.nextLong(100);
                }
            }
            String sql_insert = "INSERT INTO UserRecord (mid, name, sex, birthday, level, sign, following, identity, password, qq, wechat, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement thirdStmt = conn.prepareStatement(sql_insert)) {
                thirdStmt.setLong(1, mid);
                thirdStmt.setString(2, req.getName());
                // Enum values in Java have a name() method that returns the name of the enum constant as a String
                String sex = req.getSex().name();
                if (sex.equals("男") || sex.equals("女")) {
                    thirdStmt.setString(3, req.getSex().name());
                } else {
                    thirdStmt.setString(3, "保密");
                }
                // the format of birthday has been changed into standard
                thirdStmt.setString(4, req.getBirthday());
                String birthday = req.getBirthday();
                String[] parts;
                int month = 0;
                int day = 0;

                int DefaultYear = Calendar.getInstance().get(Calendar.YEAR); // 默认年份，例如当前年份

                // 通过正则表达式匹配不同的格式
                if (birthday.matches("\\d{1,2}月\\d{1,2}日")) {
                    parts = birthday.split("月");
                    month = Integer.parseInt(parts[0]);
                    day = Integer.parseInt(parts[1].replace("日", ""));
                } else if (birthday.matches("\\d{1,2}-\\d{1,2}")) {
                    parts = birthday.split("-");
                    month = Integer.parseInt(parts[0]);
                    day = Integer.parseInt(parts[1]);
                }

                // 检查日期是否在日历上有效
                boolean isValidDate = false;
                if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.set(Calendar.YEAR, DefaultYear);
                    calendar.set(Calendar.MONTH, month - 1); // Calendar中月份从0开始
                    calendar.set(Calendar.DAY_OF_MONTH, day);
                    isValidDate = day == calendar.get(Calendar.DAY_OF_MONTH);
                }

                // 如果月份和日期有效，则设置日期，否则设置为 NULL
                if (isValidDate) {
                    String completeBirthday = DefaultYear + "-" + month + "-" + day;
                    thirdStmt.setDate(4, Date.valueOf(completeBirthday));
                } else {
                    return -1;
                }

                // level is also Enum type
                thirdStmt.setInt(5, 0);
                thirdStmt.setString(6, "");
                thirdStmt.setArray(7, conn.createArrayOf("bigint", new Long[0]));
                thirdStmt.setString(8, "USER");
                thirdStmt.setString(9, req.getPassword());
                thirdStmt.setString(10, req.getQq());
                thirdStmt.setString(11, req.getWechat());
                thirdStmt.setObject(12, false);

                int rowsAffected = thirdStmt.executeUpdate();
                // 如果需要获取插入的行数，可以使用 rowsAffected 变量
                if (rowsAffected == 1) {
                    return mid;
                } else {
                    return -1;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        String sql = "DELETE FROM UserRecord WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            if (isValidAuth(auth, conn) && isValidMid(mid, conn) && isAuthorized(auth, mid, conn)) {
                stmt.setLong(1, mid);
                // perform the deletion operation
                int rowsAffected = stmt.executeUpdate();
                if (rowsAffected == 1) {
                    return true;
                }
            } else {
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private boolean isValidMid(long mid, Connection conn) {
        String sql = "SELECT COUNT(*) FROM UserRecord WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    // rowCount is the answer of COUNT(*)
                    int rowCount = rs.getInt(1);
                    return rowCount == 1;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public boolean isValidAuth(AuthInfo auth, Connection conn) {
        // judge qq and wechat firstly
        if (auth.getWechat() != null) {
            String sql = "SELECT COUNT(mid) FROM UserRecord WHERE wechat = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getWechat());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // valid--not can't-find, not point-to-different-user
                        int rowCount = rs.getInt(1);
                        return rowCount == 1;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (auth.getQq() != null) {
            String sql = "SELECT COUNT(mid) FROM UserRecord WHERE qq = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getQq());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // valid--not can't-find, not point-to-different-user
                        int rowCount = rs.getInt(1);
                        return rowCount == 1;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (auth.getMid() != 0) {
                String sql = "SELECT COUNT(*) FROM UserRecord WHERE mid = ? AND password = ?";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setLong(1, auth.getMid());
                    stmt.setString(2, auth.getPassword());
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            int rowCount = rs.getInt(1);
                            return rowCount == 1;
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }

    private boolean isAuthorized(AuthInfo auth, long mid, Connection conn) {
        String sql = "SELECT identity FROM UserRecord WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    // when using ENUM type in postgres, it will return a String type in Java.
                    String AuthIdentity = rs.getString(1);
                    if (AuthIdentity.equals("USER")) {
                        if (auth.getMid() == mid) {
                            return true;
                        }
                    } else {
                        stmt.setLong(1, mid);
                        String MidIdentity = "";
                        // Create a new PreparedStatement for the second query
                        try (PreparedStatement secondStmt = conn.prepareStatement(sql)) {
                            secondStmt.setLong(1, mid);
                            try (ResultSet midRs = secondStmt.executeQuery()) {
                                if (midRs.next()) {
                                    MidIdentity = midRs.getString(1);
                                }
                            }
                            if (auth.getMid() == mid) {
                                return true;
                            } else if (MidIdentity.equals("USER")) {
                                return true;
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        String sql = "SELECT COUNT(*) FROM UserRecord WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            if (!isValidAuth(auth, conn)) {
                return false;
            }
            stmt.setLong(1, followeeMid);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    // valid--not can't-find, not point-to-different-user
                    int rowCount = rs.getInt(1);
                    if (rowCount != 1) {
                        return false;
                    }
                }
            }
            String sql2 = "SELECT following FROM UserRecord WHERE mid = ?";
            try (PreparedStatement secondStmt = conn.prepareStatement(sql2)) {
                secondStmt.setLong(1, auth.getMid());
                try (ResultSet rs = secondStmt.executeQuery()) {
                    if (rs.next()) {
                        Array followingArray = rs.getArray("following");
                        if (followingArray != null) {
                            ArrayList<Long> followees = new ArrayList<>(Arrays.asList((Long[]) followingArray.getArray()));
                            if (followees.contains(followeeMid)) {
                                // If already following, unfollow the user
                                followees.remove(followeeMid);
                                if (update(conn, auth.getMid(), followees)) {
                                    return false;
                                }
                            } else {
                                // If not following, follow the user
                                followees.add(followeeMid);
                                if (update(conn, auth.getMid(), followees)) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private boolean update(Connection conn, long mid, ArrayList<Long> followees) {
        Long[] followingArray = followees.toArray(new Long[0]);
        String sql = "UPDATE UserRecord SET following = ? WHERE mid = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            Array following = conn.createArrayOf("BIGINT", followingArray);
            stmt.setArray(1, following);
            stmt.setLong(2, mid);
            int rowsAffected = stmt.executeUpdate();
            if (rowsAffected == 1) {
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        String sql0 = "SELECT COUNT(*) FROM UserInfoResp WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt0 = conn.prepareStatement(sql0)) {
            stmt0.setLong(1, mid);
            try (ResultSet rs = stmt0.executeQuery()) {
                if (rs.next()) {
                    int rowCount = rs.getInt(1);
                    if (rowCount == 0) {
                        return null;
                    }
                }
            }
            int coin = 0;
            long[] following = new long[0];
            long[] follower = new long[0];
            String[] watched = new String[0];
            String[] liked;
            String[] collected;
            String[] posted;
            String sql = "SELECT coin, following, follower, watched FROM UserInfoResp WHERE mid = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, mid);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        coin = rs.getInt("coin");
                        Array followingArray = rs.getArray("following");
                        Long[] objectArray1 = (Long[]) followingArray.getArray();
                        following = new long[objectArray1.length];

                        for (int i = 0; i < objectArray1.length; i++) {
                            following[i] = objectArray1[i];
                        }
                        Array followerArray = rs.getArray("follower");
                        Long[] objectArray2 = (Long[]) followerArray.getArray();
                        follower = new long[objectArray2.length];

                        for (int i = 0; i < objectArray2.length; i++) {
                            follower[i] = objectArray2[i];
                        }
                        Array watchedArray = rs.getArray("watched");
                        watched = (String[]) watchedArray.getArray();
                    }
                }
                String like_sql = "SELECT bv_liked FROM likes WHERE mid_liked = ?";
                try (PreparedStatement stmt1 = conn.prepareStatement(like_sql)) {
                    stmt1.setLong(1, mid);
                    try (ResultSet rs = stmt1.executeQuery()) {
                        List<String> likedArray = new ArrayList<>();
                        while (rs.next()) {
                            likedArray.add(rs.getString("bv_liked"));
                        }
                        liked = likedArray.toArray(new String[0]);
                    }
                }
                String collect_sql = "SELECT bv_favorite FROM favorites WHERE mid_favorite = ?";
                try (PreparedStatement stmt2 = conn.prepareStatement(collect_sql)) {
                    stmt2.setLong(1, mid);
                    try (ResultSet rs = stmt2.executeQuery()) {
                        List<String> collectedArray = new ArrayList<>();
                        while (rs.next()) {
                            collectedArray.add(rs.getString("bv_favorite"));
                        }
                        collected = collectedArray.toArray(new String[0]);
                    }
                }
                String post_sql = "SELECT bv FROM VideoRecord WHERE ownermid = ?";
                try (PreparedStatement stmt3 = conn.prepareStatement(post_sql)) {
                    stmt3.setLong(1, mid);
                    try (ResultSet rs = stmt3.executeQuery()) {
                        List<String> postedArray = new ArrayList<>();
                        while (rs.next()) {
                            postedArray.add(rs.getString("bv"));
                        }
                        posted = postedArray.toArray(new String[0]);

                    }
                }
            }
            return new UserInfoResp(mid, coin, following, follower, watched, liked, collected, posted);
            // Return null if no user is found
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
