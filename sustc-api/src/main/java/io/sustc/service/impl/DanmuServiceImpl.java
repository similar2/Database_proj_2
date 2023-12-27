package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {


        String sql_danmu = "INSERT INTO DanmuRecord (bv, mid, time, content, postTime, likedBy) VALUES (?, ?, ?, ?, ?, ?) RETURNING danmu_id;";
        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        UserImpl userimpl = new UserImpl();
        try (Connection conn = dataSource.getConnection()) {
            if (!userimpl.isValidAuth(auth, conn) || !is_valid_bv(bv, conn) || !is_valid_content(content) || !is_valid_video(bv, auth, now, conn)) {
                return -1;
            }
            try (PreparedStatement stmt = conn.prepareStatement(sql_danmu)) {
                stmt.setString(1, bv);
                stmt.setLong(2, auth.getMid());
                stmt.setFloat(3, time);
                stmt.setString(4, content);
                stmt.setTimestamp(5, now); // setting postTime to null
                stmt.setNull(6, Types.ARRAY);     // setting likedBy to null

                // Execute the statement and get generated keys
                ResultSet generatedKeys = stmt.executeQuery(); // Use executeQuery for returning values
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1); // Return the generated danmu_id
                } else {
                    return -1;
                }
            }
        } catch (SQLException e) {
            return -1;
        }
        // Consider what to return here if necessary
    }


    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {

        if (timeEnd <= timeStart || timeStart < 0) {
            return null;
        }


        List<Long> ans = new ArrayList<>();
        String sql_no_duplicated = """
                SELECT MIN(d.danmu_id),time AS danmu_id
                FROM danmurecord d
                         INNER JOIN (SELECT content, MIN(time) AS min_time
                                     FROM danmurecord
                                     WHERE bv = ?
                                       AND time <= ?
                                       AND time >= ?
                                     GROUP BY content) sub ON sub.content = d.content AND sub.min_time = d.time
                GROUP BY d.content,time
                order by time;
                """;

        String sql_duplicate = """
                select danmu_id
                from danmurecord
                where time <= ?
                  and time >=?
                  and bv =?
                  order by time;""";
        String sql_video = """
                select duration,publictime
                from videorecord
                where bv = ?;
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql_video)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            int duration;
            Timestamp publicTime;
            if (resultSet.next()) {
                duration = resultSet.getInt(1);
                publicTime = resultSet.getTimestamp(2);
            } else {
                return null;
            }
            if (publicTime != null) {
                if (Timestamp.valueOf(LocalDateTime.now()).before(publicTime)) {
                    return null;
                }
            }
            if (duration < (timeEnd - timeStart)) {
                return null;
            }

            if (filter) {
                PreparedStatement preparedStatement = connection.prepareStatement(sql_no_duplicated);
                preparedStatement.setString(1, bv);
                preparedStatement.setFloat(2, timeEnd);
                preparedStatement.setFloat(3, timeStart);
                ResultSet rs = preparedStatement.executeQuery();

                while (rs.next()) {
                    ans.add(rs.getLong(1));
                }
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement(sql_duplicate);
                preparedStatement.setFloat(1, timeEnd);
                preparedStatement.setFloat(2, timeStart);
                preparedStatement.setString(3, bv);
                ResultSet rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    ans.add(rs.getLong(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ans;
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        UserImpl userimpl = new UserImpl();
        String sql_find_danmu = """
                select bv,likedby
                from danmurecord
                where danmu_id = ?;""";   // find the danmu by its id
        String sql_update_mids = """
                update danmurecord
                set likedby = ?
                where danmu_id = ?;""";
        try (Connection con = dataSource.getConnection();
             PreparedStatement stmt_danmu = con.prepareStatement(sql_find_danmu);
             PreparedStatement stmt_update = con.prepareStatement(sql_update_mids)
        ) {
            if (!userimpl.isValidAuth(auth, con)) {
                return false;
            }

            stmt_danmu.setLong(1, id);
            ResultSet rs = stmt_danmu.executeQuery();
            String bv;
            List<Long> mids = new ArrayList<>();
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());

            if (rs.next()) {
                bv = rs.getString(1);
                if (!is_valid_video(bv, auth, now, con)) {
                    return false;
                }
                Array sqlArray = rs.getArray(2);
                if (sqlArray != null) {
                    Long[] array = (Long[]) sqlArray.getArray();  // Convert SQL Array to Java Array
                    mids = new ArrayList<>(Arrays.asList(array));  // Convert Java Array to List
                }
            } else {
                return false;
            }
            long mid = auth.getMid();
            if (mids.contains(mid)) {
                mids.remove(mid);
            } else {
                mids.add(mid);
            }
            if (!mids.isEmpty()) {
                stmt_update.setArray(1, con.createArrayOf("bigint", mids.toArray(new Long[0])));
            } else {
                stmt_update.setArray(1, con.createArrayOf("bigint", new Long[0]));
            }
            stmt_update.setLong(2, id);
            int roweffected = stmt_update.executeUpdate();
            return (roweffected > 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private boolean is_valid_bv(String bv, Connection connection) {
        if (bv == null || bv.isEmpty()) {
            return false;
        }
        String query = "SELECT COUNT(*) FROM VideoRecord WHERE bv = ?";

        try (
                PreparedStatement stmt = connection.prepareStatement(query)) {

            stmt.setString(1, bv);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                int count = rs.getInt(1);
                return count > 0;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;

    }

    private boolean is_valid_content(String content) {
        return content != null && !content.isEmpty();
    }

    private boolean is_valid_video(String bv, AuthInfo authInfo, Timestamp now, Connection connection) {

        String query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM ViewRecord
                    WHERE bv = ?
                        AND mid = ?
                )
                AND (
                    publicTime IS NULL OR publicTime < ?
                )
                FROM VideoRecord
                WHERE bv = ?;
                """;
        long mid = authInfo.getMid();
        try (
                PreparedStatement pstmt = connection.prepareStatement(query)) {

            pstmt.setString(1, bv);
            pstmt.setLong(2, mid);
            pstmt.setTimestamp(3, now);
            pstmt.setString(4, bv);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getBoolean(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

}