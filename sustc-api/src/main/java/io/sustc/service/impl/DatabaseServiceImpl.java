package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12212921, 12210653, 12211617);
    }

    @Override
    public void importData(List<DanmuRecord> danmuRecords, List<UserRecord> userRecords, List<VideoRecord> videoRecords) {

        {
            final int batch_size = 500;
            final int DefaultYear = 2000;
            String trigger = """
                                  CREATE OR REPLACE FUNCTION add_view_to_userinfo()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            IF TG_OP = 'INSERT' THEN
                                                                RAISE NOTICE 'Adding bv: % to watched list of user MID: %', NEW.bv, NEW.mid;
                                                                UPDATE UserInfoResp
                                                                SET watched = array_append(watched, NEW.bv)
                                                                WHERE mid = NEW.mid;
                                                            END IF;
                                                            IF TG_OP = 'UPDATE' THEN
                                                                RAISE NOTICE 'Updating bv from: % to: % for user MID: %', OLD.bv, NEW.bv, NEW.mid;
                                                                UPDATE UserInfoResp
                                                                SET watched = array_replace(watched, OLD.bv, NEW.bv)
                                                                WHERE mid = NEW.mid;
                                                            END IF;
                                                            IF TG_OP = 'DELETE' THEN
                                                                RAISE NOTICE 'Removing bv: % from watched list of user MID: %', OLD.bv, OLD.mid;
                                                                -- Remove the bv from the watched list of the user
                                                                UPDATE UserInfoResp
                                                                SET watched = array_remove(watched, OLD.bv)
                                                                WHERE mid = OLD.mid;
                                                            END IF;
                                                        
                                                            -- Return the appropriate record based on the operation
                                                            RETURN COALESCE(NEW, OLD);
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE or replace TRIGGER viewrecord_insert
                                                            AFTER INSERT
                                                            ON ViewRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION add_view_to_userinfo();
                                                        CREATE or replace TRIGGER viewrecord_update
                                                            AFTER UPDATE
                                                            ON ViewRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION add_view_to_userinfo();
                                                        CREATE or replace TRIGGER viewrecord_delete
                                                            AFTER DELETE
                                                            ON ViewRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION add_view_to_userinfo();
                                                        CREATE OR REPLACE FUNCTION userrecord_insert_trigger()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        declare
                                                            followed_mid bigint;
                                                        BEGIN
                                                            IF NOT EXISTS (SELECT 1 FROM UserInfoResp WHERE mid = NEW.mid) THEN
                                                                INSERT INTO UserInfoResp (mid, coin, following, follower, watched, liked, collected, posted)
                                                                VALUES (NEW.mid,
                                                                        0,
                                                                        COALESCE(NEW.following, ARRAY []::bigint[]),
                                                                        ARRAY []::bigint[],
                                                                        ARRAY []::VARCHAR(255)[],
                                                                        ARRAY []::VARCHAR(255)[],
                                                                        ARRAY []::VARCHAR(255)[],
                                                                        ARRAY []::VARCHAR(255)[]);
                                                            END IF;
                                                            IF NEW.following IS NOT NULL THEN
                                                                FOREACH followed_mid IN ARRAY NEW.following
                                                                    LOOP
                                                                        UPDATE UserInfoResp
                                                                        SET follower = array_append(follower, NEW.mid)
                                                                        WHERE mid = followed_mid;
                                                                    END LOOP;
                                                            END IF;
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE or replace TRIGGER userrecord_after_insert
                                                            AFTER INSERT
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION userrecord_insert_trigger();
                                                        CREATE OR REPLACE FUNCTION userrecord_update_trigger()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        DECLARE
                                                            removed_following BIGINT[];
                                                            added_following   BIGINT[];
                                                            followed_mid      BIGINT;
                                                        BEGIN
                                                            RAISE NOTICE 'Update trigger started for MID: %', NEW.mid;
                                                            UPDATE UserInfoResp
                                                            SET following = NEW.following
                                                            WHERE mid = NEW.mid;
                                                            removed_following := ARRAY(
                                                                    SELECT unnest(OLD.following)
                                                                    EXCEPT
                                                                    SELECT unnest(NEW.following)
                                                                                 );
                                                            FOREACH followed_mid IN ARRAY removed_following
                                                                LOOP
                                                                    RAISE NOTICE 'Removing unfollowed MID: % from follower list of MID: %', OLD.mid, followed_mid;
                                                                    UPDATE UserInfoResp
                                                                    SET follower = array_remove(follower, OLD.mid)
                                                                    WHERE mid = followed_mid;
                                                                END LOOP;
                                                            added_following := ARRAY(
                                                                    SELECT unnest(NEW.following)
                                                                    EXCEPT
                                                                    SELECT unnest(OLD.following)
                                                                               );
                                                            FOREACH followed_mid IN ARRAY added_following
                                                                LOOP
                                                                    RAISE NOTICE 'Adding new follower MID: % to follower list of MID: %', NEW.mid, followed_mid;
                                                                    UPDATE UserInfoResp
                                                                    SET follower = array_append(follower, NEW.mid)
                                                                    WHERE mid = followed_mid
                                                                      AND NEW.mid <> followed_mid;
                                                                END LOOP;
                                                        
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE or replace TRIGGER userrecord_after_update
                                                            AFTER UPDATE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION userrecord_update_trigger();
                                                        CREATE OR REPLACE FUNCTION userrecord_delete_trigger()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        DECLARE
                                                            followed_mid   BIGINT;
                                                            following_user BIGINT;
                                                        BEGIN
                                                            RAISE NOTICE 'Deleting user MID: % from UserInfoResp table', OLD.mid;
                                                            DELETE FROM UserInfoResp WHERE mid = OLD.mid;
                                                            IF OLD.following IS NOT NULL THEN
                                                                FOREACH followed_mid IN ARRAY OLD.following
                                                                    LOOP
                                                                        RAISE NOTICE 'Removing MID: % from follower list of MID: %', OLD.mid, followed_mid;
                                                                        UPDATE UserInfoResp
                                                                        SET follower = array_remove(follower, OLD.mid)
                                                                        WHERE mid = followed_mid;
                                                                    END LOOP;
                                                            END IF;
                                                            FOR following_user IN
                                                                SELECT mid FROM UserInfoResp WHERE OLD.mid = ANY (following)
                                                                LOOP
                                                                    RAISE NOTICE 'Removing MID: % from following list of MID: %', OLD.mid, following_user;
                                                                    UPDATE UserInfoResp
                                                                    SET following = array_remove(following, OLD.mid)
                                                                    WHERE mid = following_user;
                                                                END LOOP;
                                                        
                                                            RETURN OLD;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE or replace TRIGGER userrecord_after_delete
                                                            before DELETE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION userrecord_delete_trigger();
                                                        CREATE OR REPLACE FUNCTION update_userinfo_on_likes()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            IF (TG_OP = 'DELETE') THEN
                                                                UPDATE UserInfoResp
                                                                SET liked = array_remove(liked, OLD.BV_liked)
                                                                WHERE mid = OLD.mid_liked;
                                                            ELSE
                                                                UPDATE UserInfoResp
                                                                SET liked = array_append(liked, NEW.BV_liked)
                                                                WHERE mid = NEW.mid_liked;
                                                            END IF;
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE OR REPLACE FUNCTION update_userinfo_on_favorites()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            -- 更新 UserInfoResp 表中的 collected 列
                                                            IF (TG_OP = 'DELETE') THEN
                                                                UPDATE UserInfoResp
                                                                SET collected = array_remove(collected, OLD.BV_favorite)
                                                                WHERE mid = OLD.mid_favorite;
                                                            ELSE
                                                                UPDATE UserInfoResp
                                                                SET collected = array_append(collected, NEW.BV_favorite)
                                                                WHERE mid = NEW.mid_favorite;
                                                            END IF;
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE OR REPLACE FUNCTION update_userinfo_on_posted()
                                                            RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            IF (TG_OP = 'DELETE') THEN
                                                                UPDATE UserInfoResp
                                                                SET posted = array_remove(posted, OLD.bv)
                                                                WHERE mid = OLD.ownerMid;
                                                            ELSE
                                                                UPDATE UserInfoResp
                                                                SET posted = array_append(posted, NEW.bv)
                                                                WHERE mid = NEW.ownerMid;
                                                            END IF;
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE or replace TRIGGER trigger_likes_after_change
                                                            AFTER INSERT OR DELETE OR UPDATE
                                                            ON likes
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION update_userinfo_on_likes();
                                                        CREATE or replace TRIGGER trigger_favorites_after_change
                                                            AFTER INSERT OR DELETE OR UPDATE
                                                            ON favorites
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION update_userinfo_on_favorites();
                                                        CREATE or replace TRIGGER trigger_videorecord_after_change
                                                            AFTER INSERT OR DELETE OR UPDATE
                                                            ON VideoRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION update_userinfo_on_posted();
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_insert() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Inserting into AuthInfo for mid: %', NEW.mid;
                                                            INSERT INTO AuthInfo (mid, password, qq, wechat)
                                                            VALUES (NEW.mid, NEW.password, NEW.qq, NEW.wechat);
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_update() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Updating AuthInfo for mid: %', NEW.mid;
                                                            UPDATE AuthInfo
                                                            SET password = NEW.password,
                                                                qq       = NEW.qq,
                                                                wechat   = NEW.wechat
                                                            WHERE mid = NEW.mid;
                                                            RETURN NEW;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_delete() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Deleting from AuthInfo for mid: %', OLD.mid;
                                                            DELETE FROM AuthInfo WHERE mid = OLD.mid;
                                                            RETURN OLD;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_delete_viewrecord() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Deleting from ViewRecord for mid: %', OLD.mid;
                                                            DELETE FROM ViewRecord WHERE mid = OLD.mid;
                                                            RETURN OLD;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_delete_likes() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Deleting from likes for mid: %', OLD.mid;
                                                            DELETE FROM likes WHERE mid_liked = OLD.mid;
                                                            RETURN OLD;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_delete_favorites() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Deleting from favorites for mid: %', OLD.mid;
                                                            DELETE FROM favorites WHERE mid_favorite = OLD.mid;
                                                            RETURN OLD;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE OR REPLACE FUNCTION trigger_userrecord_delete_coins() RETURNS TRIGGER AS
                                                        $$
                                                        BEGIN
                                                            RAISE NOTICE 'Deleting from coins for mid: %', OLD.mid;
                                                            DELETE FROM coins WHERE mid_coin = OLD.mid;
                                                            RETURN OLD;
                                                        END;
                                                        $$ LANGUAGE plpgsql;
                                                        CREATE or replace TRIGGER userrecord_insert
                                                            AFTER INSERT
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_insert();
                                                        
                                                        CREATE or replace TRIGGER userrecord_update
                                                            AFTER UPDATE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_update();
                                                        
                                                        
                                                        CREATE or replace TRIGGER userrecord_delete
                                                            before DELETE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_delete();
                                                        
                                                        CREATE or replace TRIGGER userrecord_delete_viewrecord
                                                            before DELETE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_delete_viewrecord();
                                                        
                                                        
                                                        CREATE or replace TRIGGER userrecord_delete_likes
                                                            BEFORE DELETE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_delete_likes();
                                                        
                                                        CREATE or replace TRIGGER userrecord_delete_favorites
                                                            BEFORE DELETE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_delete_favorites();
                                                        ;
                                                        
                                                        CREATE or replace TRIGGER userrecord_delete_coins
                                                            BEFORE DELETE
                                                            ON UserRecord
                                                            FOR EACH ROW
                                                        EXECUTE FUNCTION trigger_userrecord_delete_coins();
                    """;
            String sql_user = "INSERT INTO UserRecord (mid,name, sex, birthday, level, sign, following, identity, password, qq, wechat,coin) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
            String sql_video = "INSERT INTO VideoRecord (bv, title, ownerMid, ownerName, commitTime, reviewTime, publicTime, duration, description, reviewer) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String sql_danmu = "INSERT INTO DanmuRecord (bv, mid, time, content, postTime, likedBy)" + "VALUES (?, ?, ?, ?, ?, ?)";
            String sql_view = "insert into ViewRecord " + "values (?,?,?);";
            String sql_coin = """
                    INSERT INTO coins (BV_coin, mid_coin)
                    VALUES (?, ?);""";
            String sql_favorite = """
                    insert into favorites (bv_favorite, mid_favorite)
                    VALUES (?, ?);""";
            String sql_like = """
                    INSERT INTO likes (BV_liked, mid_liked)
                    VALUES (?, ?);""";
            String sql_sync_2_userinforeq = """
                                    
                    -- 创建包含所有 follower-followed 对的临时表
                    WITH FollowersMapping AS (SELECT unnest(ur.following) AS followed_mid,
                                                     ur.mid               AS follower_mid
                                              FROM UserRecord ur),
                    -- 为每个 followed_mid 聚合其 followers
                         AggregatedFollowers AS (SELECT followed_mid,
                                                        ARRAY_AGG(DISTINCT follower_mid) AS followers
                                                 FROM FollowersMapping
                                                 GROUP BY followed_mid),
                    -- 为每个用户聚合其 watched 列表
                         AggregatedWatched AS (SELECT vr.mid,
                                                      ARRAY_AGG(DISTINCT vr.bv) AS watched_videos
                                               FROM ViewRecord vr
                                               GROUP BY vr.mid),
                    -- 为每个用户聚合其 liked 列表
                         AggregatedLiked AS (SELECT lr.mid_liked                    AS mid,
                                                    ARRAY_AGG(DISTINCT lr.BV_liked) AS liked_videos
                                             FROM likes lr
                                             GROUP BY lr.mid_liked),
                    -- 为每个用户聚合其 favorited 列表
                         AggregatedFavorites AS (SELECT fr.mid_favorite                    AS mid,
                                                        ARRAY_AGG(DISTINCT fr.BV_favorite) AS favorited_videos
                                                 FROM favorites fr
                                                 GROUP BY fr.mid_favorite),
                    -- 为每个用户聚合其 posted 列表
                         AggregatedPosted AS (SELECT vr.ownerMid               AS mid,
                                                     ARRAY_AGG(DISTINCT vr.bv) AS posted_videos
                                              FROM VideoRecord vr
                                              GROUP BY vr.ownerMid)
                    -- 插入新记录到 UserInfoResp 表
                    INSERT
                    INTO UserInfoResp (mid, coin, following, follower, watched, liked, collected, posted)
                    SELECT ur.mid,
                           ur.coin,                                                  -- 从 UserRecord 获取 coin 值
                           ur.following,
                           COALESCE(af.followers, ARRAY []::bigint[]),               -- 如果有 followers，则插入，否则插入空数组
                           COALESCE(aw.watched_videos, ARRAY []::VARCHAR(255)[]),    -- 如果有 watched_videos，则插入，否则插入空数组
                           COALESCE(al.liked_videos, ARRAY []::VARCHAR(255)[]),      -- 如果有 liked_videos，则插入，否则插入空数组
                           COALESCE(afv.favorited_videos, ARRAY []::VARCHAR(255)[]), -- 如果有 favorited_videos，则插入，否则插入空数组
                           COALESCE(ap.posted_videos, ARRAY []::VARCHAR(255)[])      -- 如果有 posted_videos，则插入，否则插入空数组
                    FROM UserRecord ur
                             LEFT JOIN AggregatedFollowers af ON ur.mid = af.followed_mid
                             LEFT JOIN AggregatedWatched aw ON ur.mid = aw.mid
                             LEFT JOIN AggregatedLiked al ON ur.mid = al.mid
                             LEFT JOIN AggregatedFavorites afv ON ur.mid = afv.mid
                             LEFT JOIN AggregatedPosted ap ON ur.mid = ap.mid
                    WHERE NOT EXISTS (SELECT 1 FROM UserInfoResp WHERE mid = ur.mid);
                                  
                    """;
            String sql_sync_2_authinfo = """              
                    INSERT INTO AuthInfo (mid, password, qq, wechat)
                    SELECT mid,
                           password,
                           qq,
                           wechat
                    FROM UserRecord
                    WHERE NOT EXISTS (SELECT 1 FROM AuthInfo WHERE mid = UserRecord.mid);
                    """;
            String sql_disable_trigger = """
                      ALTER TABLE UserRecord DISABLE TRIGGER ALL;
                    ALTER TABLE ViewRecord DISABLE TRIGGER ALL;
                    ALTER TABLE UserInfoResp DISABLE TRIGGER ALL;
                    ALTER TABLE likes DISABLE TRIGGER ALL;
                    ALTER TABLE favorites DISABLE TRIGGER ALL;
                    ALTER TABLE VideoRecord DISABLE TRIGGER ALL;
                    ALTER TABLE AuthInfo DISABLE TRIGGER ALL;
             
                      """;
            String sql_enable_trigger = """
                                 ALTER TABLE UserRecord ENABLE TRIGGER ALL;
                                 ALTER TABLE ViewRecord ENABLE TRIGGER ALL;
                                 ALTER TABLE UserInfoResp ENABLE TRIGGER ALL;
                                 ALTER TABLE likes ENABLE TRIGGER ALL;
                                 ALTER TABLE favorites ENABLE TRIGGER ALL;
                                 ALTER TABLE VideoRecord ENABLE TRIGGER ALL;
                                 ALTER TABLE AuthInfo ENABLE TRIGGER ALL;
                                 
                    """;
            try (Connection conn = dataSource.getConnection();
                 Statement sync = conn.createStatement(); PreparedStatement stmt_danmu = conn.prepareStatement(sql_danmu); PreparedStatement stmt_user = conn.prepareStatement(sql_user); PreparedStatement stmt_video = conn.prepareStatement(sql_video); PreparedStatement stmt_view = conn.prepareStatement(sql_view); PreparedStatement stmt_coin = conn.prepareStatement(sql_coin); PreparedStatement stmt_like = conn.prepareStatement(sql_like); PreparedStatement stmt_favorite = conn.prepareStatement(sql_favorite)) {
                int cnt = 0;
                sync.execute(trigger);
                sync.execute(sql_disable_trigger);
                for (UserRecord temp : userRecords) {
                    long mid = temp.getMid();
                    stmt_user.setLong(1, mid);
                    String name = temp.getName();
                    stmt_user.setString(2, name);
                    String sex = temp.getSex();
                    stmt_user.setString(3, sex);
                    String birthday = temp.getBirthday();

                    if (birthday != null && !birthday.isEmpty()) {

                        // 通过正则表达式匹配不同的格式
                        String[] parts ;
                        int month = 0;
                        int day = 0;

                        if (birthday.matches("\\d{1,2}月\\d{1,2}日")) {
                            parts = birthday.split("月");
                            month = Integer.parseInt(parts[0]);
                            day = Integer.parseInt(parts[1].replace("日", ""));
                        } else if (birthday.matches("\\d{1,2}-\\d{1,2}")) {
                            parts = birthday.split("-");
                            month = Integer.parseInt(parts[0]);
                            day = Integer.parseInt(parts[1]);
                        } else {
                            stmt_user.setNull(4, java.sql.Types.DATE);
                        }
                        String completeBirthday = DefaultYear + "-" + month + "-" + day;

                        stmt_user.setDate(4, Date.valueOf(completeBirthday));
                    } else {
                        stmt_user.setNull(4, java.sql.Types.DATE);
                    }
                    short level = temp.getLevel();
                    stmt_user.setShort(5, level);
                    String sign = temp.getSign();
                    stmt_user.setString(6, sign);

                    long[] following = temp.getFollowing();
                    if (following != null) {
                        stmt_user.setArray(7, conn.createArrayOf("bigint", Arrays.stream(following).boxed().toArray()));
                    } else {
                        stmt_user.setNull(7, Types.ARRAY);
                    }
                    if (temp.getIdentity() != null) {
                        stmt_user.setString(8, temp.getIdentity().toString());
                    } else {
                        stmt_user.setNull(8, Types.VARCHAR);
                    }

                    String password = temp.getPassword();
                    stmt_user.setString(9, password);
                    String qq = temp.getQq();
                    stmt_user.setString(10, qq);
                    String wechat = temp.getWechat();
                    stmt_user.setString(11, wechat);
                    int coin = temp.getCoin();
                    stmt_user.setInt(12, coin);

                    cnt++;
                    stmt_user.addBatch();
                    if (cnt % batch_size == 0) {
                        stmt_user.executeBatch();
                    }
                }
                stmt_user.executeBatch();
                stmt_user.close();
                sync.execute(sql_sync_2_userinforeq);
                sync.execute(sql_sync_2_authinfo);

                cnt = 0;
                int cnt_view = 0;
                int cnt_like = 0;
                int cnt_coin = 0;
                int cnt_favorite = 0;
                for (VideoRecord temp : videoRecords) {
                    String bv = temp.getBv();
                    stmt_video.setString(1, bv);//bv
                    stmt_video.setString(2, temp.getTitle());//title

                    stmt_video.setLong(3, temp.getOwnerMid());//owner mid
                    stmt_video.setString(4, temp.getOwnerName());//owner name
                    stmt_video.setTimestamp(5, temp.getCommitTime());//commit time
                    stmt_video.setTimestamp(6, temp.getReviewTime());//review time
                    stmt_video.setTimestamp(7, temp.getPublicTime());//public time
                    stmt_video.setFloat(8, temp.getDuration());//duration
                    stmt_video.setString(9, temp.getDescription());//description
                    stmt_video.setLong(10, temp.getReviewer());//reviewer
                    stmt_video.addBatch();
                    stmt_video.executeBatch();
                    long[] like = temp.getLike();
                    if (like != null) {
                        for (long l : like) {
                            stmt_like.setString(1, bv);
                            stmt_like.setLong(2, l);//like
                            cnt_like++;
                            stmt_like.addBatch();
                            if (cnt_like % batch_size == 0) {
                                stmt_like.executeBatch();
                            }
                        }

                    } else {
                        stmt_like.setString(1, bv);
                        stmt_like.setNull(2, Types.BIGINT);
                    }


                    long[] coin = temp.getCoin();
                    if (coin != null) {
                        for (long l : coin) {
                            stmt_coin.setString(1, bv);
                            stmt_coin.setLong(2, l);//coin
                            stmt_coin.addBatch();
                            cnt_coin++;
                            if (cnt_coin % batch_size == 0) {
                                stmt_coin.executeBatch();
                            }
                        }

                    } else {
                        stmt_coin.setString(1, bv);
                        stmt_coin.setNull(2, Types.BIGINT);
                    }
                    stmt_coin.executeBatch();


                    long[] favorite = temp.getFavorite();
                    if (favorite != null) {
                        for (long l : favorite) {
                            stmt_favorite.setString(1, bv);
                            stmt_favorite.setLong(2, l);//favorite
                            stmt_favorite.addBatch();
                            cnt_favorite++;
                            if (cnt_favorite % batch_size == 0) {
                                stmt_favorite.executeBatch();
                            }
                        }

                    } else {
                        stmt_favorite.setString(1, bv);
                        stmt_favorite.setNull(2, Types.BIGINT);
                    }
                    stmt_favorite.executeBatch();


                    //add to batch
                    stmt_like.addBatch();


                    long[] view = temp.getViewerMids();
                    if (view != null) {
                        long mid;
                        float timestamp;
                        for (int i = 0; i < view.length; i++) {
                            mid = temp.getViewerMids()[i];
                            timestamp = temp.getViewTime()[i];
                            stmt_view.setString(1, temp.getBv());
                            stmt_view.setLong(2, mid);
                            stmt_view.setFloat(3, timestamp);
                            cnt_view++;
                            stmt_view.addBatch();
                            if (cnt_view % batch_size == 0) {
                                stmt_view.executeBatch();
                            }
                        }
                    }


                }
                //execute what remains in the batch
                stmt_video.executeBatch();
                stmt_view.executeBatch();
                stmt_video.executeBatch();
                stmt_like.executeBatch();
                stmt_coin.executeBatch();


                //close statements
                stmt_video.close();
                stmt_view.close();
                stmt_like.close();
                stmt_coin.close();
                stmt_favorite.close();
                for (DanmuRecord temp : danmuRecords) {
                    String Bv = temp.getBv();
                    long mid = temp.getMid();
                    float time = temp.getTime();
                    String content = temp.getContent();
                    Timestamp postTime = temp.getPostTime();
                    long[] likedby = temp.getLikedBy();

                    stmt_danmu.setString(1, Bv);
                    stmt_danmu.setLong(2, mid);
                    stmt_danmu.setFloat(3, time);
                    stmt_danmu.setString(4, content);
                    stmt_danmu.setTimestamp(5, postTime);

                    if (likedby != null) {
                        stmt_danmu.setArray(6, conn.createArrayOf("bigint", Arrays.stream(likedby).boxed().toArray()));
                    } else {
                        stmt_danmu.setNull(6, Types.ARRAY);
                    }

                    stmt_danmu.addBatch();
                    cnt++;
                    if (cnt % batch_size == 0) {
                        stmt_danmu.executeBatch();
                    }
                }
                stmt_danmu.executeBatch();

                sync.execute(sql_enable_trigger);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = """
                DO $$
                DECLARE
                    tables CURSOR FOR
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public';
                BEGIN
                    FOR t IN tables
                    LOOP
                        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';
                    END LOOP;
                END $$;
                """;

        try (Connection conn = dataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
