INSERT INTO AuthInfo (mid, password, qq, wechat)
SELECT mid,
       password,
       qq,
       wechat
FROM UserRecord
WHERE NOT EXISTS (SELECT 1 FROM AuthInfo WHERE mid = UserRecord.mid);


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
