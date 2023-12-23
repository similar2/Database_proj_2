-- 首先创建一个包含所有 follower-followed 对的临时表
WITH FollowersMapping AS (SELECT unnest(ur.following) AS followed_mid,
                                 ur.mid               AS follower_mid
                          FROM UserRecord ur),
-- 然后为每个 followed_mid 聚合其 followers
     AggregatedFollowers AS (SELECT followed_mid,
                                    ARRAY_AGG(DISTINCT follower_mid) AS followers
                             FROM FollowersMapping
                             GROUP BY followed_mid)
-- 插入新记录到 UserInfoResp 表
INSERT
INTO UserInfoResp (mid, coin, following, follower, watched, liked, collected, posted)
SELECT ur.mid,
       ur.coin,                                    -- 从 UserRecord 获取 coin 值
       ur.following,
       COALESCE(af.followers, ARRAY []::bigint[]), -- 如果有 followers，则插入，否则插入空数组
       ARRAY []::VARCHAR(255)[],
       ARRAY []::VARCHAR(255)[],
       ARRAY []::VARCHAR(255)[],
       ARRAY []::VARCHAR(255)[]
FROM UserRecord ur
         LEFT JOIN
     AggregatedFollowers af ON ur.mid = af.followed_mid
WHERE NOT EXISTS (SELECT 1 FROM UserInfoResp WHERE mid = ur.mid);
