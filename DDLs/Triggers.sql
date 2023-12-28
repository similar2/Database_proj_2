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
CREATE OR REPLACE FUNCTION delete_user_danmu() RETURNS TRIGGER AS
$$
BEGIN
    DELETE FROM DanmuRecord WHERE mid = OLD.mid;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_delete_user_danmu
    BEFORE DELETE
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION delete_user_danmu();


