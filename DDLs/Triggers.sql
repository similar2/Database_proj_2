-- Create or replace the trigger function
CREATE OR REPLACE FUNCTION add_view_to_userinfo()
    RETURNS TRIGGER AS
$$
BEGIN
    -- Handle insert operations
    IF TG_OP = 'INSERT' THEN
        RAISE NOTICE 'Adding bv: % to watched list of user MID: %', NEW.bv, NEW.mid;
        -- Append the bv to the watched list of the user
        UPDATE UserInfoResp
        SET watched = array_append(watched, NEW.bv)
        WHERE mid = NEW.mid;
    END IF;

    -- Handle update operations
    IF TG_OP = 'UPDATE' THEN
        RAISE NOTICE 'Updating bv from: % to: % for user MID: %', OLD.bv, NEW.bv, NEW.mid;
        -- Remove the old bv and add the new bv to the watched list of the user
        UPDATE UserInfoResp
        SET watched = array_replace(watched, OLD.bv, NEW.bv)
        WHERE mid = NEW.mid;
    END IF;

    -- Handle delete operations
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



-- Create a trigger for insert operations on ViewRecord
CREATE or replace TRIGGER viewrecord_insert
    AFTER INSERT
    ON ViewRecord
    FOR EACH ROW
EXECUTE FUNCTION add_view_to_userinfo();
-- Create a trigger for update operations on ViewRecord
CREATE or replace TRIGGER viewrecord_update
    AFTER UPDATE
    ON ViewRecord
    FOR EACH ROW
EXECUTE FUNCTION add_view_to_userinfo();

-- Create a trigger for delete operations on ViewRecord
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

    -- 首先更新 UserInfoResp 中的 following 列
    UPDATE UserInfoResp
    SET following = NEW.following
    WHERE mid = NEW.mid;

    -- 计算不再关注的用户
    removed_following := ARRAY(
            SELECT unnest(OLD.following)
            EXCEPT
            SELECT unnest(NEW.following)
                         );

    -- 移除不再关注的用户
    FOREACH followed_mid IN ARRAY removed_following
        LOOP
            RAISE NOTICE 'Removing unfollowed MID: % from follower list of MID: %', OLD.mid, followed_mid;
            UPDATE UserInfoResp
            SET follower = array_remove(follower, OLD.mid)
            WHERE mid = followed_mid;
        END LOOP;

    -- 计算新添加的关注用户
    added_following := ARRAY(
            SELECT unnest(NEW.following)
            EXCEPT
            SELECT unnest(OLD.following)
                       );

    -- 添加新的关注
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

    -- 首先从 UserInfoResp 表中删除相应的记录
    DELETE FROM UserInfoResp WHERE mid = OLD.mid;

    -- 更新其他用户的 follower 列
    IF OLD.following IS NOT NULL THEN
        FOREACH followed_mid IN ARRAY OLD.following
            LOOP
                RAISE NOTICE 'Removing MID: % from follower list of MID: %', OLD.mid, followed_mid;
                UPDATE UserInfoResp
                SET follower = array_remove(follower, OLD.mid)
                WHERE mid = followed_mid;
            END LOOP;
    END IF;

    -- 更新其他用户的 following 列，移除被删除的 mid
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
    AFTER DELETE
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION userrecord_delete_trigger();

create table if not exists restricted_words
(
    id   serial primary key,
    word varchar(255) not null
);


CREATE OR REPLACE FUNCTION content_check()
    RETURNS TRIGGER AS
$$
DECLARE
    word record;
begin
    for word in select * from restricted_words
        loop
            if lower(word) = lower(new.content) then
                return null;
            end if;
        end loop;
end ;
$$ language plpgsql;
create or replace trigger dirty_words
    before insert
    on danmurecord
    for each row
execute procedure content_check();

