-- Create or replace the trigger function
CREATE OR REPLACE FUNCTION sync_user_info()
    RETURNS TRIGGER AS
$$
declare
    followed_mid bigint;
BEGIN
    -- Handle insert operations
    IF TG_OP = 'INSERT' THEN
        -- Insert the new user record into UserInfoResp
        INSERT INTO UserInfoResp (mid, following)
        VALUES (NEW.mid, NEW.following);

        -- For each user in the new record's following list, add new.mid as a follower
        FOREACH followed_mid IN ARRAY NEW.following
            LOOP
                UPDATE UserInfoResp
                SET follower = array_append(follower, NEW.mid)
                WHERE mid = followed_mid;
            END LOOP;
        RETURN NEW;
    END IF;

-- Handle update operations
    IF upper(TG_OP) = 'UPDATE' THEN
        raise notice 'update userrecord';
        -- Update the following list for the user
        UPDATE UserInfoResp
        SET following = NEW.following
        WHERE mid = NEW.mid;

        -- Remove the user's mid from the follower list of users no longer followed
        UPDATE UserInfoResp
        SET follower = array_remove(follower, NEW.mid)
        WHERE mid = ANY (SELECT unnest(OLD.following)
                         EXCEPT
                         SELECT unnest(NEW.following));

        -- Add the user's mid to the follower list of newly followed users
        UPDATE UserInfoResp
        SET follower = array_append(follower, NEW.mid)
        WHERE mid = ANY (SELECT unnest(NEW.following)
                         EXCEPT
                         SELECT unnest(OLD.following));

        RETURN NEW;
    END IF;

-- Handle delete operations
    IF TG_OP = 'DELETE' THEN
        -- Delete the user's record from UserInfoResp
        DELETE FROM UserInfoResp WHERE mid = OLD.mid;
        -- Remove the user's mid from the follower list of all users they were following
        UPDATE UserInfoResp
        SET follower = array_remove(follower, OLD.mid)
        WHERE mid = ANY (OLD.following);
        RETURN OLD;
    END IF;
    --have to mention that the return value doesn't matter because this is a after trigger
END;
$$ LANGUAGE plpgsql;

-- Create a trigger for insert operations
CREATE or replace TRIGGER userrecord_insert
    AFTER INSERT
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION sync_user_info();

-- Create a trigger for update operations
CREATE or replace TRIGGER userrecord_update
    AFTER UPDATE
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION sync_user_info();

-- Create a trigger for delete operations
CREATE or replace TRIGGER userrecord_delete
    AFTER DELETE
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION sync_user_info();



-- Create or replace the trigger function
CREATE OR REPLACE FUNCTION add_view_to_userinfo()
    RETURNS TRIGGER AS
$$
BEGIN
    -- Handle insert operations
    IF TG_OP = 'INSERT' THEN
        -- Append the bv to the watched list of the user
        UPDATE UserInfoResp
        SET watched = array_append(watched, NEW.bv)
        WHERE mid = NEW.mid;
    END IF;

    -- Handle update operations
    IF TG_OP = 'UPDATE' THEN
        -- Remove the old bv and add the new bv to the watched list of the user
        UPDATE UserInfoResp
        SET watched = array_replace(watched, OLD.bv, NEW.bv)
        WHERE mid = NEW.mid;
    END IF;

    -- Handle delete operations
    IF TG_OP = 'DELETE' THEN
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



CREATE OR REPLACE FUNCTION userrecord_sync_trigger()
    RETURNS TRIGGER AS
$$
BEGIN
    -- Handle insert operations
    IF TG_OP = 'INSERT' THEN
        INSERT INTO AuthInfo (mid, password, qq, wechat)
        VALUES (NEW.mid, NEW.password, NEW.qq, NEW.wechat);
    END IF;

    -- Handle update operations
    IF TG_OP = 'UPDATE' THEN
        UPDATE AuthInfo
        SET password = NEW.password,
            qq       = NEW.qq,
            wechat   = NEW.wechat
        WHERE mid = NEW.mid;
    END IF;

    -- Handle delete operations
    IF TG_OP = 'DELETE' THEN
        DELETE
        FROM AuthInfo
        WHERE mid = OLD.mid;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE or replace TRIGGER userrecord_after_insert
    AFTER INSERT
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION userrecord_sync_trigger();
CREATE or replace TRIGGER userrecord_after_update
    AFTER UPDATE
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION userrecord_sync_trigger();
CREATE or replace TRIGGER userrecord_after_delete
    AFTER DELETE
    ON UserRecord
    FOR EACH ROW
EXECUTE FUNCTION userrecord_sync_trigger();


