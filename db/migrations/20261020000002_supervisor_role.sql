-- migrate:up
-- The supervisor only reads W (and LISTENs for settings_changed, which needs no grant).
GRANT SELECT ON settings TO supervisor;

-- migrate:down
REVOKE ALL ON settings FROM supervisor;
