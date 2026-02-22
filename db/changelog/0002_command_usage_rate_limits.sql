--liquibase formatted sql

--changeset evan:0002-command-usage-rate-limits
CREATE TABLE IF NOT EXISTS catch_command_usage (
    user_id TEXT NOT NULL,
    day_start_date DATE NOT NULL,
    uses INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, day_start_date)
);

CREATE TABLE IF NOT EXISTS open_pack_command_usage (
    user_id TEXT NOT NULL,
    day_start_date DATE NOT NULL,
    uses INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, day_start_date)
);
