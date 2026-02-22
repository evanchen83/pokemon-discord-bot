--liquibase formatted sql

--changeset evan:0001-baseline
CREATE TABLE IF NOT EXISTS pack_openings (
    pack_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    set_id TEXT NOT NULL,
    set_name TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pack_cards (
    id BIGSERIAL PRIMARY KEY,
    pack_id UUID NOT NULL REFERENCES pack_openings(pack_id) ON DELETE CASCADE,
    card_id TEXT NOT NULL,
    card_name TEXT NOT NULL,
    rarity TEXT NOT NULL,
    card_number TEXT,
    image_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_pack_openings_user_opened
    ON pack_openings(user_id, opened_at DESC);

CREATE INDEX IF NOT EXISTS idx_pack_cards_pack
    ON pack_cards(pack_id);

CREATE TABLE IF NOT EXISTS pokemon_catches (
    user_id TEXT NOT NULL,
    pokemon_id INTEGER NOT NULL,
    catches INTEGER NOT NULL DEFAULT 0,
    first_caught_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_caught_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, pokemon_id)
);

CREATE INDEX IF NOT EXISTS idx_pokemon_catches_user_last
    ON pokemon_catches(user_id, last_caught_at DESC);

CREATE TABLE IF NOT EXISTS thread_state (
    user_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_thread_state_activity
    ON thread_state(last_activity_at);
