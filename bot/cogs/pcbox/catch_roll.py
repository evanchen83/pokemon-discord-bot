from __future__ import annotations

import random

MAX_RANDOM_POKEMON_ID = 1025
ROLL_ROWS = 5
ROLL_COLUMNS = 3
ROW_JACKPOT_CHANCE = 0.20
ROW_PAIR_CHANCE = 0.35

RARITY_TIERS: tuple[tuple[str, float], ...] = (
    ("common", 0.72),
    ("uncommon", 0.2),
    ("rare", 0.07),
    ("legendary", 0.01),
)

LEGENDARY_POOL = {
    144, 145, 146, 150, 151, 243, 244, 245, 249, 250, 251, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386,
    480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 638, 639, 640, 641, 642, 643,
    644, 645, 646, 647, 648, 649, 716, 717, 718, 719, 720, 721, 785, 786, 787, 788, 789, 790, 791, 792, 800,
    801, 802, 807, 808, 809, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 905, 1001, 1002, 1003,
    1004, 1007, 1008, 1014, 1015, 1024, 1025,
}


def roll_one() -> tuple[int, str]:
    tier = random.choices(
        population=[name for name, _weight in RARITY_TIERS],
        weights=[weight for _name, weight in RARITY_TIERS],
        k=1,
    )[0]
    if tier == "legendary":
        return random.choice(tuple(LEGENDARY_POOL)), tier
    if tier == "rare":
        return random.randint(350, MAX_RANDOM_POKEMON_ID), tier
    if tier == "uncommon":
        return random.randint(151, 700), tier
    return random.randint(1, 400), tier


def roll_row() -> list[tuple[int, str]]:
    if random.random() < ROW_JACKPOT_CHANCE:
        pick = roll_one()
        return [pick, pick, pick]

    if random.random() < ROW_PAIR_CHANCE:
        pair = roll_one()
        off = roll_one()
        row = [pair, pair, off]
        random.shuffle(row)
        return row

    return [roll_one() for _ in range(ROLL_COLUMNS)]


def resolve_catches(rolled: list[list[tuple[int, str]]]) -> tuple[list[int], set[int]]:
    catches: list[int] = []
    won_rows: set[int] = set()
    for row_index, row in enumerate(rolled):
        ids = [pokemon_id for pokemon_id, _tier in row]
        if len(set(ids)) == 1:
            catches.append(ids[0])
            won_rows.add(row_index)
    return catches, won_rows
